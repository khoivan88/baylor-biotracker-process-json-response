import csv
import json
import logging
from pathlib import Path, PurePath
from typing import Dict

from rich.console import Console
from rich.logging import RichHandler


console = Console()
# Set logger using Rich: https://rich.readthedocs.io/en/latest/logging.html
logging.basicConfig(
    level="DEBUG",
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True)]
)
log = logging.getLogger("rich")


CURRENT_FILEPATH = Path(__file__).resolve().parent

CSV_HEADER = ['Chemical Name', 'CAS Number', 'Group Name', 'Location (space)',
              'Physical State', 'chemical_formula', 'Amount', 'Units',
              'Container ID', 'Manufacturer', 'Product Name', 'Product Number',
              'Date Received', 'Expiration Date',
              ]

PHYSICAL_STATES = {'S': 'Solid', 'L': 'Liquid', 'G': 'Gas'}


def extract_container_info(row_data: Dict[str, Dict], reference: Dict[str, Dict]) -> Dict[str, Dict]:
    """Extract each chemical container (`node--chemical_container`) into its own Dict

    Parameters
    ----------
    row_data : Dict[str, Dict]
        chemical container data, should be the info of each item with type == 'node--chemical_container'
    reference : Dict[str, Dict]
        the reference dict

    Returns
    -------
    Dict[str, Dict]
        the return extract info
    """
    # Extract chemical info
    chemical_type_id = row_data['relationships']['field_chemical_type']['data']['id']
    cas_number = reference['chemdb_type'][chemical_type_id]['field_chemdb_cas_number']
    physical_state = PHYSICAL_STATES[reference['chemdb_type'][chemical_type_id]['field_chemdb_physical_state']]
    chemical_formula = reference['chemdb_type'][chemical_type_id]['field_chemdb_chemical_formula']

    # Extract location info
    chemical_space_id = row_data['relationships']['field_chemical_space']['data']['id']
    location = reference['space'][chemical_space_id]['title']

    # Extract owner (what research group) info
    # !Watch out for the `row_data['relationships']['og_audience']['data']`, the result is a list
    # the first item is picked but double check for accuracy
    lab_id = row_data['relationships']['og_audience']['data'][0]['id']
    owner = reference['laboratory'][lab_id]['title']

    return {
        'Chemical Name': row_data['attributes']['field_chemical_product_name'],
        'CAS Number': cas_number,
        'Group Name': owner,
        'Location (space)': location,
        'Physical State': physical_state,
        'chemical_formula': chemical_formula,
        'Amount': row_data['attributes']['field_chemical_amount'],
        'Units': row_data['attributes']['field_chemical_unit_of_measure'],
        'Container ID': row_data['attributes']['field_chemical_container_id'],
        'Manufacturer': row_data['attributes']['field_chemical_product_name'],
        'Product Name': row_data['attributes']['field_chemical_product_name'],
        'Product Number': row_data['attributes']['field_chemical_product_number'],
        'Date Received': row_data['attributes']['field_chemical_received'],
        'Expiration Date': row_data['attributes']['field_chemical_expiration'],
    }


def load_reference_tables(data: Dict[str, Dict]) -> Dict[str, Dict]:
    """Load reference info into a Dict for relational lookup

    Parameters
    ----------
    data : Dict[str, Dict]
        The data from the json file, in dict type, can be read in using `json.load()`

    Returns
    -------
    Dict[str, Dict]
        The dict of the specific reference type with each type listing IDs as keys
    """
    return {
        'chemdb_type': {
            supplementary_info['id']: {
                title: value
                for title, value in supplementary_info['attributes'].items()
                if title in ['field_chemdb_cas_number',
                             'field_chemdb_chemical_formula',
                             'field_chemdb_physical_state',
                             ]
            }
            for supplementary_info in data['included']
            if supplementary_info['type'] == 'node--chemdb_type'
        },
        'space': {
            supplementary_info['id']: {
                title: value
                for title, value in supplementary_info['attributes'].items()
                if title in ['title']
            }
            for supplementary_info in data['included']
            if supplementary_info['type'] == 'node--space'
        },
        'laboratory': {
            supplementary_info['id']: {
                title: value
                for title, value in supplementary_info['attributes'].items()
                if title in ['title']
            }
            for supplementary_info in data['included']
            if supplementary_info['type'] == 'node--laboratory'
        },
    }


def process_json_response(json_file: str, output_file: str = 'output.csv'):
    with open(CURRENT_FILEPATH / json_file, 'r') as f_in:
        json_data = json.load(f_in)

    # Get the reference data
    reference = load_reference_tables(data=json_data)
    # console.print(f'{reference=}')

    # Create output file data folder if not exists
    outfile = CURRENT_FILEPATH / output_file
    outfile_parent = outfile.parent
    outfile_parent.mkdir(exist_ok=True)

    with open(outfile, 'w', newline='') as f_output:
        dict_writer = csv.DictWriter(f_output, fieldnames=CSV_HEADER)
        dict_writer.writeheader()
        for container in json_data['data']:
            row = extract_container_info(row_data=container, reference=reference)
            # console.log(f'{row=}')
            dict_writer.writerow(row)


if __name__ == '__main__':
    # File path should be relative to this python file
    file = '../data/input/wood.json'
    output_file = '../data/output/output.csv'
    process_json_response(json_file=file, output_file=output_file)