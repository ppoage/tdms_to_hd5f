#!/usr/bin/env python3
import sys
import os, h5py
from nptdms import TdmsFile

def convert_tdms_to_hdf5(tdms_filepath, hdf5_filepath=None):
    """
    Convert a TDMS file to HDF5 format using nptdms' hdf_export method.

    Parameters:
        tdms_filepath (str): The path to the input TDMS file.
        hdf5_filepath (str, optional): The desired path for the output HDF5 file.
                                       If not provided, the script will use the same base name as the TDMS file.
    """
    if not os.path.exists(tdms_filepath):
        print(f"Error: The file '{tdms_filepath}' does not exist.")
        sys.exit(1)

    # If output path not provided, generate one from the input filename.
    if hdf5_filepath is None:
        base, _ = os.path.splitext(tdms_filepath)
        hdf5_filepath = base + ".h5"

    print(f"Converting '{tdms_filepath}' to '{hdf5_filepath}'...")

    # Read the TDMS file
    tdms_file = TdmsFile.read(tdms_filepath)

    # Export to HDF5 format
    #tdms_file.hdf_export(hdf5_filepath)
    tdms_file.as_hdf(hdf5_filepath, mode='w', group='/')
    print("Conversion complete!")

def compress_hdf5_file(input_path, compression="gzip", compression_opts=9):
    """
    Reads an HDF5 file from `input_path` and writes a new file with the same contents,
    but with datasets compressed using the specified compression method.
    
    The new file will have '_compressed' appended to the original file name.
    
    Parameters:
        input_path (str): Path to the original HDF5 file.
        compression (str): Compression algorithm (default: "gzip").
        compression_opts (int): Compression level or options (default: 9).
    
    Returns:
        str: The path to the compressed HDF5 file.
    """
    # Build the output file name: insert '_compressed' before the extension
    base, ext = os.path.splitext(input_path)
    output_path = f"{base}_compressed{ext}"

    # Open the original file in read mode and the output file in write mode.
    with h5py.File(input_path, 'r') as f_in, h5py.File(output_path, 'w') as f_out:
        
        def copy_item(name, obj):
            """
            Callback function to recursively copy groups and datasets.
            Datasets are created with compression.
            """
            if isinstance(obj, h5py.Dataset):
                # Read the data from the original dataset.
                data = obj[()]
                # Create the dataset in the output file with the same shape and dtype,
                # applying the specified compression.
                dset = f_out.create_dataset(
                    name,
                    data=data,
                    compression=compression,
                    compression_opts=compression_opts
                )
                # Copy dataset attributes.
                for key, value in obj.attrs.items():
                    dset.attrs[key] = value
            elif isinstance(obj, h5py.Group):
                # Create the group in the output file.
                grp = f_out.require_group(name)
                # Copy group attributes.
                for key, value in obj.attrs.items():
                    grp.attrs[key] = value

        # Recursively visit all objects in the input file and copy them.
        f_in.visititems(copy_item)
    print(f"Compressed file saved as: {output_path}")
    #return output_path


def main(tdms_filepath, hdf5_filepath=None):
    # Check if command-line arguments are provided.
    if len(sys.argv) > 1:
        tdms_filepath = sys.argv[1]
        hdf5_filepath = sys.argv[2] if len(sys.argv) > 2 else None
    else:
        # No command-line args; prompt the user for input.
        #tdms_filepath = input("Enter the path to the input TDMS file: ").strip()
        if not tdms_filepath:
            print("No TDMS file provided. Exiting.")
            sys.exit(1)
        #hdf5_filepath = input("Enter the path for the output HDF5 file (press Enter to use default): ").strip()
        if not hdf5_filepath:
            hdf5_filepath = None

    convert_tdms_to_hdf5(tdms_filepath, hdf5_filepath)
    #compress_hdf5_file(hdf5_filepath)


if __name__ == '__main__':
    tdms_filepath = "/Applications/Code/Code/tdms_convert/huge_test_file_low.tdms"
    hdf5_filepath = "/Applications/Code/Code/tdms_convert/huge_test_file_low.h5"
    #main(tdms_filepath, hdf5_filepath)
    compress_hdf5_file(hdf5_filepath)