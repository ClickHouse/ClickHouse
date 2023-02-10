#!/usr/bin/env python3
import subprocess
import logging
import os


def compress_file_fast(path, archive_path):
    if archive_path.endswith(".zst"):
        subprocess.check_call("zstd < {} > {}".format(path, archive_path), shell=True)
    elif os.path.exists("/usr/bin/pigz"):
        subprocess.check_call("pigz < {} > {}".format(path, archive_path), shell=True)
    else:
        subprocess.check_call("gzip < {} > {}".format(path, archive_path), shell=True)


def compress_fast(path, archive_path, exclude=None):
    program_part = ""
    if archive_path.endswith(".zst"):
        logging.info("zstd will be used for compression")
        program_part = "--use-compress-program='zstd --threads=0'"
    elif os.path.exists("/usr/bin/pigz"):
        logging.info("pigz found, will compress and decompress faster")
        program_part = "--use-compress-program='pigz'"
    else:
        program_part = "-z"
        logging.info("no pigz, compressing with default tar")

    if exclude is None:
        exclude_part = ""
    elif isinstance(exclude, list):
        exclude_part = " ".join(["--exclude {}".format(x) for x in exclude])
    else:
        exclude_part = "--exclude {}".format(str(exclude))

    fname = os.path.basename(path)
    if os.path.isfile(path):
        path = os.path.dirname(path)
    else:
        path += "/.."

    cmd = "tar {} {} -cf {} -C {} {}".format(
        program_part, exclude_part, archive_path, path, fname
    )
    logging.debug("compress_fast cmd: %s", cmd)
    subprocess.check_call(cmd, shell=True)


def decompress_fast(archive_path, result_path=None):
    program_part = ""
    if archive_path.endswith(".zst"):
        logging.info(
            "zstd will be used for decompression ('%s' -> '%s')",
            archive_path,
            result_path,
        )
        program_part = "--use-compress-program='zstd --threads=0'"
    elif os.path.exists("/usr/bin/pigz"):
        logging.info(
            "pigz found, will compress and decompress faster ('%s' -> '%s')",
            archive_path,
            result_path,
        )
        program_part = "--use-compress-program='pigz'"
    else:
        program_part = "-z"
        logging.info(
            "no pigz, decompressing with default tar ('%s' -> '%s')",
            archive_path,
            result_path,
        )

    if result_path is None:
        subprocess.check_call(
            "tar {} -xf {}".format(program_part, archive_path), shell=True
        )
    else:
        subprocess.check_call(
            "tar {} -xf {} -C {}".format(program_part, archive_path, result_path),
            shell=True,
        )
