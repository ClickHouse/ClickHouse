#!/usr/bin/env python3
import subprocess
import logging
import os


def compress_file_fast(path, archive_path):
    if os.path.exists("/usr/bin/pigz"):
        subprocess.check_call("pigz < {} > {}".format(path, archive_path), shell=True)
    else:
        subprocess.check_call("gzip < {} > {}".format(path, archive_path), shell=True)


def compress_fast(path, archive_path, exclude=None):
    pigz_part = ""
    if os.path.exists("/usr/bin/pigz"):
        logging.info("pigz found, will compress and decompress faster")
        pigz_part = "--use-compress-program='pigz'"
    else:
        pigz_part = "-z"
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
        pigz_part, exclude_part, archive_path, path, fname
    )
    logging.debug("compress_fast cmd: %s", cmd)
    subprocess.check_call(cmd, shell=True)


def decompress_fast(archive_path, result_path=None):
    pigz_part = ""
    if os.path.exists("/usr/bin/pigz"):
        logging.info(
            "pigz found, will compress and decompress faster ('%s' -> '%s')",
            archive_path,
            result_path,
        )
        pigz_part = "--use-compress-program='pigz'"
    else:
        pigz_part = "-z"
        logging.info(
            "no pigz, decompressing with default tar ('%s' -> '%s')",
            archive_path,
            result_path,
        )

    if result_path is None:
        subprocess.check_call(
            "tar {} -xf {}".format(pigz_part, archive_path), shell=True
        )
    else:
        subprocess.check_call(
            "tar {} -xf {} -C {}".format(pigz_part, archive_path, result_path),
            shell=True,
        )
