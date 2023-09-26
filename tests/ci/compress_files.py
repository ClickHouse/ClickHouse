#!/usr/bin/env python3
import subprocess
import logging

from pathlib import Path
from typing import Optional


PIGZ = Path("/usr/bin/pigz")


def compress_file_fast(path: Path, archive_path: Path) -> None:
    if archive_path.suffix == ".zst":
        subprocess.check_call(f"zstd < {path} > {archive_path}", shell=True)
    elif PIGZ.exists():
        subprocess.check_call(f"pigz < {path} > {archive_path}", shell=True)
    else:
        subprocess.check_call(f"gzip < {path} > {archive_path}", shell=True)


def compress_fast(
    path: Path, archive_path: Path, exclude: Optional[Path] = None
) -> None:
    program_part = ""
    if archive_path.suffix == ".zst":
        logging.info("zstd will be used for compression")
        program_part = "--use-compress-program='zstd --threads=0'"
    elif PIGZ.exists():
        logging.info("pigz found, will compress and decompress faster")
        program_part = "--use-compress-program='pigz'"
    else:
        program_part = "-z"
        logging.info("no pigz, compressing with default tar")

    if exclude is None:
        exclude_part = ""
    elif isinstance(exclude, list):
        exclude_part = " ".join([f"--exclude {x}" for x in exclude])
    else:
        exclude_part = f"--exclude {exclude}"

    fname = path.name

    cmd = (
        f"tar {program_part} {exclude_part} -cf {archive_path} -C {path.parent} {fname}"
    )
    logging.debug("compress_fast cmd: %s", cmd)
    subprocess.check_call(cmd, shell=True)


def decompress_fast(archive_path: Path, result_path: Optional[Path] = None) -> None:
    program_part = ""
    if archive_path.suffix == ".zst":
        logging.info(
            "zstd will be used for decompression ('%s' -> '%s')",
            archive_path,
            result_path,
        )
        program_part = "--use-compress-program='zstd --threads=0'"
    elif PIGZ.exists():
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
        subprocess.check_call(f"tar {program_part} -xf {archive_path}", shell=True)
    else:
        subprocess.check_call(
            f"tar {program_part} -xf {archive_path} -C {result_path}",
            shell=True,
        )
