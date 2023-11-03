#!/usr/bin/env python3

import bisect
from hashlib import md5
from logging import getLogger
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, List, Optional, Union
from sys import modules

if TYPE_CHECKING:
    from hashlib import (  # pylint:disable=no-name-in-module,ungrouped-imports
        _Hash as HASH,
    )
else:
    HASH = "_Hash"

logger = getLogger(__name__)


def _digest_file(file: Path, hash_object: HASH) -> None:
    assert file.is_file()
    with open(file, "rb") as fd:
        for chunk in iter(lambda: fd.read(4096), b""):
            hash_object.update(chunk)


def digest_path(
    path: Union[Path, str],
    hash_object: Optional[HASH] = None,
    exclude_files: Optional[Iterable[str]] = None,
    exclude_dirs: Optional[Iterable[str]] = None,
) -> HASH:
    """Calculates md5 (or updates existing hash_object) hash of the path, either it's
    directory or file
    @exclude_files - file extension(s) or any filename suffix(es) that you want to exclude from digest
    @exclude_dirs - dir names that you want to exclude from digest
    """
    path = Path(path)
    hash_object = hash_object or md5()
    if path.is_file():
        if not exclude_files or not any(path.name.endswith(x) for x in exclude_files):
            _digest_file(path, hash_object)
    elif path.is_dir():
        if not exclude_dirs or not any(path.name == x for x in exclude_dirs):
            for p in sorted(path.iterdir()):
                digest_path(p, hash_object, exclude_files, exclude_dirs)
    else:
        pass  # broken symlink
    return hash_object


def digest_paths(
    paths: Iterable[Union[Path, str]],
    hash_object: Optional[HASH] = None,
    exclude_files: Optional[Iterable[str]] = None,
    exclude_dirs: Optional[Iterable[str]] = None,
) -> HASH:
    """Calculates aggregated md5 (or updates existing hash_object) hash of passed paths.
    The order is processed as given"""
    hash_object = hash_object or md5()
    paths_all: List[str] = []
    for p in paths:
        if isinstance(p, str) and "*" in p:
            for path in Path(".").glob(p):
                bisect.insort(paths_all, path.absolute())
        else:
            bisect.insort(paths_all, Path(p).absolute())
    for path in paths_all:
        if path.exists():
            digest_path(path, hash_object, exclude_files, exclude_dirs)
        else:
            raise AssertionError(f"Invalid path: {path}")
    return hash_object


def digest_script(path_str: str) -> HASH:
    """Accepts value of the __file__ executed script and calculates the md5 hash for it"""
    path = Path(path_str)
    parent = path.parent
    md5_hash = md5()
    try:
        for script in modules.values():
            script_path = getattr(script, "__file__", "")
            if parent.absolute().as_posix() in script_path:
                logger.debug("Updating the hash with %s", script_path)
                _digest_file(Path(script_path), md5_hash)
    except RuntimeError:
        logger.warning("The modules size has changed, retry calculating digest")
        return digest_script(path_str)
    return md5_hash


def digest_string(string: str) -> str:
    hash_object = md5()
    hash_object.update(string.encode("utf-8"))
    return hash_object.hexdigest()
