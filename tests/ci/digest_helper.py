#!/usr/bin/env python3

from hashlib import md5
from logging import getLogger
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, Optional
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


def _digest_directory(directory: Path, hash_object: HASH) -> None:
    assert directory.is_dir()
    for p in sorted(directory.rglob("*")):
        if p.is_symlink() and p.is_dir():
            # The symlink directory is not listed recursively, so we process it manually
            (_digest_directory(p, hash_object))
        if p.is_file():
            (_digest_file(p, hash_object))


def digest_path(path: Path, hash_object: Optional[HASH] = None) -> HASH:
    """Calculates md5 (or updates existing hash_object) hash of the path, either it's
    directory or file"""
    hash_object = hash_object or md5()
    if path.is_dir():
        _digest_directory(path, hash_object)
    elif path.is_file():
        _digest_file(path, hash_object)
    return hash_object


def digest_paths(paths: Iterable[Path], hash_object: Optional[HASH] = None) -> HASH:
    """Calculates aggregated md5 (or updates existing hash_object) hash of passed paths.
    The order is processed as given"""
    hash_object = hash_object or md5()
    for path in paths:
        if path.exists():
            digest_path(path, hash_object)
    return hash_object


def digest_consistent_paths(
    paths: Iterable[Path], hash_object: Optional[HASH] = None
) -> HASH:
    """Calculates aggregated md5 (or updates existing hash_object) hash of passed paths.
    The order doesn't matter, paths are converted to `absolute` and ordered before
    calculation"""
    return digest_paths(sorted(p.absolute() for p in paths), hash_object)


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
