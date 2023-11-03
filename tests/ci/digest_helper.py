#!/usr/bin/env python3

from hashlib import md5
from logging import getLogger
from pathlib import Path
from typing import TYPE_CHECKING, Iterable
from sys import modules

if TYPE_CHECKING:
    from hashlib import (  # pylint:disable=no-name-in-module,ungrouped-imports
        _Hash as HASH,
    )
else:
    HASH = "_Hash"

logger = getLogger(__name__)


def _digest_file(file: Path) -> HASH:
    assert file.is_file()
    md5_hash = md5()
    with open(file, "rb") as fd:
        for chunk in iter(lambda: fd.read(4096), b""):
            md5_hash.update(chunk)
    return md5_hash


def _digest_directory(directory: Path) -> HASH:
    assert directory.is_dir()
    md5_hash = md5()
    for p in sorted(directory.rglob("*")):
        if p.is_symlink() and p.is_dir():
            # The symlink directory is not listed recursively, so we process it manually
            md5_hash.update(_digest_directory(p).digest())
        if p.is_file():
            md5_hash.update(_digest_file(p).digest())
    return md5_hash


def digest_path(path: Path) -> HASH:
    """Calculates md5 hash of the path, either it's directory or file"""
    if path.is_dir():
        return _digest_directory(path)
    if path.is_file():
        return _digest_file(path)
    return md5()


def digest_paths(paths: Iterable[Path]) -> HASH:
    """Calculates aggregated md5 hash of passed paths. The order matters"""
    md5_hash = md5()
    for path in paths:
        if path.exists():
            md5_hash.update(digest_path(path).digest())
    return md5_hash


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
                md5_hash.update(_digest_file(Path(script_path)).digest())
    except RuntimeError:
        logger.warning("The modules size has changed, retry calculating digest")
        return digest_script(path_str)
    return md5_hash
