#!/usr/bin/env python3

import bisect
from dataclasses import asdict
from hashlib import md5
from logging import getLogger
from pathlib import Path
from sys import modules
from typing import TYPE_CHECKING, Dict, Iterable, List, Optional, Union

from ci_config import CI
from ci_utils import cd
from docker_images_helper import get_images_info
from env_helper import ROOT_DIR
from git_helper import Runner

DOCKER_DIGEST_LEN = 12
JOB_DIGEST_LEN = 10

if TYPE_CHECKING:
    from hashlib import (
        _Hash as HASH,  # pylint:disable=no-name-in-module,ungrouped-imports
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
    exclude_dirs: Optional[Iterable[Union[Path, str]]] = None,
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
    exclude_dirs: Optional[Iterable[Union[Path, str]]] = None,
) -> HASH:
    """Calculates aggregated md5 (or updates existing hash_object) hash of passed paths.
    The order is processed as given"""
    hash_object = hash_object or md5()
    paths_all: List[Path] = []
    with cd(ROOT_DIR):
        for p in paths:
            if isinstance(p, str) and "*" in p:
                for path in Path(".").glob(p):
                    bisect.insort(paths_all, path.absolute())  # type: ignore[misc]
            else:
                bisect.insort(paths_all, Path(p).absolute())  # type: ignore[misc]
        for path in paths_all:  # type: ignore
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
    with cd(ROOT_DIR):
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


class DockerDigester:
    EXCLUDE_FILES = [".md"]

    def __init__(self):
        self.images_info = get_images_info()
        assert self.images_info, "Fetch image info error"

    def get_image_digest(self, name: str) -> str:
        assert isinstance(name, str)
        with cd(ROOT_DIR):
            deps = [name]
            digest = None
            while deps:
                dep_name = deps.pop(0)
                digest = digest_path(
                    self.images_info[dep_name]["path"],
                    digest,
                    exclude_files=self.EXCLUDE_FILES,
                )
                deps += self.images_info[dep_name]["deps"]
            assert digest
        return digest.hexdigest()[0:DOCKER_DIGEST_LEN]

    def get_all_digests(self) -> Dict:
        res = {}
        for image_name in self.images_info:
            res[image_name] = self.get_image_digest(image_name)
        return res


class JobDigester:
    def __init__(self, dry_run: bool = False):
        self.dd = DockerDigester()
        self.cache: Dict[str, str] = {}
        self.dry_run = dry_run

    @staticmethod
    def _get_config_hash(digest_config: CI.DigestConfig) -> str:
        data_dict = asdict(digest_config)
        hash_obj = md5()
        hash_obj.update(str(data_dict).encode())
        hash_string = hash_obj.hexdigest()
        return hash_string

    def get_job_digest(self, digest_config: CI.DigestConfig) -> str:
        if not digest_config.include_paths or self.dry_run:
            # job is not for digest
            return "f" * JOB_DIGEST_LEN

        cache_key = self._get_config_hash(digest_config)
        if cache_key in self.cache:
            return self.cache[cache_key]

        digest_str: List[str] = []
        if digest_config.include_paths:
            digest = digest_paths(
                digest_config.include_paths,
                hash_object=None,
                exclude_files=digest_config.exclude_files,
                exclude_dirs=digest_config.exclude_dirs,
            )
            digest_str += (digest.hexdigest(),)
        if digest_config.docker:
            for image_name in digest_config.docker:
                image_digest = self.dd.get_image_digest(image_name)
                digest_str += (image_digest,)
        if digest_config.git_submodules:
            submodules_sha = Runner().run(
                "git submodule | awk '{print $1}' | sed 's/^[+-]//'"
            )
            assert submodules_sha and len(submodules_sha) > 10
            submodules_digest = digest_string("-".join(submodules_sha))
            digest_str += (submodules_digest,)
        res = digest_string("-".join(digest_str))[0:JOB_DIGEST_LEN]
        self.cache[cache_key] = res
        return res
