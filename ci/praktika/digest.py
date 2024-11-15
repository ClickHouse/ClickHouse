import dataclasses
import hashlib
import os
from hashlib import md5
from pathlib import Path
from typing import List

from praktika import Job
from praktika.docker import Docker
from praktika.settings import Settings
from praktika.utils import Utils


class Digest:
    def __init__(self):
        self.digest_cache = {}

    @staticmethod
    def _hash_digest_config(digest_config: Job.CacheDigestConfig) -> str:
        data_dict = dataclasses.asdict(digest_config)
        hash_obj = md5()
        hash_obj.update(str(data_dict).encode())
        hash_string = hash_obj.hexdigest()
        return hash_string

    def calc_job_digest(self, job_config: Job.Config):
        config = job_config.digest_config
        if not config:
            return "f" * Settings.CACHE_DIGEST_LEN

        cache_key = self._hash_digest_config(config)

        if cache_key in self.digest_cache:
            return self.digest_cache[cache_key]

        included_files = Utils.traverse_paths(
            job_config.digest_config.include_paths,
            job_config.digest_config.exclude_paths,
            sorted=True,
        )

        print(
            f"calc digest for job [{job_config.name}]: hash_key [{cache_key}], include [{len(included_files)}] files"
        )
        # Sort files to ensure consistent hash calculation
        included_files.sort()

        # Calculate MD5 hash
        res = ""
        if not included_files:
            res = "f" * Settings.CACHE_DIGEST_LEN
            print(f"NOTE: empty digest config [{config}] - return dummy digest")
        else:
            hash_md5 = hashlib.md5()
            for file_path in included_files:
                res = self._calc_file_digest(file_path, hash_md5)
        assert res
        self.digest_cache[cache_key] = res
        return res

    def calc_docker_digest(
        self,
        docker_config: Docker.Config,
        dependency_configs: List[Docker.Config],
        hash_md5=None,
    ):
        """

        :param hash_md5:
        :param dependency_configs: list of Docker.Config(s) that :param docker_config: depends on
        :param docker_config: Docker.Config to calculate digest for
        :return:
        """
        print(f"Calculate digest for docker [{docker_config.name}]")
        paths = Utils.traverse_path(docker_config.path, sorted=True)
        if not hash_md5:
            hash_md5 = hashlib.md5()

        dependencies = []
        for dependency_name in docker_config.depends_on:
            for dependency_config in dependency_configs:
                if dependency_config.name == dependency_name:
                    print(
                        f"Add docker [{dependency_config.name}] as dependency for docker [{docker_config.name}] digest calculation"
                    )
                    dependencies.append(dependency_config)

        for dependency in dependencies:
            _ = self.calc_docker_digest(dependency, dependency_configs, hash_md5)

        for path in paths:
            _ = self._calc_file_digest(path, hash_md5=hash_md5)

        return hash_md5.hexdigest()[: Settings.CACHE_DIGEST_LEN]

    @staticmethod
    def _calc_file_digest(file_path, hash_md5):
        # Resolve file path if it's a symbolic link
        resolved_path = file_path
        if Path(file_path).is_symlink():
            resolved_path = os.path.realpath(file_path)
            if not Path(resolved_path).is_file():
                print(
                    f"WARNING: No valid file resolved by link {file_path} -> {resolved_path} - skipping digest calculation"
                )
                return hash_md5.hexdigest()[: Settings.CACHE_DIGEST_LEN]

        with open(resolved_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)

        return hash_md5.hexdigest()[: Settings.CACHE_DIGEST_LEN]
