import dataclasses
import hashlib
import os
from hashlib import md5
from pathlib import Path
from typing import List

from . import Job
from .docker import Docker
from .settings import Settings
from .utils import Utils


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

    def calc_job_digest(self, job_config: Job.Config, docker_digests):
        config = job_config.digest_config
        if not config:
            return "f" * Settings.CACHE_DIGEST_LEN

        cache_key = self._hash_digest_config(config)

        if cache_key in self.digest_cache:
            print(
                f"calc digest for job [{job_config.name}]: hash_key [{cache_key}] - from cache"
            )
            digest = self.digest_cache[cache_key]
        else:
            included_files = Utils.traverse_paths(
                job_config.digest_config.include_paths,
                job_config.digest_config.exclude_paths,
                sorted=True,
            )
            print(
                f"calc digest for job [{job_config.name}]: hash_key [{cache_key}], include [{len(included_files)}] files"
            )

            hash_md5 = hashlib.md5()
            for i, file_path in enumerate(included_files):
                hash_md5 = self._calc_file_digest(file_path, hash_md5)
            digest = hash_md5.hexdigest()[: Settings.CACHE_DIGEST_LEN]
            self.digest_cache[cache_key] = digest

        if job_config.run_in_docker:
            # respect docker digest in the job digest
            docker_digest = docker_digests[job_config.run_in_docker.split("+")[0]]
            digest = "-".join([docker_digest, digest])

        return digest

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
                return hash_md5

        with open(resolved_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)

        return hash_md5
