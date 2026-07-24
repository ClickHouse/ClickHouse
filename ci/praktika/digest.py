import dataclasses
import hashlib
import json
import os
from hashlib import md5
from pathlib import Path
from typing import List

from .docker import Docker
from .info import Info
from .job import Job
from .settings import Settings
from .utils import Shell, Utils


def _is_local_run():
    """Check if running locally. Returns False if can't determine (e.g., during workflow generation)."""
    try:
        return Info().is_local_run
    except Exception:
        # During workflow generation, Info can't be initialized - treat as CI mode
        return False


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

    @classmethod
    def get_null_digest(cls):
        return "f" * Settings.CACHE_DIGEST_LEN

    def calc_job_digest(self, job_config: Job.Config, docker_digests, artifact_configs):
        config = job_config.digest_config
        if not config:
            return self.get_null_digest()

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
            if config.with_git_submodules:
                submodules_shas = Shell.get_output(
                    "git submodule | awk '{print $1}' | sed 's/^[+-]//'", verbose=True
                )
                hash_md5.update(submodules_shas.encode())
            digest = hash_md5.hexdigest()[: Settings.CACHE_DIGEST_LEN]

        self.digest_cache[cache_key] = digest

        if (
            job_config.run_in_docker and ":" not in job_config.run_in_docker
        ):  # if : in image name - there is a tag, thus it's not managed by praktika e.g.: ubuntu:22.04
            # respect docker digest in the job digest
            docker_digest = docker_digests[job_config.run_in_docker.split("+")[0]]
            digest = "-".join([docker_digest, digest])

        job_config_dict = dataclasses.asdict(job_config)

        drop_fields = [
            "requires",
            "enable_commit_status",
            "allow_merge_on_failure",
            "digest_config",
        ]
        filtered_job_dict = {
            k: v for k, v in job_config_dict.items() if k not in drop_fields
        }
        # add Articat.Configs list to the job config dict so that changed Articat.Config object affects job digest
        job_provides_artifact_configs = []
        for a in job_config.provides:
            if a in artifact_configs:
                job_provides_artifact_configs.append(
                    dataclasses.asdict(artifact_configs[a])
                )
        filtered_job_dict["provides"] = job_provides_artifact_configs

        config_digest = hashlib.md5(
            json.dumps(filtered_job_dict, sort_keys=True).encode()
        ).hexdigest()[: min(Settings.CACHE_DIGEST_LEN // 4, 4)]
        return digest + "-" + config_digest

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
        if not _is_local_run():
            print(f"Calculate digest for docker [{docker_config.name}]")
        paths = Utils.traverse_path(docker_config.path, sorted=True)
        if not hash_md5:
            hash_md5 = hashlib.md5()

        dependencies = []
        for dependency_name in docker_config.depends_on:
            for dependency_config in dependency_configs:
                if dependency_config.name == dependency_name:
                    if not _is_local_run():
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
