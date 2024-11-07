import dataclasses
from typing import List

from praktika.utils import Shell


class Docker:
    class Platforms:
        ARM = "linux/arm64"
        AMD = "linux/amd64"
        arm_amd = [ARM, AMD]

    @dataclasses.dataclass
    class Config:
        name: str
        path: str
        depends_on: List[str]
        platforms: List[str]

    @classmethod
    def build(cls, config: "Docker.Config", log_file, digests, add_latest):
        tags_substr = f" -t {config.name}:{digests[config.name]}"
        if add_latest:
            tags_substr = f" -t {config.name}:latest"

        from_tag = ""
        if config.depends_on:
            assert (
                len(config.depends_on) == 1
            ), f"Only one dependency in depends_on is currently supported, docker [{config}]"
            from_tag = f" --build-arg FROM_TAG={digests[config.depends_on[0]]}"

        command = f"docker buildx build --platform {','.join(config.platforms)} {tags_substr} {from_tag} --cache-to type=inline --cache-from type=registry,ref={config.name} --push {config.path}"
        return Shell.run(command, log_file=log_file, verbose=True)

    @classmethod
    def sort_in_build_order(cls, dockers: List["Docker.Config"]):
        ready_names = []
        i = 0
        while i < len(dockers):
            docker = dockers[i]
            if not docker.depends_on or all(
                dep in ready_names for dep in docker.depends_on
            ):
                ready_names.append(docker.name)
                i += 1
            else:
                dockers.append(dockers.pop(i))
        return dockers

    @classmethod
    def login(cls, user_name, user_password):
        print("Docker: log in to dockerhub")
        return Shell.check(
            f"docker login --username '{user_name}' --password-stdin",
            strict=True,
            stdin_str=user_password,
            encoding="utf-8",
            verbose=True,
        )
