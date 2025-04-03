import dataclasses
from typing import List

from .utils import Shell, Utils


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
    def build(cls, config: "Docker.Config", digests, amd_only, arm_only, with_log):
        from .result import Result

        sw = Utils.Stopwatch()
        tag = digests[config.name]
        if amd_only:
            aarch_suffix = "_amd"
        elif arm_only:
            aarch_suffix = "_arm"
        else:
            aarch_suffix = ""
        tag += aarch_suffix
        name = f"build: {config.name}:{tag}"

        code, out, err = Shell.get_res_stdout_stderr(
            f"docker manifest inspect {config.name}:{tag}"
        )
        print(
            f"Docker inspect results for {config.name}:{tag}: exit code [{code}], out [{out}], err [{err}]"
        )
        if "no such manifest" in err:
            tags_substr = f" -t {config.name}:{tag}"

            from_tag = ""
            if config.depends_on:
                assert (
                    len(config.depends_on) == 1
                ), f"Only one dependency in depends_on is currently supported, docker [{config}]"
                from_tag = f" --build-arg FROM_TAG={digests[config.depends_on[0]]}{aarch_suffix}"

            platforms = []
            for platform in config.platforms:
                if amd_only and "amd" not in platform:
                    continue
                if arm_only and "arm" not in platform:
                    continue
                platforms.append(platform)

            command = f"docker buildx build --builder default {tags_substr} {from_tag} --platform {','.join(platforms)} --cache-to type=inline --cache-from type=registry,ref={config.name} {config.path} --push"

            return Result.from_commands_run(
                name=name, command=command, with_info=with_log
            )
        else:
            return Result(
                name=name,
                status=Result.Status.SKIPPED,
                info="image exists",
                start_time=sw.start_time,
                duration=sw.duration,
            )

    @classmethod
    def merge_manifest(
        cls, config: "Docker.Config", digests, add_latest, with_log=False
    ):

        from .result import Result

        tags = [digests[config.name]]

        for platform in config.platforms:
            if platform == Docker.Platforms.AMD:
                tags.append(f"{digests[config.name]}_amd")
            elif platform == Docker.Platforms.ARM:
                tags.append(f"{digests[config.name]}_arm")
            else:
                assert f"Not supported platform [{platform}]"

        commands = [
            "docker manifest create --amend "
            + " ".join(f"{config.name}:{t}" for t in tags)
        ]
        commands.append(f"docker manifest push {config.name}:{digests[config.name]}")

        if add_latest:
            tags[0] = "latest"
            commands += [
                "docker manifest create --amend "
                + " ".join(f"{config.name}:{t}" for t in tags)
            ]
            commands.append(f"docker manifest push {config.name}:latest")

        return Result.from_commands_run(
            name=f"merge: {config.name}:{digests[config.name]} (latest={add_latest})",
            command=commands,
            with_info=with_log,
            fail_fast=True,
        )

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
