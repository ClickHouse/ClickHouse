import dataclasses
import os
from typing import Dict, List

from .settings import Settings
from .utils import Shell, Utils

# Matched against the pull's stderr. Transport-class phrases only: must never match a
# permanent failure (`manifest unknown`, `pull access denied`, `no matching manifest`).
_IMAGE_PULL_RETRY_ERRORS = [
    "connection reset by peer",
    "connection refused",
    "TLS handshake timeout",
    "i/o timeout",
    "unexpected EOF",
    # A nameserver answered badly (SERVFAIL), so the name can resolve next attempt.
    # Its NXDOMAIN sibling `no such host` is permanent and is deliberately absent.
    "server misbehaving",
    # What `timeout --verbose` writes when it kills a stalled attempt. Plain `timeout`
    # writes nothing, so without this entry a stall is not retried.
    "sending signal TERM to command",
]
_IMAGE_PULL_TIMEOUT_S = 300  # per attempt, matching prefetch-integration-test-images
_IMAGE_PULL_RETRIES = 3


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
        # Extra `--build-arg NAME=VALUE` passed to `docker buildx build` for this
        # image (e.g. apt_archive / apt_ports_archive to point apt at an in-region
        # mirror). Images that don't declare the arg silently ignore it.
        build_args: Dict[str, str] = dataclasses.field(default_factory=dict)

    @classmethod
    def build(
        cls, config: "Docker.Config", digests, amd_only, arm_only, disable_push=False
    ):
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
        # A successful inspect is the only evidence that the image is already there.
        # A missing tag in an existing repository reports "no such manifest", but the
        # first ever build of a new image reports "denied: requested access to the
        # resource is denied" instead, because the repository itself does not exist
        # yet - and treating that as "image exists" leaves the manifest merge with
        # nothing to merge.
        if code != 0:
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

            build_args = "".join(
                f" --build-arg {name}={value}"
                for name, value in config.build_args.items()
            )

            if disable_push:
                push_out = ""
            else:
                push_out = (
                    " --output type=image,push=true"
                    f",compression={Settings.DOCKER_LAYER_COMPRESSION}"
                    f",compression-level={Settings.DOCKER_LAYER_COMPRESSION_LEVEL}"
                    ",force-compression=true"
                )

            command = f"docker buildx build {tags_substr} {from_tag}{build_args} --platform {','.join(platforms)} --provenance=mode=max --attest=type=sbom,generator=docker/buildkit-syft-scanner:1.11 {config.path}{push_out}"

            return Result.from_commands_run(
                name=name,
                command=command,
                retry_errors=[
                    "Error response from daemon: manifest unknown: manifest unknown",
                ],
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

        # Use imagetools create instead of manifest create/push: when images are
        # built with --sbom=true --provenance=mode=max, buildx produces OCI image
        # indices (not plain manifests), which docker manifest create cannot handle.
        # imagetools create works correctly with both plain manifests and indices,
        # preserving attestation manifests in the merged result.
        src_refs = " ".join(f"{config.name}:{t}" for t in tags[1:])
        commands = [
            f"docker buildx imagetools create --tag {config.name}:{digests[config.name]} {src_refs}"
        ]

        if add_latest:
            commands.append(
                f"docker buildx imagetools create --tag {config.name}:latest {src_refs}"
            )

        return Result.from_commands_run(
            name=f"merge: {config.name}:{digests[config.name]} (latest={add_latest})",
            command=commands,
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
    def pull_image(
        cls,
        image,
        *,
        strict=False,
        on_retry=None,
        verbose=True,
        timeout_s=_IMAGE_PULL_TIMEOUT_S,
        retries=_IMAGE_PULL_RETRIES,
    ):
        """Pull `image`, retrying only transport-class failures.

        `strict` raises on a failed pull; `on_retry(matched, attempt, attempts)`
        is called once per actual retry, so a caller with a report surface can
        make the retry visible. Returns the pull's exit code.

        `timeout_s` bounds one attempt and `retries` caps their number; a caller
        that pays the retries out of its own job timeout passes a budget that
        fits inside it.
        """
        # Below these floors the budget silently widens: `timeout 0` runs unbounded,
        # and Shell.run raises `retries` to 2 whenever `retry_errors` is set.
        assert timeout_s >= 1, f"timeout_s must be >= 1, got [{timeout_s}]"
        assert retries >= 2, f"retries must be >= 2, got [{retries}]"
        return Shell.run(
            f"timeout --verbose {timeout_s} docker pull {image}",
            strict=strict,
            retries=retries,
            retry_errors=_IMAGE_PULL_RETRY_ERRORS,
            verbose=verbose,
            on_retry=on_retry,
        )

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

    @classmethod
    def find_affected_docker_images(
        cls, docker_configs: List["Docker.Config"], changed_files: List[str]
    ) -> List[str]:
        if not changed_files:
            return []

        # Normalize all changed file paths
        normalized_files = [os.path.normpath(f) for f in changed_files]

        # Map name → Docker.Config
        name_to_config = {cfg.name: cfg for cfg in docker_configs}
        affected = set()

        def is_path_affected(path: str) -> bool:
            normalized_path = os.path.normpath(path)
            return any(
                f.startswith(normalized_path + os.sep) or f == normalized_path
                for f in normalized_files
            )

        def collect_affected(config):
            if config.name in affected:
                return
            if is_path_affected(config.path):
                affected.add(config.name)
                return
            for dep_name in config.depends_on:
                dep = name_to_config.get(dep_name)
                if dep:
                    collect_affected(dep)
                    if dep.name in affected:
                        affected.add(config.name)
                        return

        for config in docker_configs:
            collect_affected(config)

        return sorted(affected)
