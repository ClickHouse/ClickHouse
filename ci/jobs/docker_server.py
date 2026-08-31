import argparse
import atexit
import json
import logging
import os
import shlex
import tempfile
import traceback
from pathlib import Path
from typing import Dict, List

from ci.defs.job_configs import JobConfigs
from ci.jobs.scripts.clickhouse_version import CHVersion
from ci.praktika import Secret
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

ARCH = ("amd64", "arm64")

temp_path = Path(f"{Utils.cwd()}/ci/tmp")

GITHUB_SERVER_URL = os.getenv("GITHUB_SERVER_URL", "https://github.com")
with tempfile.NamedTemporaryFile("w", delete=False) as f:
    GIT_KNOWN_HOSTS_FILE = f.name
    GIT_PREFIX = (  # All commits to remote are done as robot-clickhouse
        "git -c user.email=robot-clickhouse@users.noreply.github.com "
        "-c user.name=robot-clickhouse -c commit.gpgsign=false "
        "-c core.sshCommand="
        f"'ssh -o UserKnownHostsFile={GIT_KNOWN_HOSTS_FILE} "
        "-o StrictHostKeyChecking=accept-new'"
    )
    atexit.register(os.remove, f.name)


def read_build_urls(build_name: str):
    artifact_report = temp_path / f"artifact_report_build_{build_name}.json"
    if artifact_report.is_file():
        with open(artifact_report, "r", encoding="utf-8") as f:
            return json.load(f)["build_urls"]
    return []


class DockerImageData:
    def __init__(self, name: str, path: str):
        self.name = name
        assert not path.startswith("/")
        self.path = path


def is_distroless_image(docker_image: str) -> bool:
    _, tag = docker_image.rsplit(":", 1)
    return "distroless" in tag.split("-")


def get_official_images_variant(docker_image: str) -> str:
    # The official-images test runner derives its lookup variant from the final
    # tag suffix. For example, head-distroless-amd64 is looked up as repo:amd64.
    _, tag = docker_image.rsplit(":", 1)
    return tag.rsplit("-", 1)[-1]


def write_distroless_docker_library_config(docker_image: str, config_dir: Path) -> Path:
    """Map arch-suffixed distroless tags to the distroless-safe config tests."""
    # Generate a short config fragment for local arch-suffixed distroless CI tags.
    # The runner derives tags like head-distroless-amd64 as repo:amd64; map that
    # derived key to the distroless-safe tests because this helper is only used
    # for images already identified as distroless.
    repo, _ = docker_image.rsplit(":", 1)
    variant = get_official_images_variant(docker_image)
    image_variant = shlex.quote(f"{repo}:{variant}")
    tests_var = (
        "keeperDistrolessSafeTests"
        if "clickhouse-keeper" in repo
        else "clickhouseDistrolessSafeTests"
    )

    generated_config = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            prefix="docker-library-distroless-",
            suffix=".sh",
            dir=config_dir,
            delete=False,
            encoding="utf-8",
        ) as f:
            generated_config = Path(f.name)
            f.write(
                "#!/usr/bin/env bash\n"
                "\n"
                "explicitTests+=(\n"
                f"\t[{image_variant}]=1\n"
                ")\n"
                "\n"
                "imageTests+=(\n"
                f"\t[{image_variant}]=\"${{{tests_var}}}\"\n"
                ")\n"
            )
            return generated_config
    except Exception:
        if generated_config:
            generated_config.unlink(missing_ok=True)
        raise


class DelOS(argparse.Action):
    def __call__(self, _, namespace, __, option_string=None):
        no_build = self.dest[3:] if self.dest.startswith("no_") else self.dest
        if no_build in namespace.os:
            namespace.os.remove(no_build)


def docker_login(relogin: bool = True) -> None:
    if relogin or not Shell.check(
        "docker system info | grep --quiet -E 'Username|Registry'"
    ):
        Shell.check(
            "docker login --username 'robotclickhouse' --password-stdin",
            strict=True,
            stdin_str=Secret.Config(
                "dockerhub_robot_password",
                type=Secret.Type.AWS_SSM_PARAMETER,
                region="us-east-1",
            ).get_value(),
            encoding="utf-8",
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="A program to build clickhouse-server image, both alpine and "
        "ubuntu versions",
    )
    parser.add_argument(
        "--tag-type",
        type=str,
        choices=("head", "release", "release-latest"),
        default="head",
        help="defines required tags for resulting docker image. "
        "head - for master image (tag: head) "
        "release - for release image (tags: XX, XX.XX, XX.XX.XX, XX.XX.XX.XX) "
        "release-latest - for latest release image (tags: XX, XX.XX, XX.XX.XX, XX.XX.XX.XX, latest) ",
    )
    parser.add_argument(
        "--image-path",
        type=str,
        default="",
        help="a path to docker context directory",
    )
    parser.add_argument(
        "--image-repo",
        type=str,
        default="",
        help="image name on docker hub",
    )
    parser.add_argument(
        "--bucket-prefix",
        help="if set, then is used as source for deb and tgz files",
    )
    parser.add_argument("--reports", default=True, help=argparse.SUPPRESS)
    parser.add_argument("--push", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--os", default=["ubuntu", "alpine", "distroless"], help=argparse.SUPPRESS)
    parser.add_argument(
        "--no-ubuntu",
        action=DelOS,
        nargs=0,
        default=argparse.SUPPRESS,
        help="don't build ubuntu image",
    )
    parser.add_argument(
        "--no-alpine",
        action=DelOS,
        nargs=0,
        default=argparse.SUPPRESS,
        help="don't build alpine image",
    )
    parser.add_argument(
        "--no-distroless",
        action=DelOS,
        nargs=0,
        default=argparse.SUPPRESS,
        help="don't build distroless image",
    )
    parser.add_argument(
        "--allow-build-reuse",
        action="store_true",
        help="allows binaries built on different branch if source digest matches current repo state",
    )
    parser.add_argument(
        "--apt-mirror-region",
        type=str,
        default="",
        help="if set, point apt at the in-region AWS Ubuntu mirror for this region "
        "(e.g. us-east-1) instead of Canonical's archive.ubuntu.com / "
        "ports.ubuntu.com, which are frequently unreachable from the runners. "
        "An image whose retries are exhausted on an apt download failure is then "
        "rebuilt against Canonical, for the days when the in-region mirror is the "
        "broken one. Empty means use the Dockerfile default (canonical mirror) "
        "with no second attempt.",
    )

    return parser.parse_args()


def gen_tags(version_str: str, tag_type: str) -> List[str]:
    """
    @tag_type release-latest, @version 22.2.2.2:
    - latest
    - 22
    - 22.2
    - 22.2.2
    - 22.2.2.2
    @tag_type release, @version 22.2.2.2:
    - 22
    - 22.2
    - 22.2.2
    - 22.2.2.2
    @tag_type head:
    - head
    """
    parts = version_str.split(".")
    tags = []
    if tag_type == "release-latest":
        tags.append("latest")
        for i in range(len(parts)):
            tags.append(".".join(parts[: i + 1]))
    elif tag_type == "head":
        tags.append(tag_type)
    elif tag_type == "release":
        for i in range(len(parts)):
            tags.append(".".join(parts[: i + 1]))
    else:
        assert False, f"Invalid release type [{tag_type}]"
    return tags


# `docker buildx build` resolves base/SBOM-scanner images such as
# `docker/buildkit-syft-scanner` (pulled for the SBOM attestation) from docker.io, which
# intermittently returns transient HTTP errors while resolving and while pushing
# image layers, and the build itself hits `apt-get` package mirrors that occasionally
# refuse connections. Retry the buildx commands only on genuine
# registry/network/mirror *failure* signatures. None of these strings appear in
# normal `--progress=plain` output (unlike progress text such as "resolve image
# config"), so a real Dockerfile/build error (RUN/COPY/package install) still fails
# fast on the first attempt. The count is bounded by the job budget below.
BUILDX_RETRIES = 2
BUILDX_RETRY_ERRORS = [
    # Docker registry (docker.io / registry-1.docker.io)
    "failed to do request",
    "unexpected status from HEAD request",
    "500 Internal Server Error",
    "502 Bad Gateway",
    "503 Service Unavailable",
    "504 Gateway Timeout",
    "429 Too Many Requests",
    # Network / TLS
    "TLS handshake timeout",
    "i/o timeout",
    "connection reset by peer",
    "connection refused",
    "unexpected EOF",
    # apt-get package mirrors
    "Failed to fetch",
    "Connection failed",
    "Connection timed out",
]

# A single apt mirror can stay broken for longer than `BUILDX_RETRIES` attempts
# take: on 2026-08-21 `us-east-1.ec2.ports.ubuntu.com` answered `503` and served
# ~20 kB/s for half an hour, which is long enough to exhaust every retry and red
# the arm64 server image on master. When the retries against one mirror are used
# up on an apt *download* failure, the whole build is repeated against the next
# mirror in `apt_mirror_variants` instead of failing the check. These signatures
# say "this mirror did not hand over the file"; a broken package name, an
# unsatisfiable dependency or any other genuine packaging error produces none of
# them and still fails right away.
APT_MIRROR_ERRORS = [
    # `apt-get install` could not download a .deb, and the summary line after it
    "Failed to fetch",
    "Unable to fetch some archives",
    # `apt-get update` could not refresh the package lists
    "Some index files failed to download",
]


# `--progress=plain` prints the whole build, so a signature found anywhere in the
# output says nothing about what stopped the build: `apt-get update` warns with
# `W: Failed to fetch ...` and carries on, and the step that actually failed can be a
# later `wget` or `dpkg` with a genuine packaging error. Two things narrow the match
# to the failure itself - the fenced context block that buildx prints for the step it
# stopped on, and apt's own `E:` severity, which distinguishes "could not get the
# file" from a miss apt recovered from.
BUILDX_FAILURE_FENCE = "------"
# buildx has reworded this line: it used to read `ERROR: failed to solve: ...` and
# since 0.23 reads `ERROR: failed to build: failed to solve: ...` (measured on 0.30.1).
# Matching the whole phrase pinned the old wording, so `terminal_build_failure` found
# no terminal step, every failure was classified as "not the mirror", and the fallback
# below was dead code from the day it was added: on 2026-08-26 the arm64 server and
# keeper images reded on master on a `503` from `us-east-1.ec2.ports.ubuntu.com`
# without ever rebuilding against Canonical. Anchor on the two parts that did not
# move - buildx starts its top-level errors at the beginning of the line with
# `ERROR: `, and this one names the solve step - so a future infix cannot break it.
BUILDX_ERROR_PREFIX = "ERROR: "
BUILDX_SOLVE_ERROR = "failed to solve:"
# Inside the fence, buildx repeats the failing step's command as a header before its
# output. That is the recipe, not what happened: `RUN ... && apt-get install ...` names
# apt, and a `RUN` that echoes or greps for one of the signatures below would match on
# its own text alone. Only the output lines get a verdict.
BUILDX_STEP_HEADER_PREFIX = " > "
APT_ERROR_SEVERITY = "E: "


def is_buildx_solve_error(line: str) -> bool:
    """Is this buildx's own top-level "the build failed" line?

    The step output that precedes it repeats the same text, so requiring the line to
    *start* with `ERROR: ` is what keeps the per-step `#12 ERROR: ...` copy and the
    fenced `61.2 E: ...` body from standing in for the terminal line.
    """
    return line.startswith(BUILDX_ERROR_PREFIX) and BUILDX_SOLVE_ERROR in line


def terminal_build_failure(info: str) -> str:
    """Output of the step the build stopped on, fences included.

    A failed `docker buildx build --progress=plain` ends with the failing step's own
    output fenced between `------` lines, then the offending `Dockerfile` lines, then a
    top-level `ERROR: ... failed to solve: ...`. Everything above that fence belongs to
    steps that succeeded, and everything below it is source, not output - so the block
    ends at the closing fence, while the top-level error still has to be there as proof
    that this is where the build stopped. Empty when the shape is absent - a timed-out
    or truncated log - so callers fail closed.
    """
    lines = info.splitlines()
    # Retries truncate the log, so a log with several attempts in it is one that was
    # concatenated; either way the last solve error is the one that ended the build.
    ends = [i for i, line in enumerate(lines) if is_buildx_solve_error(line)]
    if not ends:
        return ""
    end = ends[-1]
    fences = [
        i for i, line in enumerate(lines[:end]) if line.strip() == BUILDX_FAILURE_FENCE
    ]
    if len(fences) < 2:
        return ""
    return "\n".join(lines[fences[-2] : fences[-1] + 1])


def is_apt_mirror_failure(info: str) -> bool:
    """Did the step that stopped the build fail because a mirror withheld a file?"""
    return any(
        not line.startswith(BUILDX_STEP_HEADER_PREFIX)
        and APT_ERROR_SEVERITY in line
        and any(error in line for error in APT_MIRROR_ERRORS)
        for line in terminal_build_failure(info).splitlines()
    )


def apt_mirror_variants(apt_mirror_region: str) -> List[List[str]]:
    """buildx `--build-arg` sets pointing apt at each mirror to try, in order."""
    # An empty set of arguments leaves the Dockerfile defaults in place, i.e.
    # Canonical's archive.ubuntu.com (amd64) and ports.ubuntu.com (arm64).
    canonical: List[str] = []
    if not apt_mirror_region:
        return [canonical]
    # The in-region AWS mirror goes first: Canonical's mirrors are frequently
    # unreachable over IPv4 from the runners, while the in-region one is normally
    # reachable and fast. Canonical is kept as the second attempt for the days
    # when the in-region mirror is the broken one.
    in_region = [
        f"--build-arg=apt_archive=http://{apt_mirror_region}.ec2.archive.ubuntu.com",
        f"--build-arg=apt_ports_archive=http://{apt_mirror_region}.ec2.ports.ubuntu.com",
    ]
    return [in_region, canonical]

# `Acquire::http::Timeout` is an inactivity timeout, so an apt mirror that keeps
# trickling bytes is never bounded by it and the build runs until the job's own cap
# kills it. Bound each invocation, above the slowest healthy attempt seen in 90 days.
BUILDX_TIMEOUT = 2700
BUILDX_TIMEOUT_MESSAGE = "ERROR: docker buildx timed out"
# Logged by `timeout --verbose` under LC_ALL=C when it escalates to SIGKILL; a bare 137
# is ambiguous (OOM, external kill), so only this proves the expiry. Same discrimination
# as clickhouse_proc.py's _TIMEOUT_KILL_DIAG.
BUILDX_TIMEOUT_KILL_DIAG = "sending signal KILL to command"
# Both sentinels must go to stderr: Shell.run matches retry_errors against stderr only.
BUILDX_RETRY_ERRORS += [BUILDX_TIMEOUT_MESSAGE, BUILDX_TIMEOUT_KILL_DIAG]
BUILDX_TIMEOUT_KILL_AFTER = 120
# A per-invocation bound does not bound the job: main() loops over os variants and tags,
# so the invocation count is not fixed. Keep back this much of the cap for recording a
# result, and give each invocation whatever is left.
BUILDX_JOB_RESERVE = 3600
# `timeout 0` runs unbounded (measured: rc 0 after the full command), so a deadline that
# has passed must never reach `timeout` as 0. Clamp to a floor that still expires.
BUILDX_TIMEOUT_FLOOR = 60


def buildx_timeout(elapsed: float = 0.0, job_timeout: int = 0) -> int:
    """Per-invocation bound, shrunk so the whole job stays inside its own cap."""
    if not job_timeout:
        return BUILDX_TIMEOUT
    # One invocation may retry, so it costs up to BUILDX_RETRIES * (bound + kill-after).
    attempts = max(BUILDX_RETRIES, 2)
    budget = (job_timeout - BUILDX_JOB_RESERVE - elapsed) / attempts
    return max(BUILDX_TIMEOUT_FLOOR, min(BUILDX_TIMEOUT, int(budget)))


def with_timeout(cmd: str, seconds: int = BUILDX_TIMEOUT) -> str:
    # A brace group, not `bash -c '...'`: cmd already contains single-quoted arguments,
    # which a surrounding single-quoted string would break. LC_ALL=C pins timeout's
    # diagnostic, which is a translated string.
    return (
        f"{{ LC_ALL=C timeout --verbose --signal=TERM "
        f"--kill-after={BUILDX_TIMEOUT_KILL_AFTER} {seconds} {cmd}; "
        f'rc=$?; if [ "$rc" = 124 ]; then '
        f'echo "{BUILDX_TIMEOUT_MESSAGE} after {seconds}s" >&2; fi; exit $rc; }}'
    )


def is_buildx_timeout(info: str) -> bool:
    return BUILDX_TIMEOUT_MESSAGE in info or BUILDX_TIMEOUT_KILL_DIAG in info


def should_try_next_mirror(info: str) -> bool:
    """After the retries against one mirror are gone: is that mirror a plausible cause?

    A mirror breaks in two shapes, and each is bounded by a different mechanism. It
    refuses the file, which apt reports and `BUILDX_RETRIES` then burns through; or it
    trickles the file, which apt's inactivity timeout never bounds and `with_timeout`
    kills. The second shape is the one that reded the arm64 server image, so an expiry
    has to reach the next mirror too - it is only reached once the build is failing
    anyway, and `buildx_timeout` keeps the extra attempt inside the job's own cap.
    """
    return is_apt_mirror_failure(info) or is_buildx_timeout(info)


def buildx_args(
    urls: Dict[str, str],
    arch: str,
    direct_urls: List[str],
    version: str,
    sha: str,
    action_url: str,
    attest: bool,
) -> List[str]:
    args = [
        f"--platform=linux/{arch}",
        f"--label=build-url={action_url}",
        f"--label=com.clickhouse.build.githash={sha}",
        f"--label=com.clickhouse.build.version={version}",
    ]
    # Attestations survive only in pushed manifests (the local docker exporter
    # drops them), while the SBOM scan of a multi-GB image OOMs smaller runners,
    # so generate them only for pushed images.
    # The scanner is pinned because the floating stable-1 tag can silently move
    # to a version whose scan of a multi-GB image exceeds the runner's memory.
    if attest:
        args.append("--provenance=true")
        args.append("--attest=type=sbom,generator=docker/buildkit-syft-scanner:1.11")
    if direct_urls:
        args.append(f"--build-arg=DIRECT_DOWNLOAD_URLS='{' '.join(direct_urls)}'")
    elif urls:
        url = urls[arch]
        args.append(f"--build-arg=REPOSITORY='{url}'")
        args.append(f"--build-arg=deb_location_url='{url}'")
    return args


def build_and_push_image(
    image: DockerImageData,
    push: bool,
    repo_urls: dict[str, str],
    os: str,
    tag: str,
    version: str,
    direct_urls: Dict[str, List[str]],
    run_url: str,
    sha: str,
    apt_mirror_region: str,
    # Required: a default here silently selects the fixed bound.
    sw: Utils.Stopwatch,
    job_timeout: int,
) -> List[Result]:
    result = []
    if os != "ubuntu":
        tag += f"-{os}"
    init_args = ["docker", "buildx", "build"]
    if push:
        init_args.append("--push")
        init_args.append("--output=type=image,push-by-digest=true")
        init_args.append(f"--tag={image.name}")
    else:
        init_args.append("--output=type=docker")

    # `docker buildx build --load` does not support multiple images currently
    # images must be built separately and merged together with `docker manifest`
    digests = []

    for arch in ARCH:
        arch_tag = f"{tag}-{arch}"
        metadata_path = temp_path / arch_tag
        dockerfile = f"{image.path}/Dockerfile.{os}"
        urls = []
        if direct_urls:
            # distroless and ubuntu-server use an Ubuntu builder with dpkg, so they
            # need .deb packages. alpine and ubuntu-keeper use .tgz packages.
            uses_deb = os == "distroless" or (
                os == "ubuntu" and "clickhouse-server" in image.name
            )
            if uses_deb:
                urls = [
                    url
                    for url in direct_urls[arch]
                    if ".deb" in url and "-dbg" not in url
                ]
            else:
                # For keeper/alpine tgz builds, only pass the keeper tgz.
                # Excluding clickhouse-common-static.tgz avoids a large unnecessary download.
                tgz_urls = [url for url in direct_urls[arch] if ".tgz" in url]
                if "keeper" in image.name:
                    urls = [url for url in tgz_urls if "clickhouse-keeper" in url]
                else:
                    urls = tgz_urls
        cmd_args = list(init_args)
        cmd_args.extend(
            buildx_args(
                repo_urls,
                arch,
                direct_urls=urls,
                version=version,
                action_url=run_url,
                sha=sha,
                attest=push,
            )
        )
        if not push:
            cmd_args.append(f"--tag={image.name}:{arch_tag}")
        cmd_args.extend(
            [
                f"--metadata-file={metadata_path}",
                f"--build-arg=VERSION='{version}'",
                "--progress=plain",
            ]
        )
        # Distroless Dockerfiles have a multi-stage build with both production
        # (no shell) and debug (busybox) targets. Build the production target
        # explicitly to ensure the published image has no shell.
        if os == "distroless":
            cmd_args.append("--target=production")
        mirrors = apt_mirror_variants(apt_mirror_region)
        for attempt, apt_mirror_args in enumerate(mirrors, start=1):
            cmd = " ".join(
                [
                    *cmd_args,
                    *apt_mirror_args,
                    f"--file={dockerfile}",
                    Path(image.path).as_posix(),
                ]
            )
            logging.info(
                "Building image %s:%s for arch %s, apt mirror %s/%s: %s",
                image.name,
                tag,
                arch,
                attempt,
                len(mirrors),
                cmd,
            )
            build_result = Result.from_commands_run(
                name=f"{image.name}:{tag}-{arch}",
                command=with_timeout(cmd, buildx_timeout(sw.duration, job_timeout)),
                retries=BUILDX_RETRIES,
                retry_errors=BUILDX_RETRY_ERRORS,
            )
            if build_result.is_ok() or not should_try_next_mirror(build_result.info):
                if not build_result.is_ok() and not terminal_build_failure(
                    build_result.info
                ):
                    # Fail-closed and silent is what let a reworded buildx error keep
                    # this loop from ever running: say so, so the next log shows it.
                    logging.info(
                        "Image %s:%s for arch %s failed and no terminal build step "
                        "could be identified in its output, so no apt mirror can be "
                        "blamed for it; not trying another mirror",
                        image.name,
                        tag,
                        arch,
                    )
                break
            logging.info(
                "Image %s:%s for arch %s exhausted its retries on a failure the apt "
                "mirror can explain; %s",
                image.name,
                tag,
                arch,
                (
                    "rebuilding against the next apt mirror"
                    if attempt < len(mirrors)
                    else "no apt mirror left to try"
                ),
            )
        result.append(build_result)
        if not result[-1].is_ok():
            return result
        with open(metadata_path, "rb") as m:
            metadata = json.load(m)
            digests.append(metadata["containerimage.digest"])
    if push:
        cmd = (
            "docker buildx imagetools create "
            f"--tag {image.name}:{tag} {' '.join(digests)}"
        )
        logging.info("Pushing merged %s:%s image: %s", image.name, tag, cmd)
        result.append(
            Result.from_commands_run(
                name=f"{image.name}:{tag}",
                command=with_timeout(cmd, buildx_timeout(sw.duration, job_timeout)),
                retries=BUILDX_RETRIES,
                retry_errors=BUILDX_RETRY_ERRORS,
            )
        )
        if not result[-1].is_ok():
            return result
    else:
        logging.info(
            "Merging is available only on push, separate %s images are created",
            f"{image.name}:{tag}-$arch",
        )
    return result


def test_docker_library(test_results) -> None:
    """we test our images vs the official docker library repository to track integrity"""
    arch = "amd64" if Utils.is_amd() else "arm64"
    check_images = [tr.name for tr in test_results if tr.name.endswith(f"-{arch}")]
    if not check_images:
        return
    test_name = "docker library image test"
    try:
        repo = "docker-library/official-images"
        logging.info("Cloning %s repository to run tests for 'clickhouse' image", repo)
        repo_path = temp_path / repo
        config_override = (
            Path(Utils.cwd()) / "ci/jobs/scripts/docker_server/config.sh"
        ).absolute()
        if not Shell.check(
            f"git clone --depth 1 {GITHUB_SERVER_URL}/{repo} {repo_path}",
            verbose=True,
            retries=3,
        ):
            raise RuntimeError(f"Failed to clone {repo}")
        run_sh = (repo_path / "test/run.sh").absolute()
        for image in check_images:
            generated_config = None
            try:
                configs = [repo_path / "test/config.sh", config_override]
                if is_distroless_image(image):
                    generated_config = write_distroless_docker_library_config(
                        image, config_override.parent
                    )
                    configs.append(generated_config)
                config_args = " ".join(
                    f"-c {shlex.quote(config.as_posix())}" for config in configs
                )
                cmd = (
                    f"{shlex.quote(run_sh.as_posix())} "
                    f"{shlex.quote(image)} {config_args}"
                )
                test_results.append(
                    Result.from_commands_run(
                        name=f"{test_name} ({image})", command=cmd
                    )
                )
            finally:
                if generated_config:
                    generated_config.unlink(missing_ok=True)

    except Exception as e:
        logging.error("Failed while testing the docker library image: %s", e)
        test_results.append(
            Result(
                name=test_name,
                status=Result.Status.FAIL,
                info=f"Exception while testing docker library: {traceback.format_exc()}",
            )
        )


def check_server_readme(image_path: str) -> Result:
    name = "Check README"
    script = Path(f"{image_path}/README.sh")
    if not script.is_file():
        return Result(
            name=name,
            status=Result.Status.SKIPPED,
            info="README.sh file is missing in the docker context",
        )
    # Regenerate README
    Shell.check(script.as_posix())
    return Result.from_commands_run(
        name=name, command=f"git diff --exit-code {image_path}/README.md"
    )


def main():
    logging.basicConfig(level=logging.INFO)
    sw = Utils.Stopwatch()
    os.makedirs(temp_path, exist_ok=True)

    args = parse_args()
    info = Info()

    version = None
    if not info.is_local_run:
        version = CHVersion.get_current_version_from_ci_pipeline()
    if not version:
        # Repo-read fallback: the merge-queue workflow runs no version_log hook,
        # so KV storage is empty and this is the only path. The checkout is
        # shallow there, so the tweak cannot be counted from git history -- read
        # non-strict and let it degrade to the placeholder tweak instead of
        # raising, matching the pre-refactor behavior.
        version = CHVersion.get_current_version(no_strict=True)
        if not info.is_local_run:
            print(
                "WARNING: ClickHouse version has not been found in workflow kv storage - read from repo"
            )
            info.add_workflow_warning(
                "ClickHouse version has not been found in workflow kv storage"
            )
    assert version

    if not info.is_local_run:
        assert not args.image_path and not args.image_repo

    if "server image" in info.job_name:
        image_path = args.image_path or "docker/server"
        image_repo = args.image_repo or "clickhouse/clickhouse-server"
        job_timeout = JobConfigs.docker_server.timeout
    elif "keeper image" in info.job_name:
        image_path = args.image_path or "docker/keeper"
        image_repo = args.image_repo or "clickhouse/clickhouse-keeper"
        job_timeout = JobConfigs.docker_keeper.timeout
    else:
        assert False, f"Unexpected job name [{info.job_name}]"

    push = args.push
    apt_mirror_region = args.apt_mirror_region
    del args.image_path
    del args.image_repo
    del args.push
    del args.apt_mirror_region

    if (
        info.is_push_event
        and info.git_branch == "master"
        and info.pr_number == 0
        and not info.is_local_run
    ):
        print("Set push flag for Master CI run")
        push = True

    image = DockerImageData(image_repo, image_path)
    tags = gen_tags(version.string, args.tag_type)
    repo_urls = {}
    direct_urls: Dict[str, List[str]] = {}

    for arch, build_name in zip(ARCH, ("amd_release", "arm_release")):
        if args.allow_build_reuse:
            # read s3 urls from pre-downloaded build reports
            if "clickhouse-server" in image_repo:
                PACKAGES = [
                    "clickhouse-client",
                    "clickhouse-server",
                    "clickhouse-common-static",
                ]
            elif "clickhouse-keeper" in image_repo:
                # Both packages are needed to cover all three keeper image variants:
                #   distroless: installs from .deb via dpkg; clickhouse-common-static
                #               provides the clickhouse multi-tool binary (clickhouse-keeper
                #               is a symlink to it). clickhouse-keeper .deb is not published
                #               separately, so the common-static .deb is the only source.
                #   alpine/ubuntu: installs from .tgz; clickhouse-keeper provides the
                #               standalone keeper binary and its symlinks. The common-static
                #               .tgz is implicitly excluded because the url filter below
                #               keeps only urls containing "clickhouse-keeper" in the name.
                PACKAGES = ["clickhouse-common-static", "clickhouse-keeper"]
            else:
                assert False, "BUG"
            urls = read_build_urls(build_name)
            assert urls, "URLS has not been read from build report"
            direct_urls[arch] = [
                url
                for url in urls
                if any(package in url for package in PACKAGES) and "-dbg" not in url
            ]
        elif args.bucket_prefix:
            assert not args.allow_build_reuse
            repo_urls[arch] = f"{args.bucket_prefix}/{build_name}"
            print(f"Bucket prefix is set: Fetching packages from [{repo_urls}]")
        else:
            assert (
                False
            ), "--sha, --bucket_prefix or --allow-build-reuse (to fetch packages from build report) must be provided"

    if push:
        docker_login()

    logging.info("Following tags will be created: %s", ", ".join(tags))
    test_results = []
    for os_ in args.os:
        for tag in tags:
            test_results.extend(
                build_and_push_image(
                    image,
                    push,
                    repo_urls,
                    os_,
                    tag,
                    version.describe,
                    direct_urls,
                    run_url=info.run_url,
                    sha=info.sha,
                    apt_mirror_region=apt_mirror_region,
                    sw=sw,
                    job_timeout=job_timeout,
                )
            )

    if not push:
        # The image is built locally only when we don't push it
        # See `--output=type=docker`
        test_docker_library(test_results)

    test_results.append(check_server_readme(image.path))

    Result.create_from(results=test_results, stopwatch=sw).complete_job()


if __name__ == "__main__":
    main()
