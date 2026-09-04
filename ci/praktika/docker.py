import dataclasses
import os
import time
from typing import Dict, List

from .settings import Settings
from .utils import (
    SHELL_IDLE_TIMEOUT_MESSAGE,
    SHELL_TOTAL_TIMEOUT_MESSAGE,
    Shell,
    Utils,
)

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

# `docker buildx build` resolves base/SBOM-scanner images such as
# `docker/buildkit-syft-scanner` (pulled for the SBOM attestation) from docker.io, which
# intermittently returns transient HTTP errors while resolving and while pushing
# image layers, and the build itself hits `apt-get` package mirrors that occasionally
# refuse connections. Retry the buildx commands only on genuine
# registry/network/mirror *failure* signatures. None of these strings appear in
# normal `--progress=plain` output (unlike progress text such as "resolve image
# config"), so a real Dockerfile/build error (RUN/COPY/package install) still fails
# fast on the first attempt.
_BUILDX_REGISTRY_TRANSIENT_ERRORS = (
    # Docker registry (docker.io / registry-1.docker.io)
    "failed to do request",
    # One containerd error, formatted `unexpected status from <method> request to <url>:
    # <status>`, so the method is an interpolated field and not part of the failure.
    "unexpected status from ",
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
)
_BUILDX_APT_TRANSIENT_ERRORS = (
    # apt-get package mirrors
    "Failed to fetch",
    "Connection failed",
    "Connection timed out",
)
# A tuple, and shared with `ci/jobs/docker_server.py`: neither job can extend the other's
# retry class.
BUILDX_TRANSIENT_ERRORS = (
    _BUILDX_REGISTRY_TRANSIENT_ERRORS + _BUILDX_APT_TRANSIENT_ERRORS
)

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


# Read from buildx's terminal error, which embeds the failing `RUN` recipe verbatim, so a
# recipe naming a mirror error would match on its own text. The apt class is decided inside
# the failing step's fenced block at `E:` severity instead.
_IMAGE_BUILD_REGISTRY_ERRORS = _BUILDX_REGISTRY_TRANSIENT_ERRORS + (
    # The one signature `Docker.build` retried before this ladder existed.
    "Error response from daemon: manifest unknown: manifest unknown",
)
# 8.6x the worst silence in the four healthy leg logs still on S3 (314s, generating the
# SBOM attestation for `binary-builder`), and 7.3x below the 19722s that consumed a whole
# leg on 2026-09-04. Silence does not scale with build length: the slowest healthy single
# image needs 6646s, so no total bound can tell them apart.
_IMAGE_BUILD_IDLE_CAP_S = 2700
# From the FIRST transient failure, and it only gates STARTING another attempt. Spans the
# outages measured on 2026-09-04: minutes for one archive, about half an hour for the
# in-region mirror.
_IMAGE_BUILD_RETRY_DEADLINE_S = 1800
_IMAGE_BUILD_RETRY_INTERVAL_S = 15
# Left for recording a result after the loop, and for the kill grace inside it.
_IMAGE_BUILD_JOB_RESERVE_S = 600
# Under this, an attempt cannot get far enough to be worth starting; returning a shaped
# result beats a doomed attempt the runner would kill mid-way.
_IMAGE_BUILD_MIN_ATTEMPT_S = 600
# Backstop only. The deadlines are the real bound.
_IMAGE_BUILD_ATTEMPTS = 6


def _terminal_error_tail(log_text: str) -> str:
    """The log from buildx's last top-level error line onwards, empty if there is none.

    A per-step copy is prefixed (`#12 ERROR: ...`), so requiring the line to start with
    `ERROR: ` is what keeps an earlier hiccup the build recovered from out of the tail. Our
    own kill sentinels are appended after buildx has finished and start the same way, so
    they are skipped rather than mistaken for the error.
    """
    lines = log_text.splitlines()
    for i in range(len(lines) - 1, -1, -1):
        line = lines[i]
        if line in (SHELL_TOTAL_TIMEOUT_MESSAGE, SHELL_IDLE_TIMEOUT_MESSAGE):
            continue
        if line.startswith(BUILDX_ERROR_PREFIX):
            return "\n".join(lines[i:])
    return ""


def _is_transient(log_text: str) -> bool:
    """Is this failure worth another attempt?

    Fails closed: an unrecognised shape is permanent. The apt class is read only from the
    step the build stopped on and only at `E:` severity, so a `W: Failed to fetch` that an
    earlier successful step recovered from cannot make a later permanent failure look
    retryable.
    """
    if SHELL_IDLE_TIMEOUT_MESSAGE in log_text:
        return True
    if is_apt_mirror_failure(log_text):
        return True
    tail = _terminal_error_tail(log_text)
    return any(error in tail for error in _IMAGE_BUILD_REGISTRY_ERRORS)


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
        cls,
        config: "Docker.Config",
        digests,
        amd_only,
        arm_only,
        disable_push=False,
        job_timeout=0,
        elapsed=0.0,
    ):
        """Build and push one image.

        `job_timeout` and `elapsed` are the leg's own budget and how much of it is gone.
        Given them, every attempt is bounded by what is left, so the retry ladder cannot
        outlast the job; left at 0 nothing is bounded, which is the behaviour of every
        caller that does not know its budget. The deadline is stamped before the metadata
        probe below, so that probe is charged against the budget like everything else.
        """
        from .result import Result

        sw = Utils.Stopwatch()
        # One deadline for the whole ladder. Every bound below is derived from what is left
        # of it, so no attempt can outlive the runner's own kill.
        job_deadline = (
            time.monotonic() + max(0, job_timeout - elapsed - _IMAGE_BUILD_JOB_RESERVE_S)
            if job_timeout
            else None
        )
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

            return cls._build_with_retries(name, command, job_deadline, sw)
        else:
            return Result(
                name=name,
                status=Result.Status.SKIPPED,
                info="image exists",
                start_time=sw.start_time,
                duration=sw.duration,
            )

    @classmethod
    def _build_with_retries(cls, name, command, job_deadline, sw):
        """Run one buildx command, retrying transient failures inside the leg's budget."""
        from .result import Result

        ladder_deadline = None
        result = None
        attempts = 0
        matched = ""

        def exhausted(budget):
            info = (
                f"Out of job budget after {attempts} attempt(s): "
                f"{int(budget)}s left, an attempt needs {_IMAGE_BUILD_MIN_ATTEMPT_S}s."
            )
            # The last attempt's log, not just its `info`: `from_commands_run` attaches the
            # log exactly when it had to truncate `info`, which is when `info` is worst.
            files = None
            if result is not None:
                info += f"\nLast attempt:\n{result.info}"
                files = result.files or None
            return Result.create_from(
                name=name,
                status=Result.Status.FAIL,
                stopwatch=sw,
                info=info,
                files=files,
            )

        for attempt in range(_IMAGE_BUILD_ATTEMPTS):
            if job_deadline is None:
                total = idle = None
            else:
                # Re-read the clock every attempt: the previous one has spent some of it.
                budget = job_deadline - time.monotonic()
                if budget < _IMAGE_BUILD_MIN_ATTEMPT_S:
                    return exhausted(budget)
                total = int(budget)
                idle = min(_IMAGE_BUILD_IDLE_CAP_S, total)

            attempts += 1
            # retries=1: the outer ladder is the only one, so one Result is one buildx run
            # and its duration is the attempt's own.
            result = Result.from_commands_run(
                name=name,
                command=command,
                timeout=total,
                idle_timeout=idle,
                retries=1,
                retry_errors="",
            )
            if result.is_ok():
                break

            # `from_commands_run` attaches the log exactly when it had to truncate `info`,
            # and its truncation window can centre on an early compiler warning and drop
            # the terminal error, so classify the untruncated text.
            log_text = result.info
            if result.files:
                with open(result.files[0], "r", errors="backslashreplace") as f:
                    log_text = f.read()

            if SHELL_TOTAL_TIMEOUT_MESSAGE in log_text:
                # The total bound IS the remaining budget, so this expiry means the budget
                # is gone. Terminal by construction, never classified.
                return exhausted(0)
            if not _is_transient(log_text):
                break

            now = time.monotonic()
            if ladder_deadline is None:
                ladder_deadline = now + _IMAGE_BUILD_RETRY_DEADLINE_S
            matched = (
                SHELL_IDLE_TIMEOUT_MESSAGE
                if SHELL_IDLE_TIMEOUT_MESSAGE in log_text
                else "transient build failure"
            )
            next_start = now + _IMAGE_BUILD_RETRY_INTERVAL_S
            if next_start >= ladder_deadline:
                print(f"Retry deadline of {_IMAGE_BUILD_RETRY_DEADLINE_S}s reached")
                break
            if job_deadline is not None and (
                job_deadline - next_start < _IMAGE_BUILD_MIN_ATTEMPT_S
            ):
                return exhausted(job_deadline - next_start)
            if attempt == _IMAGE_BUILD_ATTEMPTS - 1:
                break
            print(
                f"Transient failure [{matched}] building {name}, "
                f"retrying in {_IMAGE_BUILD_RETRY_INTERVAL_S}s"
            )
            time.sleep(_IMAGE_BUILD_RETRY_INTERVAL_S)

        if attempts > 1 and not result.is_ok():
            # The retries go in `info`, never in `name`: the name is the CIDB row key.
            result.info = (
                f"Failed after {attempts} attempts, last transient signature "
                f"[{matched}].\n{result.info}"
            )
        # Timed over the whole ladder: each attempt stamps its own stopwatch, so returning
        # one unchanged would hide the earlier attempts from the report and from CIDB.
        return result.set_timing(sw)

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
