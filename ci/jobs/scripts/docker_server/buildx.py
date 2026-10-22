import logging
from typing import Callable, List

from ci.praktika.result import Result

# Registry and network failure signatures only; an apt mirror failure moves to the next mirror, not a rebuild.
BUILDX_RETRIES = 5
BUILDX_RETRY_ERRORS = [
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
]

# A mirror did not hand over the file; a genuine packaging error produces none of these.
APT_MIRROR_ERRORS = [
    # `apt-get install` could not download a .deb, and the summary line after it
    "Failed to fetch",
    "Unable to fetch some archives",
    # `apt-get update` could not refresh the package lists
    "Some index files failed to download",
]

# `apt-get update` does not fail when a mirror withholds a suite's `InRelease`: it
# warns (`W: Failed to fetch ... 503`, then `W: Some index files failed to download`),
# carries on with whatever suites it did get and exits 0. So the `RUN` chain reaches
# `apt-get install`, which dies resolving the packages against a partial index, and it
# is that resolution error - naming no mirror, no URL and no `Failed to fetch` - that
# buildx fences. `APT_MIRROR_ERRORS` matches none of it, which is why not one of the
# 26 arm64 image builds that reded across the fleet inside the 00:00 UTC hour of
# 2026-09-05 rebuilt against Canonical.
#
# Which of the three shapes apt reports depends only on what the missing suites were
# providing, so all three have to be here: `wget` and `tzdata` came out as `Unable to
# locate package`, `busybox`, `ca-certificates` and `locales` as `has no installation
# candidate` because the pristine base image's `/var/lib/dpkg/status` still names them,
# and a `wget` whose `libpsl5` had gone as `held broken packages`.
APT_RESOLUTION_ERRORS = [
    "Unable to locate package",
    "has no installation candidate",
    "Unable to correct problems, you have held broken packages",
]
# On its own a resolution error is also what a misspelled package name in one of these
# Dockerfiles produces, and that must keep failing on the first attempt. What separates
# the two is the `apt-get update` in the very same step: against a healthy mirror it
# refreshes every suite silently, so this warning is the mirror's own signature and not
# an inference from the base image's contents.
APT_INDEX_WARNING = "Some index files failed to download"


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


def terminal_step_prefix(info: str) -> str:
    """`--progress=plain` line prefix (`#8 `) of the step the build stopped on.

    buildx tags every output line with the step it came from and announces the failure
    as `#8 ERROR: process ... did not complete successfully`, which is the only place
    the fenced block's step number is stated. Empty when that line is absent, so
    callers fail closed.
    """
    prefix = ""
    for line in info.splitlines():
        if line.startswith("#") and line.partition(" ")[2].startswith(
            BUILDX_ERROR_PREFIX
        ):
            prefix = line.partition(" ")[0] + " "
    return prefix


def is_apt_index_failure(info: str) -> bool:
    """Did the step that stopped the build resolve packages against a partial index?

    Both halves have to come from that one step: apt's `E:` resolution error inside the
    terminal fenced block, and the warning that the step's own index refresh came back
    incomplete, which apt prints far enough above the fence that only the step's
    `#N ` prefix ties the two together.
    """
    if not any(
        not line.startswith(BUILDX_STEP_HEADER_PREFIX)
        and APT_ERROR_SEVERITY in line
        and any(error in line for error in APT_RESOLUTION_ERRORS)
        for line in terminal_build_failure(info).splitlines()
    ):
        return False
    prefix = terminal_step_prefix(info)
    return bool(prefix) and any(
        line.startswith(prefix) and APT_INDEX_WARNING in line
        for line in info.splitlines()
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
# Neither sentinel is in `BUILDX_RETRY_ERRORS`: an expiry is retried only if its output
# matches another entry, else a build is rebuilt against the next apt mirror, if any.
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
    """Is the apt mirror a plausible cause: it refused the file, or trickled it until `with_timeout` killed the build?"""
    return (
        is_apt_mirror_failure(info)
        or is_apt_index_failure(info)
        or is_buildx_timeout(info)
    )


def build_rotating_apt_mirrors(
    name: str, apt_mirror_region: str, make_command: Callable[[List[str]], str]
) -> Result:
    """Run a `docker buildx build`, rebuilding against the next apt mirror when the mirror explains the failure.

    `make_command` gets the mirror's `--build-arg`s and is called once per mirror, so a
    caller can recompute a timeout that depends on the time already spent.
    """
    mirrors = apt_mirror_variants(apt_mirror_region)
    for attempt, apt_mirror_args in enumerate(mirrors, start=1):
        command = make_command(apt_mirror_args)
        logging.info(
            "Building %s, apt mirror %s/%s: %s", name, attempt, len(mirrors), command
        )
        result = Result.from_commands_run(
            name=name,
            command=command,
            retries=BUILDX_RETRIES,
            retry_errors=BUILDX_RETRY_ERRORS,
        )
        if result.is_ok() or not should_try_next_mirror(result.info):
            if not result.is_ok() and not terminal_build_failure(result.info):
                # Fail-closed and silent is what let a reworded buildx error keep
                # this loop from ever running: say so, so the next log shows it.
                logging.info(
                    "%s failed and no terminal build step could be identified in its "
                    "output, so no apt mirror can be blamed for it; not trying another "
                    "mirror",
                    name,
                )
            break
        logging.info(
            "%s exhausted its retries on a failure the apt mirror can explain; %s",
            name,
            (
                "rebuilding against the next apt mirror"
                if attempt < len(mirrors)
                else "no apt mirror left to try"
            ),
        )
    return result
