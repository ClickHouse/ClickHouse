"""
Tests for the apt mirror fallback in ci/jobs/docker_server.py.

`Docker server image` went red on master on 2026-08-21 because
`us-east-1.ec2.ports.ubuntu.com` answered `503` for one package and served the rest
at ~20 kB/s: one mirror stayed broken for longer than `BUILDX_RETRIES` attempts take.
`--apt-mirror-region` exists because the opposite happens too, so the build now tries
the in-region mirror first and rebuilds against Canonical once when the retries are
gone and the mirror can explain the failure.

What is load-bearing is the classifier, not the loop: the second attempt costs a whole
image build, and reading "this mirror withheld a file" out of a log that merely
mentions apt would both waste that build and bury a genuine packaging error behind it.
So the arms below pin that only the step the build stopped on is consulted, that apt's
own `W:`/`E:` severity is respected, and that the loop is actually wired to it.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
# `ci/defs/job_configs.py` does `from praktika import ...` rather than
# `from ci.praktika import ...`, so `ci/` itself must be on the path too.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ci.defs.job_configs import JobConfigs
from ci.jobs import docker_server
from ci.jobs.docker_server import (
    ARCH,
    BUILDX_TIMEOUT_KILL_DIAG,
    BUILDX_TIMEOUT_MESSAGE,
    apt_mirror_variants,
    is_apt_mirror_failure,
    should_try_next_mirror,
)
from ci.praktika.result import Result

REGION = "us-east-1"

# An `apt-get update` that missed an index and carried on. apt marks it `W:`, the step
# is `DONE`, and the image is built from the stale lists - this is not why any later
# build failed, and it is the shape that makes matching the whole log wrong.
RECOVERED_UPDATE = """\
#8 [linux/arm64 stage-0 2/6] RUN apt-get update
#8 2.1 Get:1 http://us-east-1.ec2.ports.ubuntu.com/ubuntu-ports jammy InRelease
#8 3.4 W: Failed to fetch http://us-east-1.ec2.ports.ubuntu.com/ubuntu-ports/dists/jammy/InRelease
#8 3.4 W: Some index files failed to download. They have been ignored, or old ones used instead.
#8 DONE 3.5
"""


def _buildx_failure(step, body):
    """A failed `docker buildx build --progress=plain` tail, in its real shape."""
    run = f'process "/bin/sh -c {step}" did not complete successfully: exit code: 100'
    return (
        f"#12 [linux/arm64 stage-0 4/6] RUN {step}\n"
        + "".join(f"#12 61.2 {line}\n" for line in body)
        + f"#12 ERROR: {run}\n"
        "------\n"
        f" > [linux/arm64 stage-0 4/6] RUN {step}:\n"
        + "".join(f"61.2 {line}\n" for line in body)
        + "------\n"
        f"ERROR: failed to solve: {run}\n"
    )


MIRROR_WITHHELD_FILE = _buildx_failure(
    "apt-get install -y tzdata",
    [
        "E: Failed to fetch http://us-east-1.ec2.ports.ubuntu.com/ubuntu-ports/pool/"
        "main/t/tzdata/tzdata_2026c-0ubuntu0.22.04.1_all.deb  503  Service Unavailable",
        "E: Unable to fetch some archives, maybe run apt-get update or try with "
        "--fix-missing?",
    ],
)

BROKEN_PACKAGE_NAME = _buildx_failure(
    "apt-get install -y clickhosue-server",
    ["E: Unable to locate package clickhosue-server"],
)


def test_only_the_step_the_build_stopped_on_decides():
    """The false positive that would cost a rebuild and hide the real error.

    An `apt-get update` that recovered leaves `W: Failed to fetch` and
    `Some index files failed to download` in the log of a step that succeeded. Reading
    the whole log, every later failure in that image looks mirror-related, including a
    misspelled package name, which no mirror can fix.
    """
    assert is_apt_mirror_failure(RECOVERED_UPDATE + MIRROR_WITHHELD_FILE)
    assert not is_apt_mirror_failure(RECOVERED_UPDATE + BROKEN_PACKAGE_NAME)
    # The premise of the arm: the ignored warning really is in the text being matched.
    assert "Failed to fetch" in RECOVERED_UPDATE


def test_apt_severity_decides_inside_the_failing_step_too():
    """Confining the match to the failing step is not on its own enough.

    `RUN apt-get update && apt-get install -y foo` is one step, so a recovered miss and
    the genuine error that stopped it share a fence. apt's own severity is what still
    separates them: `W:` is a miss it recovered from, `E:` is the file it could not get.
    """
    mixed = _buildx_failure(
        "apt-get update && apt-get install -y clickhosue-server",
        [
            "W: Failed to fetch http://us-east-1.ec2.ports.ubuntu.com/ubuntu-ports/"
            "dists/jammy/InRelease",
            "E: Unable to locate package clickhosue-server",
        ],
    )
    assert not is_apt_mirror_failure(mixed)


def test_a_log_without_the_failure_shape_does_not_reach_the_second_mirror():
    """Fail closed: an unrecognized log is not evidence of a broken mirror.

    `Result.info` is capped at 300 lines and can be re-centred on the first `: error:`
    line, so the fenced block is not guaranteed to survive into it. Guessing from what
    is left would spend another full image build on a failure nothing suggests a mirror
    caused.
    """
    assert not is_apt_mirror_failure("")
    assert not is_apt_mirror_failure(RECOVERED_UPDATE)
    # The fence and the solve error are what identifies the terminal step: neither the
    # in-step copy of the same lines nor a truncated tail may stand in for them.
    unfenced = MIRROR_WITHHELD_FILE.split("------")[0]
    assert "E: Failed to fetch" in unfenced
    assert not is_apt_mirror_failure(unfenced)


def test_a_retried_build_is_judged_by_its_last_attempt():
    """`Result.from_commands_run` appends every attempt's output into one `info`.

    The verdict has to come from the attempt that ended the build; an earlier attempt
    that failed on a mirror does not make a later genuine packaging error mirror-related.
    """
    assert not is_apt_mirror_failure(MIRROR_WITHHELD_FILE + BROKEN_PACKAGE_NAME)
    assert is_apt_mirror_failure(BROKEN_PACKAGE_NAME + MIRROR_WITHHELD_FILE)


def test_a_timed_out_build_reaches_the_next_mirror():
    """The trickling mirror is the shape apt never reports at all.

    `Acquire::http::Timeout` is an inactivity timeout, so a mirror that keeps sending
    bytes slowly produces no apt error - `with_timeout` kills the build instead. That
    is exactly the 2026-08-21 failure, so an expiry has to reach the next mirror, while
    a bare kill (OOM, external) still must not.
    """
    assert should_try_next_mirror(f"{BUILDX_TIMEOUT_MESSAGE} after 2700s")
    assert should_try_next_mirror(f"timeout: {BUILDX_TIMEOUT_KILL_DIAG}")
    assert should_try_next_mirror(MIRROR_WITHHELD_FILE)
    assert not should_try_next_mirror(BROKEN_PACKAGE_NAME)
    assert not should_try_next_mirror("make: *** [all] Error 137")


def test_the_in_region_mirror_is_tried_first_and_canonical_second():
    """Order is the whole point: Canonical is the fallback, not the default."""
    assert apt_mirror_variants("") == [[]], "no region means no second attempt"

    variants = apt_mirror_variants(REGION)
    assert len(variants) == 2
    in_region, canonical = variants
    assert canonical == [], "the Dockerfile default already is Canonical"
    assert in_region == [
        f"--build-arg=apt_archive=http://{REGION}.ec2.archive.ubuntu.com",
        f"--build-arg=apt_ports_archive=http://{REGION}.ec2.ports.ubuntu.com",
    ]


class _Elapsed:
    """Minimal Utils.Stopwatch stand-in: build_and_push_image reads `.duration`."""

    @property
    def duration(self):
        return 0.0


def _commands_for(info, tmp, monkeypatch, apt_mirror_region=REGION):
    """Drive the real build_and_push_image against a canned outcome.

    `info` empty stands for a healthy build; anything else fails every attempt with
    that log, which is what makes the second invocation - or its absence - observable.
    """
    captured = []

    class _Canned:
        def is_ok(self):
            return not info

        info = ""

    _Canned.info = info

    def spy(**kwargs):
        captured.append(kwargs["command"])
        return _Canned()

    # Each per-arch branch reads the buildx metadata file after its build.
    for arch in ARCH:
        (tmp / f"head-{arch}").write_text(
            '{"containerimage.digest": "sha256:0"}', encoding="utf-8"
        )
    monkeypatch.setattr(docker_server, "temp_path", tmp)
    monkeypatch.setattr(Result, "from_commands_run", spy)
    docker_server.build_and_push_image(
        image=docker_server.DockerImageData(
            "clickhouse/clickhouse-server", "docker/server"
        ),
        push=False,
        repo_urls={},
        os="ubuntu",
        tag="head",
        version="26.8.1.1",
        direct_urls={arch: [f"http://x/{arch}/clickhouse-server.deb"] for arch in ARCH},
        run_url="http://run",
        sha="deadbeef",
        apt_mirror_region=apt_mirror_region,
        sw=_Elapsed(),
        job_timeout=JobConfigs.docker_server.timeout,
    )
    return captured


def _uses_in_region(command):
    return f"apt_archive=http://{REGION}.ec2.archive.ubuntu.com" in command


def test_the_loop_is_wired_to_the_classifier(tmp_path, monkeypatch):
    """The classifier arms above are pure; this is what spends a build on them.

    Driving the real build_and_push_image also pins that the retry is a *rebuild*
    against the other mirror rather than the same command issued twice.
    """
    withheld = _commands_for(MIRROR_WITHHELD_FILE, tmp_path, monkeypatch)
    assert len(withheld) == 2, "a withheld file must be rebuilt against Canonical"
    assert _uses_in_region(withheld[0]), "the in-region mirror stays the first choice"
    assert not _uses_in_region(withheld[1]), "the rebuild must point apt elsewhere"

    genuine = _commands_for(BROKEN_PACKAGE_NAME, tmp_path, monkeypatch)
    assert len(genuine) == 1, "a packaging error must still fail on the first attempt"

    healthy = _commands_for("", tmp_path, monkeypatch)
    assert len(healthy) == len(ARCH), "a healthy build issues one command per arch"
    assert all(_uses_in_region(command) for command in healthy)


def test_without_the_option_nothing_changes(tmp_path, monkeypatch):
    """`docker/server` and `docker/keeper` ship with Canonical as their default.

    The fallback lives entirely in the CI job, so a job that did not ask for the
    in-region mirror has nothing to fall back from and must issue one build.
    """
    commands = _commands_for(
        MIRROR_WITHHELD_FILE, tmp_path, monkeypatch, apt_mirror_region=""
    )
    assert len(commands) == 1
    assert not _uses_in_region(commands[0])
