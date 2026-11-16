"""
Regression tests for the retried job-image pull in `Runner._run`.

`docker run` pulled the image implicitly and praktika ran it once, so a reset
registry transfer killed the job with zero tests run:

    docker: failed to copy: read tcp <runner>:<port>-><registry>:443: read: connection reset by peer

The pull is now its own step before `TeePopen`, guarded by `docker image inspect`,
retried on transport-class errors and bounded per attempt.

Two properties keep the narrow allowlist safe, and both are asserted here.
`retry_errors` is matched against **stderr only** (`Shell.run` collects
`err_output` from the stderr thread alone) while a pull writes progress to stdout:
progress is never retried (arm 4), and the production error itself is one attempt
on stdout (arm 4b) where on stderr it is retried (arm 2). And the pulled command
contains no job command, so nothing the job prints reaches this matcher.

Some arms drive the **real** `Shell.run` retry loop with fake shell commands, so
they exercise the actual matching semantics rather than a model of them; the rest
drive `Runner._run`'s docker branch with stubbed collaborators. Neither docker nor
`jq` is needed.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.praktika import runner as runner_module
from ci.praktika.runner import (
    _IMAGE_PULL_RETRY_ERRORS,
    _IMAGE_PULL_RETRIES,
    _IMAGE_PULL_TIMEOUT_S,
    Runner,
)
from ci.praktika.utils import Shell

# The verbatim production error line, from the failing job's own `info`.
PRODUCTION_RESET = (
    "docker: failed to copy: read tcp 172.31.94.218:51334->54.231.230.177:443: "
    "read: connection reset by peer"
)
# The verbatim line the pull printed in the DNS failure, from that job's own log.
# The resolver answered badly for the registry proxy's name, mid-transfer.
PRODUCTION_DNS_SERVFAIL = (
    "failed to copy: httpReadSeeker: failed open: failed to do request: Get "
    '"http://dockerhub-proxy.dockerhub-proxy-zone:5000/v2/clickhouse/stateless-test'
    "/blobs/sha256:180a6c6776d6b8af402bc3f036f2a9cd65e49d65b4ae2f4c4a4c6f5df000de79"
    '?ns=docker.io": dial tcp: lookup dockerhub-proxy.dockerhub-proxy-zone on '
    "127.0.0.53:53: server misbehaving"
)
# A permanent failure: must never be retried.
PERMANENT_ERROR = (
    "Error response from daemon: pull access denied for foo, "
    "repository does not exist or may require 'docker login'"
)
# Verbatim progress lines from a successful `docker pull`, which go to stdout.
PULL_PROGRESS = [
    "latest: Pulling from library/hello-world",
    "17eec7bbc9d7: Pulling fs layer",
    "17eec7bbc9d7: Download complete",
    "17eec7bbc9d7: Pull complete",
    "Digest: sha256:0b6a027b5cf322f09f6706c754e086a232ec1ddba835c8a15c6cb74d2b114f3f",
    "Status: Downloaded newer image for hello-world:latest",
]

DOCKER_IMAGE = "clickhouse/test-base:0abcdef123456_amd"


# --------------------------------------------------------------------------- #
# Helpers driving the REAL Shell.run retry loop with a fake command.
# --------------------------------------------------------------------------- #
def _counting_command(tmp_path, stdout_lines, stderr_lines, exit_code, succeed_on=None):
    """A bash command that counts its own attempts in a file under `tmp_path`.

    Emits `stdout_lines` on stdout and `stderr_lines` on stderr, exits
    `exit_code` -- except on attempt `succeed_on`, where it exits 0 silently.
    Returns (command, counter_path); read the counter to get the attempt count.
    """
    counter = tmp_path / "attempts"
    counter.write_text("")
    out = "".join(f"echo {line!r}; " for line in stdout_lines)
    err = "".join(f"echo {line!r} >&2; " for line in stderr_lines)
    succeed = f'if [ "$n" = "{succeed_on}" ]; then exit 0; fi; ' if succeed_on else ""
    cmd = (
        f"printf x >> {counter}; n=$(wc -c < {counter}); "
        f"{succeed}{out}{err}exit {exit_code}"
    )
    return cmd, counter


def _attempts(counter):
    return len(counter.read_text())


# --------------------------------------------------------------------------- #
# Helpers driving Runner._run's docker branch.
# --------------------------------------------------------------------------- #
class _FakeJob:
    """The minimum of a praktika Job that `_run`'s docker branch touches."""

    def __init__(self):
        self.name = "Stateless tests (amd_asan_ubsan)"
        self.command = "python3 ./ci/jobs/functional_tests.py"
        self.run_in_docker = "clickhouse/test-base"
        self.timeout = 3600
        self.timeout_shell_cleanup = ""
        self.enable_gh_auth = False
        self.requires = []
        self.provides = []


class _FakeTeePopen:
    """Records the command `_run` would have executed, and reports success."""

    def __init__(self, calls, cmd, **kwargs):
        calls.append(("teepopen", cmd))
        self.timeout_exceeded = False

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def wait(self):
        return 0

    def get_latest_log(self, max_lines=20):
        return ""


def _drive_run(monkeypatch, tmp_path, *, image_present, pull_rc=0, retried_on=""):
    """Run `Runner._run`'s docker branch with collaborators stubbed.

    Returns the ordered list of recorded calls: ("check", cmd), ("run", cmd,
    kwargs), ("teepopen", cmd) and ("env", env). `image_present` decides what
    `docker image inspect` reports; `pull_rc` what the pull returns. When
    `retried_on` is a phrase, the pull's `on_retry` callback is invoked with it
    exactly as the real `Shell.run` loop would on a matched transport error, so
    `_run`'s own callback body runs rather than a model of it.
    """
    calls = []

    def fake_check(command, **kwargs):
        calls.append(("check", command))
        if command.startswith("docker image inspect"):
            return image_present
        # No stale/running container with our name; nothing else is consulted.
        return False

    def fake_run(command, **kwargs):
        calls.append(("run", command, kwargs))
        if command.startswith("timeout") and "docker pull" in command:
            if retried_on and kwargs.get("on_retry"):
                # Same arguments the real loop passes on its first retry.
                kwargs["on_retry"](retried_on, 1, kwargs.get("retries", 1) - 1)
            # Honour the real Shell.run contract: a failed command raises when
            # strict=True and merely returns non-zero otherwise. Without this the
            # fail-open arm would stub out the very mechanism it exists to pin.
            if pull_rc != 0 and kwargs.get("strict"):
                raise RuntimeError(f"command failed, exit code {pull_rc}")
            return pull_rc
        return 0

    # Pin the arch so the resolved tag is deterministic: `_run` appends `_arm`/`_amd` from the
    # host arch, and CI runs this suite on arm while dev boxes are amd.
    monkeypatch.setattr(runner_module.Utils, "is_arm", staticmethod(lambda: False))
    monkeypatch.setattr(runner_module.Utils, "is_amd", staticmethod(lambda: True))
    monkeypatch.setattr(runner_module.Shell, "check", staticmethod(fake_check))
    monkeypatch.setattr(runner_module.Shell, "run", staticmethod(fake_run))
    monkeypatch.setattr(
        runner_module,
        "TeePopen",
        lambda cmd, **kwargs: _FakeTeePopen(calls, cmd, **kwargs),
    )

    class _FakeMetrics:
        def start(self):
            return self

        def stop(self):
            return None

    monkeypatch.setattr(
        runner_module, "HostMetricsCollector", lambda *a, **kw: _FakeMetrics()
    )

    class _FakeResult:
        def is_completed(self):
            return True

        def is_skipped(self):
            return False

        def is_running(self):
            return False

        def dump(self):
            pass

    monkeypatch.setattr(
        runner_module.Result, "from_fs", staticmethod(lambda name: _FakeResult())
    )

    class _FakeEnv:
        """Records workflow warnings the way `_Environment` accumulates them."""

        JOB_NAME = ""
        WORKFLOW_CONFIG = None

        def __init__(self):
            self.warnings = []

        def dump(self):
            pass

        def add_workflow_warning(self, message, source=""):
            self.warnings.append(message)

    env = _FakeEnv()
    calls.append(("env", env))
    monkeypatch.setattr(runner_module._Environment, "get", staticmethod(lambda: env))

    class _FakeRunConfig:
        digest_dockers = {"clickhouse/test-base": "0abcdef123456"}

    monkeypatch.setattr(
        runner_module.RunConfig,
        "from_workflow_data",
        staticmethod(lambda: _FakeRunConfig()),
    )
    monkeypatch.chdir(tmp_path)

    class _FakeWorkflow:
        jobs = []
        artifacts = []

    Runner()._run(_FakeWorkflow(), _FakeJob())
    return calls


def _pull_calls(calls):
    return [c for c in calls if c[0] == "run" and "docker pull" in c[1]]


def _teepopen_index(calls):
    return next(i for i, c in enumerate(calls) if c[0] == "teepopen")


# --------------------------------------------------------------------------- #
# 1. The allowlist and the retry count reach Shell.run.
# --------------------------------------------------------------------------- #
def test_pull_is_issued_with_the_allowlist_and_retries(monkeypatch, tmp_path):
    """Without `retry_errors` the pull retries on ANY failure; without `retries`
    it does not retry at all. Assert on the constant object, not a copy, so the
    test cannot drift from the module."""
    pulls = _pull_calls(_drive_run(monkeypatch, tmp_path, image_present=False))
    assert len(pulls) == 1
    kwargs = pulls[0][2]
    assert kwargs["retry_errors"] is _IMAGE_PULL_RETRY_ERRORS
    assert kwargs["retries"] >= 2


# --------------------------------------------------------------------------- #
# 2. A transport reset IS retried -- through the real Shell.run loop.
# --------------------------------------------------------------------------- #
def test_transport_reset_is_retried(tmp_path):
    cmd, counter = _counting_command(
        tmp_path, [], [PRODUCTION_RESET], exit_code=1, succeed_on=2
    )
    rc = Shell.run(
        cmd, retries=_IMAGE_PULL_RETRIES, retry_errors=_IMAGE_PULL_RETRY_ERRORS
    )
    assert rc == 0
    assert _attempts(counter) == 2


# --------------------------------------------------------------------------- #
# 3. A permanent error is NOT retried (the anti-over-broad-allowlist arm).
# --------------------------------------------------------------------------- #
def test_permanent_error_is_not_retried(tmp_path):
    """A missing or misnamed image must fail fast rather than burn every attempt.
    This is the regression guard for the over-broad-allowlist defect that forced
    BUILDX_RETRY_ERRORS to be narrowed."""
    cmd, counter = _counting_command(tmp_path, [], [PERMANENT_ERROR], exit_code=1)
    rc = Shell.run(
        cmd, retries=_IMAGE_PULL_RETRIES, retry_errors=_IMAGE_PULL_RETRY_ERRORS
    )
    assert rc != 0
    assert _attempts(counter) == 1


# --------------------------------------------------------------------------- #
# 3b. The other two permanent errors the allowlist comment names.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "phrase",
    [
        # Verbatim from ci/praktika/docker.py's own build retry list.
        "Error response from daemon: manifest unknown: manifest unknown",
        # The phrase prefetch-integration-test-images greps for.
        "no matching manifest for linux/arm64/v8 in the manifest list entries",
    ],
)
def test_named_permanent_errors_are_not_retried(tmp_path, phrase):
    """`_IMAGE_PULL_RETRY_ERRORS`' comment and the PR body both promise these fail
    on the first attempt, but only `pull access denied` was tested (arm 3)."""
    cmd, counter = _counting_command(tmp_path, [], [phrase], exit_code=1)
    rc = Shell.run(
        cmd, retries=_IMAGE_PULL_RETRIES, retry_errors=_IMAGE_PULL_RETRY_ERRORS
    )
    assert rc != 0
    assert _attempts(counter) == 1


# --------------------------------------------------------------------------- #
# 4. Pull PROGRESS is not an error: it can never trigger a retry.
# --------------------------------------------------------------------------- #
def test_pull_progress_on_stdout_does_not_trigger_a_retry(tmp_path):
    """A successful pull writes progress lines like `Pull complete`, and none of
    them is an allowlisted phrase, so a non-zero exit whose only output is
    progress must be a single attempt."""
    cmd, counter = _counting_command(tmp_path, PULL_PROGRESS, [], exit_code=1)
    rc = Shell.run(
        cmd, retries=_IMAGE_PULL_RETRIES, retry_errors=_IMAGE_PULL_RETRY_ERRORS
    )
    assert rc != 0
    assert _attempts(counter) == 1


# --------------------------------------------------------------------------- #
# 4b. The stderr-only split itself: the SAME phrase on stdout must NOT retry.
# --------------------------------------------------------------------------- #
def test_allowlisted_phrase_on_stdout_does_not_trigger_a_retry(tmp_path):
    """Arm 4 carries no allowlisted phrase, so it reads one attempt under a
    stderr-only matcher AND under one that also matched stdout: it cannot pin the
    split. This arm can. It sends the verbatim production error to stdout with an
    empty stderr and must still be a single attempt, while arm 2 sends that same
    line to stderr and is retried."""
    cmd, counter = _counting_command(tmp_path, [PRODUCTION_RESET], [], exit_code=1)
    rc = Shell.run(
        cmd, retries=_IMAGE_PULL_RETRIES, retry_errors=_IMAGE_PULL_RETRY_ERRORS
    )
    assert rc != 0
    assert _attempts(counter) == 1


# --------------------------------------------------------------------------- #
# 4c. Every remaining transport entry is load-bearing.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "phrase",
    [
        "connection refused",
        "TLS handshake timeout",
        "i/o timeout",
        "unexpected EOF",
        "server misbehaving",
    ],
)
def test_each_transport_phrase_is_retried(tmp_path, phrase):
    """Arms 2 and 4b already pin `connection reset by peer` and arm 8 pins
    `sending signal TERM to command`; without these, dropping any of the other
    five entries from the production list would leave the suite green."""
    cmd, counter = _counting_command(tmp_path, [], [f"docker: {phrase}"], exit_code=1)
    rc = Shell.run(
        cmd, retries=_IMAGE_PULL_RETRIES, retry_errors=_IMAGE_PULL_RETRY_ERRORS
    )
    assert rc != 0
    assert _attempts(counter) == _IMAGE_PULL_RETRIES


# --------------------------------------------------------------------------- #
# 4d. The production DNS failure, verbatim, against both lists.
# --------------------------------------------------------------------------- #
def test_the_production_dns_failure_is_retried(tmp_path):
    """Arm 4c feeds a bare phrase, so it cannot tell whether the entry matches the
    line docker actually prints, where the phrase is the tail of a nested resolver
    error inside a URL. Feed that line verbatim instead.

    The list with the entry dropped is the control: without it a pass here would
    also be produced by some other entry matching, which is what the pre-fix job
    disproved by stopping after one attempt."""
    retried_dir = tmp_path / "retried"
    retried_dir.mkdir()
    cmd, counter = _counting_command(
        retried_dir, [], [PRODUCTION_DNS_SERVFAIL], exit_code=1
    )
    seen = []
    rc = Shell.run(
        cmd,
        retries=_IMAGE_PULL_RETRIES,
        retry_errors=_IMAGE_PULL_RETRY_ERRORS,
        on_retry=lambda matched, attempt, attempts: seen.append(matched),
    )
    assert rc != 0
    assert _attempts(counter) == _IMAGE_PULL_RETRIES
    # The retry is reported, so an operator sees why the job took a second attempt.
    assert seen == ["server misbehaving"] * (_IMAGE_PULL_RETRIES - 1)

    without_dns = [e for e in _IMAGE_PULL_RETRY_ERRORS if e != "server misbehaving"]
    assert len(without_dns) == len(_IMAGE_PULL_RETRY_ERRORS) - 1
    control_dir = tmp_path / "control"
    control_dir.mkdir()
    cmd2, counter2 = _counting_command(
        control_dir, [], [PRODUCTION_DNS_SERVFAIL], exit_code=1
    )
    assert Shell.run(cmd2, retries=_IMAGE_PULL_RETRIES, retry_errors=without_dns) != 0
    assert _attempts(counter2) == 1


def test_the_permanent_resolver_failure_is_not_retried(tmp_path):
    """`no such host` is the resolver's NXDOMAIN verdict: the name does not exist, so
    a typo'd image host must fail on attempt 1 rather than burn every attempt. Pins
    that the entry above was not widened to `dial tcp` or `lookup `, either of which
    would readmit this line."""
    nxdomain = (
        "failed to do request: Get "
        '"http://dockerhub-proxy.dockerhub-proxy-zone:5000/v2/foo/bar/manifests/x": '
        "dial tcp: lookup dockerhub-proxy.dockerhub-proxy-zone: no such host"
    )
    cmd, counter = _counting_command(tmp_path, [], [nxdomain], exit_code=1)
    rc = Shell.run(
        cmd, retries=_IMAGE_PULL_RETRIES, retry_errors=_IMAGE_PULL_RETRY_ERRORS
    )
    assert rc != 0
    assert _attempts(counter) == 1


# --------------------------------------------------------------------------- #
# 5. Fail-open: a failed pull must not stop the job.
# --------------------------------------------------------------------------- #
def test_failed_pull_still_runs_the_container(monkeypatch, tmp_path):
    """Images built locally by this workflow cannot be pulled at all (`docker pull`
    fails with `pull access denied` while `docker run` succeeds), so the pull must
    never be fatal. Without this arm a later `strict=True` would break every
    locally-built image with all other arms green."""
    calls = _drive_run(monkeypatch, tmp_path, image_present=False, pull_rc=1)
    assert len(_pull_calls(calls)) == 1
    teepopen = [c for c in calls if c[0] == "teepopen"]
    assert len(teepopen) == 1
    assert teepopen[0][1].startswith("docker run ")


# --------------------------------------------------------------------------- #
# 6. Ordering: the pull precedes `docker run`, on the fully resolved name:tag.
# --------------------------------------------------------------------------- #
def test_pull_precedes_docker_run_and_uses_the_resolved_tag(monkeypatch, tmp_path):
    """Also pins the isolation property the narrow allowlist depends on: the pull
    command ends at the image, so it carries no job command and nothing a job
    prints can reach the matcher or make a retry re-run the job."""
    calls = _drive_run(monkeypatch, tmp_path, image_present=False)
    pull_index = next(
        i for i, c in enumerate(calls) if c[0] == "run" and "docker pull" in c[1]
    )
    assert pull_index < _teepopen_index(calls)
    assert f"docker pull {DOCKER_IMAGE}" in calls[pull_index][1]

    pull_cmd = calls[pull_index][1]
    job = _FakeJob()
    assert job.command not in pull_cmd
    # Forbids ANY suffix after the image, not just this fixture's command.
    assert pull_cmd.endswith(DOCKER_IMAGE)


# --------------------------------------------------------------------------- #
# 7. The inspect guard: pull only when the image is absent.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "image_present,expected_pulls", [(True, 0), (False, 1)], ids=["present", "absent"]
)
def test_inspect_guard_pulls_only_when_absent(
    monkeypatch, tmp_path, image_present, expected_pulls
):
    """A bare `docker run` uses an existing local image without contacting the
    registry, so an unconditional pull would re-resolve a mutable tag. The absent
    arm is what stops the guard from disabling the fix entirely."""
    calls = _drive_run(monkeypatch, tmp_path, image_present=image_present)
    assert len(_pull_calls(calls)) == expected_pulls
    assert [c for c in calls if c[0] == "teepopen"]
    assert any(
        c[0] == "check" and c[1] == f"docker image inspect {DOCKER_IMAGE}"
        for c in calls
    )


# --------------------------------------------------------------------------- #
# 8. A stalled attempt is bounded AND still retryable.
# --------------------------------------------------------------------------- #
def test_stalled_attempt_is_bounded(monkeypatch, tmp_path):
    pulls = _pull_calls(_drive_run(monkeypatch, tmp_path, image_present=False))
    assert f"timeout --verbose {_IMAGE_PULL_TIMEOUT_S} " in pulls[0][1]


def test_stalled_attempt_is_retryable_only_with_verbose(tmp_path):
    """`Shell.run(timeout=...)` bounds an attempt but its SIGTERMed child writes
    nothing to stderr, so `retry_errors` matches nothing and the loop stops after one
    attempt. Putting the bound in the command with `--verbose` emits a matchable line
    instead. Both spellings return 124, so only the attempt COUNT discriminates them:
    that contrast is what proves the allowlist entry is load-bearing, and a naive
    "is it bounded?" assertion would stay green without it."""
    verbose_counter = tmp_path / "verbose"
    verbose_counter.write_text("")
    verbose_rc = Shell.run(
        f"printf x >> {verbose_counter}; timeout --verbose 1 sleep 30",
        retries=_IMAGE_PULL_RETRIES,
        retry_errors=_IMAGE_PULL_RETRY_ERRORS,
    )

    plain_counter = tmp_path / "plain"
    plain_counter.write_text("")
    plain_rc = Shell.run(
        f"printf x >> {plain_counter}; timeout 1 sleep 30",
        retries=_IMAGE_PULL_RETRIES,
        retry_errors=_IMAGE_PULL_RETRY_ERRORS,
    )

    assert verbose_rc == 124 and plain_rc == 124
    assert _attempts(verbose_counter) == _IMAGE_PULL_RETRIES
    assert _attempts(plain_counter) == 1


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))


# --------------------------------------------------------------------------- #
# 9. A retried pull is reported as a workflow warning.
# --------------------------------------------------------------------------- #
def test_retry_emits_a_workflow_warning(monkeypatch, tmp_path):
    """The retry must not be silent: a pull that needed a second attempt shows up
    on the job and workflow report pages."""
    calls = _drive_run(
        monkeypatch,
        tmp_path,
        image_present=False,
        retried_on="connection reset by peer",
    )
    env = next(c[1] for c in calls if c[0] == "env")
    assert len(env.warnings) == 1
    assert "connection reset by peer" in env.warnings[0]
    assert DOCKER_IMAGE in env.warnings[0]


def test_a_clean_pull_emits_no_warning(monkeypatch, tmp_path):
    """The negative control: without it the arm above would also pass against a
    hook that warns unconditionally."""
    calls = _drive_run(monkeypatch, tmp_path, image_present=False)
    env = next(c[1] for c in calls if c[0] == "env")
    assert env.warnings == []


def test_a_skipped_pull_emits_no_warning(monkeypatch, tmp_path):
    """An image already present is never pulled, so there is nothing to report."""
    calls = _drive_run(
        monkeypatch,
        tmp_path,
        image_present=True,
        retried_on="connection reset by peer",
    )
    env = next(c[1] for c in calls if c[0] == "env")
    assert env.warnings == []


def test_on_retry_fires_once_per_retry_and_only_on_matched_errors(tmp_path):
    """Drive the REAL `Shell.run` loop: the callback fires for each retry actually
    issued, receives the matched phrase, and is never called for a permanent error
    (which is the same code path that decides not to retry)."""
    seen = []

    cmd, counter = _counting_command(
        tmp_path, [], [PRODUCTION_RESET], exit_code=1, succeed_on=3
    )
    rc = Shell.run(
        cmd,
        retries=_IMAGE_PULL_RETRIES,
        retry_errors=_IMAGE_PULL_RETRY_ERRORS,
        on_retry=lambda matched, attempt, attempts: seen.append(matched),
    )
    assert rc == 0
    assert _attempts(counter) == 3
    # Two retries were issued to reach the third attempt.
    assert seen == ["connection reset by peer"] * 2

    # And when every attempt fails, the last one matches the allowlist too but nothing
    # is retried after it, so the count must stay at the retries actually issued --
    # otherwise the warning would claim a retry that never happened.
    terminal = []
    terminal_dir = tmp_path / "terminal"
    terminal_dir.mkdir()
    cmd3, counter3 = _counting_command(
        terminal_dir, [], [PRODUCTION_RESET], exit_code=1
    )
    Shell.run(
        cmd3,
        retries=_IMAGE_PULL_RETRIES,
        retry_errors=_IMAGE_PULL_RETRY_ERRORS,
        on_retry=lambda matched, attempt, attempts: terminal.append(attempt),
        verbose=False,
    )
    assert _attempts(counter3) == _IMAGE_PULL_RETRIES
    assert terminal == list(range(1, _IMAGE_PULL_RETRIES))

    permanent = []
    permanent_dir = tmp_path / "permanent"
    permanent_dir.mkdir()
    cmd2, counter2 = _counting_command(
        permanent_dir, [], [PERMANENT_ERROR], exit_code=1
    )
    Shell.run(
        cmd2,
        retries=_IMAGE_PULL_RETRIES,
        retry_errors=_IMAGE_PULL_RETRY_ERRORS,
        on_retry=lambda matched, attempt, attempts: permanent.append(matched),
    )
    assert _attempts(counter2) == 1
    assert permanent == []


def test_a_failing_warning_hook_does_not_change_the_outcome(tmp_path):
    """Reporting is best-effort. A rescued pull is the easy half; the discriminating
    half is a pull that fails on every attempt, because `Shell.run`'s own
    `except Exception` around the loop body reports an exception-terminated attempt as
    exit code 1. So a hook that raises there would REPLACE the real exit code -- 124
    for a stalled pull -- with 1, hiding the actual failure from the caller and from
    the job report. Assert the code is preserved, not merely that the pull survived."""

    def boom(matched, attempt, attempts):
        raise RuntimeError("reporting is broken")

    stalled = f"printf x >> {tmp_path / 'broken'}; timeout --verbose 1 sleep 30"
    rc_broken = Shell.run(
        stalled,
        retries=_IMAGE_PULL_RETRIES,
        retry_errors=_IMAGE_PULL_RETRY_ERRORS,
        on_retry=boom,
        verbose=False,
    )
    # Control: the same command with no hook at all, so the expected code is measured
    # rather than assumed.
    rc_plain = Shell.run(
        f"printf x >> {tmp_path / 'plain2'}; timeout --verbose 1 sleep 30",
        retries=_IMAGE_PULL_RETRIES,
        retry_errors=_IMAGE_PULL_RETRY_ERRORS,
        verbose=False,
    )
    assert rc_plain == 124
    assert rc_broken == rc_plain

    # And a hook that raises still lets a retry rescue the pull.
    cmd, counter = _counting_command(
        tmp_path, [], [PRODUCTION_RESET], exit_code=1, succeed_on=2
    )
    assert (
        Shell.run(
            cmd,
            retries=_IMAGE_PULL_RETRIES,
            retry_errors=_IMAGE_PULL_RETRY_ERRORS,
            on_retry=boom,
        )
        == 0
    )
    assert _attempts(counter) == 2
