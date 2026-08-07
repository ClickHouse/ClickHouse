"""
Tests for the `docker buildx` wall-clock bound in ci/jobs/docker_server.py.

`Docker server image` / `Docker keeper image` jobs wedged for ~5h and died on the
18000s job cap with no result recorded, four times on 2026-08-05. The build hung
inside `apt-get update` against a mirror that kept trickling bytes: apt's
`Acquire::http::Timeout` is an inactivity timeout, so it never fired, and the
`retries`/`retry_errors` already passed to `Result.from_commands_run` were
unreachable because the command never returned.

`with_timeout` bounds each invocation with GNU `timeout` and echoes a sentinel so
the existing retry machinery can act on an expiry. Three properties are load-bearing
and are pinned here: the sentinels are members of BUILDX_RETRY_ERRORS (the `+=` is
the only wiring, so a future literal rewrite would silently un-arm the retry), the
brace-group form does not mangle the already-quoted buildx arguments, and a bare 137
(OOM, external kill) is not mistaken for an expiry.
"""

import ast
import os
import re
import subprocess
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
# `ci/defs/job_configs.py` does `from praktika import ...` rather than
# `from ci.praktika import ...`, so `ci/` itself must be on the path too.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ci.defs.job_configs import JobConfigs
from ci.jobs import docker_server
from ci.jobs.docker_server import (
    ARCH,
    BUILDX_JOB_RESERVE,
    BUILDX_RETRIES,
    BUILDX_RETRY_ERRORS,
    BUILDX_TIMEOUT,
    BUILDX_TIMEOUT_FLOOR,
    BUILDX_TIMEOUT_KILL_AFTER,
    BUILDX_TIMEOUT_KILL_DIAG,
    BUILDX_TIMEOUT_MESSAGE,
    buildx_timeout,
    gen_tags,
    with_timeout,
)
from ci.praktika.result import Result
from ci.praktika.utils import Shell

# `printf` stands in for `docker buildx build` so the argv is observable. The
# quoting shape is buildx_args' own: a single-quoted value containing a space.
ARGV_CMD = (
    "printf '[%s]\\n' --provenance=true --platform=linux/amd64 "
    "--build-arg=DIRECT_DOWNLOAD_URLS='http://a/b.deb http://c/d.deb' "
    "--progress=plain --file=docker/server/Dockerfile.ubuntu docker/server"
)
SPACED_ARG = "[--build-arg=DIRECT_DOWNLOAD_URLS=http://a/b.deb http://c/d.deb]"


def _run(cmd):
    return subprocess.run(
        cmd, shell=True, executable="/bin/bash", capture_output=True, text=True
    )


def _bash_c_form(cmd, seconds):
    """The rejected form, kept only as a discriminating negative control."""
    return (
        f"bash -c 'LC_ALL=C timeout --verbose --signal=TERM --kill-after=120 "
        f"{seconds} {cmd}; rc=$?; exit $rc'"
    )


def test_expiry_sentinels_are_wired_into_the_retry_errors():
    """Pins the `+=`: without both tokens an expiry is never retried."""
    assert BUILDX_TIMEOUT_MESSAGE in BUILDX_RETRY_ERRORS
    assert BUILDX_TIMEOUT_KILL_DIAG in BUILDX_RETRY_ERRORS


def test_kill_escalation_diagnostic_is_enabled():
    """`timeout` only prints the KILL diagnostic under --verbose.

    Asserted on the command string, not by running it: observing the diagnostic
    requires waiting out --kill-after, which is far longer than the pytest budget.
    """
    wrapped = with_timeout("true", 30)
    assert "--verbose" in wrapped, "without --verbose the KILL diagnostic is never printed"
    assert "--kill-after=" in wrapped, "a TERM-ignoring build would never be reaped"
    # A zeroed delay is accepted by `timeout` and silently unbounds the very build
    # this module bounds: measured, --kill-after=0 lets a TERM-ignoring child run to
    # natural completion and returns 124, so the 124 sentinel then *arms* the retry
    # and the unbounded run repeats BUILDX_RETRIES times.
    assert BUILDX_TIMEOUT_KILL_AFTER > 0, (
        "a zeroed kill delay never escalates, so a TERM-ignoring build runs "
        "unbounded and is then retried on the 124 sentinel"
    )


def test_brace_group_preserves_quoted_arguments():
    """The oracle is the argv against an unwrapped baseline, not the exit code."""
    baseline = _run(ARGV_CMD).stdout
    assert SPACED_ARG in baseline, "baseline did not produce the space-bearing argument"

    wrapped = _run(with_timeout(ARGV_CMD, 30))
    assert wrapped.stdout == baseline, "the brace group perturbed the argv"

    # Negative control: the rejected form returns rc=0 while truncating that value
    # at the space, which is exactly why the exit code is not the oracle.
    bad = _run(_bash_c_form(ARGV_CMD, 30))
    assert bad.returncode == 0, "the rejected form is supposed to look fine by rc"
    assert bad.stdout != baseline, (
        "negative control did not mangle the argv, so byte-equality proves nothing"
    )
    assert SPACED_ARG not in bad.stdout


def test_expiry_returns_124_and_writes_the_sentinel_to_stderr():
    """Shell.run matches retry_errors against stderr only (utils.py:411-413)."""
    got = _run(with_timeout("sleep 30", 1))
    assert got.returncode == 124, f"expected 124, got {got.returncode}"
    assert BUILDX_TIMEOUT_MESSAGE in got.stderr
    assert BUILDX_TIMEOUT_MESSAGE not in got.stdout, "sentinel must not go to stdout"


def test_fast_command_does_not_emit_the_sentinel():
    got = _run(with_timeout("true", 30))
    assert got.returncode == 0
    assert BUILDX_TIMEOUT_MESSAGE not in got.stderr + got.stdout


def test_real_build_error_still_fails_fast():
    """A genuine build failure must not be turned into a retryable expiry."""
    got = _run(with_timeout("bash -c 'exit 7'", 30))
    assert got.returncode == 7
    assert BUILDX_TIMEOUT_MESSAGE not in got.stderr
    assert BUILDX_TIMEOUT_KILL_DIAG not in got.stderr


def test_bare_137_is_not_read_as_an_expiry():
    """An OOM/external SIGKILL yields 137 with neither sentinel.

    This is why the KILL diagnostic, not the exit code, identifies an escalated
    expiry: 137 alone is ambiguous and must not be retried.
    """
    got = _run(with_timeout("bash -c 'kill -9 $$'", 30))
    assert got.returncode == 137, f"expected 137, got {got.returncode}"
    assert BUILDX_TIMEOUT_MESSAGE not in got.stderr
    assert BUILDX_TIMEOUT_KILL_DIAG not in got.stderr


def test_both_buildx_invocations_are_wrapped():
    """Removing the wrapper from either call site must be caught."""
    src = open(docker_server.__file__, encoding="utf-8").read()
    assert src.count("command=with_timeout(") == 2, (
        "expected both buildx invocations to be wrapped in with_timeout"
    )


def test_both_call_sites_pass_the_retries_and_the_retry_errors():
    """The bound is useless unless the expiry it produces is retried.

    Both kwargs are named: `retries=` alone leaves an expiry unmatched, and
    `retry_errors=` alone is silently coerced to two attempts but stops matching
    the sentinels if the list is emptied.
    """
    src = open(docker_server.__file__, encoding="utf-8").read()
    assert src.count("retries=BUILDX_RETRIES") == 2
    assert src.count("retry_errors=BUILDX_RETRY_ERRORS") == 2


# --- the aggregate budget ---------------------------------------------------


def _os_default():
    """The `--os` default, read from the parser source rather than retyped."""
    src = open(docker_server.__file__, encoding="utf-8").read()
    match = re.search(r'"--os",\s*default=(\[[^\]]*\])', src)
    assert match, "could not locate the --os default in the parser"
    return ast.literal_eval(match.group(1))


def _backoff_seconds(attempts):
    """Shell.run's own schedule: delay = min(2 * delay, 60) before each retry."""
    backoff, delay = 0, 1
    for _ in range(attempts - 1):
        delay = min(2 * delay, 60)
        backoff += delay
    return backoff


def _group_exit_seconds(entry, per_group, cap):
    """Every elapsed reachable after one build_and_push_image call.

    Each invocation has three possible fates, and the third is why this returns a
    set rather than a value: Shell.run can expire on some attempts and then have a
    later one succeed (utils.py:405-406), which leaves the result ok, so
    build_and_push_image does not return early and the rest of the group still
    runs. That state costs nearly as much as a wedge yet does not end the group.
    """
    attempts = max(BUILDX_RETRIES, 2)  # utils.py coerces to a floor of two
    live, exits = {entry}, set()
    for _ in range(per_group):
        after = set()
        for elapsed in live:
            bound = buildx_timeout(elapsed, cap)
            after.add(elapsed + bound)  # succeeds first try
            exits.add(  # wedges: every attempt expires, and the group ends
                elapsed
                + attempts * (bound + BUILDX_TIMEOUT_KILL_AFTER)
                + _backoff_seconds(attempts)
            )
            for expiries in range(1, attempts):  # succeeds on a later attempt
                after.add(
                    elapsed
                    + expiries * (bound + BUILDX_TIMEOUT_KILL_AFTER)
                    + _backoff_seconds(expiries + 1)
                    + bound
                )
        live = after
    return exits | live


def _worst_case_job_seconds(n_variants, n_tags, push, cap):
    """Walk main()'s own loop structure, allowing one wedge per group.

    main() loops os variants and tags with no break and no is_ok() gate between
    groups, so every build_and_push_image call can wedge in one run; only the
    invocations *after* a failure inside one group are skipped. Pricing a single
    wedge per job under-reports, and a bound that is safe once is not safe nine
    times.

    Exact by construction rather than by enumeration: group cost is a pure
    function of (entry elapsed, per-invocation fates), so two traces reaching the
    same elapsed have identical futures and may be merged. Keeping the set of
    reachable elapsed values after each group therefore loses nothing and stays
    cheap where enumerating every trace does not.
    """
    per_group = len(ARCH) + (1 if push else 0)
    reachable = {0.0}
    for _ in range(n_variants * n_tags):
        reachable = {
            exit_at
            for entry in reachable
            for exit_at in _group_exit_seconds(entry, per_group, cap)
        }
    return max(reachable)


def test_the_whole_job_worst_case_fits_inside_the_jobs_own_cap():
    """The claim the whole change rests on, asserted against the real cap.

    A per-invocation bound does not bound the job: main() loops over os variants
    and tags, so the invocation count is not fixed and a bound that is safe once
    is not safe nine times. Raising BUILDX_TIMEOUT, weakening the reserve, or
    adding an os variant must all be caught here rather than by a five-hour job.

    Scoped to the tag count the scheduled jobs can produce, which
    test_the_scheduled_jobs_only_ever_build_one_tag pins. The envelope does not
    fit `--tag-type release` (four tags, 23948s); that shape is unreachable, and
    making it fit would force the bound below the slowest healthy attempt.
    """
    cap = JobConfigs.docker_server.timeout
    assert cap == JobConfigs.docker_keeper.timeout, "both jobs run this script"

    n_tags = len(gen_tags("26.8.1.1", "head"))
    variants = _os_default()
    for n_variants in (len(variants), len(variants) + 1):
        for push in (False, True):
            worst = _worst_case_job_seconds(n_variants, n_tags, push, cap)
            assert worst <= cap, (
                f"worst case {worst:.0f}s over {n_variants} os variants x "
                f"{n_tags} tags (push={push}) exceeds the job cap {cap}s"
            )


def test_the_scheduled_jobs_only_ever_build_one_tag():
    """The premise scoping the envelope above, made executable.

    The group count is variants x tags, and `--tag-type release` yields four
    tags, which the envelope does not fit. Both scheduled commands pass
    `--tag-type head`; if one ever switches, this fails and points at the
    envelope rather than letting the job wedge in production.
    """
    for job in (JobConfigs.docker_server, JobConfigs.docker_keeper):
        assert "--tag-type head" in job.command, (
            f"[{job.name}] no longer builds a single tag, so the worst-case "
            "envelope no longer holds; re-price it before changing this"
        )
    assert len(gen_tags("26.8.1.1", "head")) == 1


def test_a_healthy_first_build_still_gets_the_full_bound():
    """The shrinking deadline must not penalise a healthy job.

    The slowest healthy single attempt measured over 90 days was 1819.1s, so a
    bound at or below that would kill passing builds rather than wedged ones.
    """
    cap = JobConfigs.docker_server.timeout
    assert buildx_timeout(0.0, cap) == BUILDX_TIMEOUT
    assert BUILDX_TIMEOUT > 1900, "below the slowest healthy attempt seen in 90 days"


def test_an_exhausted_budget_never_yields_an_unbounded_timeout():
    """`timeout 0` runs the command unbounded, so 0 must be unreachable.

    Measured with GNU coreutils 9.4: `timeout 0 sleep 3` returns 0 after the full
    three seconds. A deadline computed as remaining-minus-reserve goes negative on
    a job that is already over budget, so without the clamp the last invocation
    would silently lose its bound - the exact hang this module exists to stop.
    """
    cap = JobConfigs.docker_server.timeout
    for elapsed in (cap - BUILDX_JOB_RESERVE, cap, cap * 10):
        assert buildx_timeout(elapsed, cap) >= BUILDX_TIMEOUT_FLOOR

    # and the floor really does expire, unlike 0
    got = _run(with_timeout("sleep 30", BUILDX_TIMEOUT_FLOOR // 60))
    assert got.returncode == 124


class _Elapsed:
    """Minimal Utils.Stopwatch stand-in: build_and_push_image reads `.duration`."""

    def __init__(self, seconds):
        self._seconds = seconds

    @property
    def duration(self):
        return self._seconds


def _commands_for(elapsed, push, tmp, monkeypatch):
    """Drive the real build_and_push_image and return the commands it produced.

    Result.from_commands_run is replaced by a spy returning an ok-stub, because
    build_and_push_image returns early on a failed result and would otherwise stop
    after the first arch.
    """
    captured = []

    def spy(**kwargs):
        captured.append(kwargs["command"])
        return type("_Ok", (), {"is_ok": lambda self: True})()

    # Each per-arch branch reads the buildx metadata file after its build.
    for arch in ARCH:
        (tmp / f"head-{arch}").write_text(
            '{"containerimage.digest": "sha256:0"}', encoding="utf-8"
        )
    monkeypatch.setattr(docker_server, "temp_path", tmp)
    monkeypatch.setattr(Result, "from_commands_run", spy)
    docker_server.build_and_push_image(
        image=docker_server.DockerImageData("clickhouse/clickhouse-server", "docker/server"),
        push=push,
        repo_urls={},
        os="ubuntu",
        tag="head",
        version="26.8.1.1",
        direct_urls={arch: [f"http://x/{arch}/clickhouse-server.deb"] for arch in ARCH},
        run_url="http://run",
        sha="deadbeef",
        apt_mirror_region="us-east-1",
        sw=_Elapsed(elapsed),
        job_timeout=JobConfigs.docker_server.timeout,
    )
    return captured


def _bounds(commands):
    """The seconds `timeout` is actually given, read back out of each command."""
    bounds = [re.search(r"--kill-after=\d+ (\d+) ", cmd) for cmd in commands]
    assert all(bounds), "a produced command carried no timeout bound at all"
    return [int(match.group(1)) for match in bounds]


def test_every_produced_command_gets_the_stopwatch_derived_bound(tmp_path, monkeypatch):
    """The envelope above proves the arithmetic; this proves it is wired in.

    test_the_whole_job_worst_case_fits_inside_the_jobs_own_cap calls buildx_timeout
    directly, and the two wiring arms only count source substrings, so reverting both
    call sites to a fixed bound keeps every other arm green. Driving the real
    build_and_push_image and reading the bound back out of each command it produces is
    what makes the shrinking deadline observable.
    """
    cap = JobConfigs.docker_server.timeout
    # Derived, not hardcoded: the elapsed that leaves room for exactly half the full
    # bound. A fixed value stops shrinking the bound if the reserve or the retry count
    # changes, which would make this arm pass vacuously on those very regressions.
    attempts = max(BUILDX_RETRIES, 2)
    late = float(cap - BUILDX_JOB_RESERVE - attempts * (BUILDX_TIMEOUT // 2))
    assert BUILDX_TIMEOUT_FLOOR < buildx_timeout(late, cap) < BUILDX_TIMEOUT, (
        "the chosen elapsed no longer shrinks the bound, so this arm proves nothing"
    )

    for push, expected_commands in ((True, len(ARCH) + 1), (False, len(ARCH))):
        fresh = _bounds(_commands_for(0.0, push, tmp_path, monkeypatch))
        shrunk = _bounds(_commands_for(late, push, tmp_path, monkeypatch))

        # Without this, "every command" would hold vacuously over a short list.
        assert len(fresh) == expected_commands, (
            f"expected {expected_commands} bounded commands with push={push}, "
            f"got {len(fresh)}"
        )
        assert len(shrunk) == len(fresh)

        for bound in fresh:
            assert bound == BUILDX_TIMEOUT, (
                "a fresh job must get the full bound, not a shrunken one"
            )
        # Computed, not hardcoded, so it tracks the constants.
        for bound in shrunk:
            assert bound == buildx_timeout(late, cap) < BUILDX_TIMEOUT, (
                f"push={push}: bound {bound} is not the stopwatch-derived value, so "
                "the call site is not passing the elapsed time"
            )


# --- the retry layer, driven through the real Shell.run ----------------------


def _attempts_through_shell_run_cmd(wrapped, retry_errors):
    """Run an already-wrapped command through Shell.run and count its attempts."""
    with tempfile.NamedTemporaryFile("w+", delete=False) as mark:
        path = mark.name
    try:
        code = Shell.run(
            f"echo x >> {path}; " + wrapped,
            verbose=False,
            retries=BUILDX_RETRIES,
            retry_errors=retry_errors,
        )
        with open(path, encoding="utf-8") as fp:
            return code, len(fp.read().split())
    finally:
        os.unlink(path)


def _attempts_through_shell_run(inner, retry_errors, seconds=1, kill_after=2):
    """Run `inner` through Shell.run and count how many attempts it made.

    The probe uses a short --kill-after so the escalated-137 path is observable
    in seconds; the shipped 120 is pinned separately by the string assertions.
    """
    wrapped = (
        f"{{ LC_ALL=C timeout --verbose --signal=TERM --kill-after={kill_after} "
        f"{seconds} {inner}; rc=$?; "
        f'if [ "$rc" = 124 ]; then echo "{BUILDX_TIMEOUT_MESSAGE} after {seconds}s" >&2; '
        "fi; exit $rc; }"
    )
    return _attempts_through_shell_run_cmd(wrapped, retry_errors)


def test_an_expiry_is_actually_retried_by_the_shipped_configuration():
    """Not the sentinel's presence in a list - the attempt count.

    BUILDX_RETRY_ERRORS is built from the sentinel constants, so asserting
    membership mutates on both sides and stays true even for a token `timeout`
    never emits. Only the attempt count distinguishes an armed retry from an
    unreachable one, which is the bug this change exists to remove.
    """
    code, attempts = _attempts_through_shell_run("sleep 30", BUILDX_RETRY_ERRORS)
    assert code == 124
    assert attempts == BUILDX_RETRIES, "the expiry was not retried"


def test_the_124_sentinel_is_the_only_thing_making_a_plain_expiry_retryable():
    """A TERM-honoured expiry matches no other shipped signature.

    `timeout` prints only `sending signal TERM to command` on that path, which no
    registry or apt token matches, so the echoed sentinel is what arms the retry.
    Asserting the constant appears in its own list cannot catch a broken value -
    dropping it from the list must change the attempt count.
    """
    plain = _run("LC_ALL=C timeout --signal=TERM 1 sleep 30")
    assert plain.returncode == 124
    others = [t for t in BUILDX_RETRY_ERRORS if t != BUILDX_TIMEOUT_MESSAGE]
    assert not [t for t in others if t in plain.stderr], (
        "another signature already matches a plain expiry, so this arm proves nothing"
    )

    code, attempts = _attempts_through_shell_run("sleep 30", others)
    assert code == 124
    assert attempts == 1, "a plain expiry retried without its own sentinel"


def test_an_escalated_expiry_is_retried_and_a_wrong_token_is_not():
    """The KILL diagnostic must match what `timeout` really prints.

    A build that ignores SIGTERM exits 137, not 124, so the diagnostic is the
    only thing identifying it as an expiry. Replacing the token with a string
    `timeout` never emits must drop the retry: that asymmetry is the assertion.
    """
    ignores_term = "bash -c 'trap \"\" TERM; sleep 30'"
    code, attempts = _attempts_through_shell_run(ignores_term, BUILDX_RETRY_ERRORS)
    assert code == 137
    assert attempts == BUILDX_RETRIES, "an escalated expiry was not retried"

    wrong = [t for t in BUILDX_RETRY_ERRORS if t != BUILDX_TIMEOUT_KILL_DIAG]
    wrong.append("timeout: this string is never printed by timeout")
    code, attempts = _attempts_through_shell_run(ignores_term, wrong)
    assert code == 137
    assert attempts == 1, (
        "a never-emitted KILL token still retried, so the arm proves nothing"
    )


def test_a_real_build_failure_is_not_retried_through_shell_run():
    """A Dockerfile error must fail fast rather than burn the retry budget."""
    code, attempts = _attempts_through_shell_run(
        "bash -c 'echo boom >&2; exit 7'", BUILDX_RETRY_ERRORS
    )
    assert code == 7
    assert attempts == 1


def test_a_bare_sigkill_is_not_retried_through_shell_run():
    """An OOM kill also exits 137, and retrying it would just repeat the OOM."""
    code, attempts = _attempts_through_shell_run(
        "bash -c 'kill -9 $$'", BUILDX_RETRY_ERRORS
    )
    assert code == 137
    assert attempts == 1


def test_the_shipped_wrapper_escalates_and_retries(monkeypatch):
    """Same escalation, but through with_timeout rather than a rebuilt copy.

    _attempts_through_shell_run above rebuilds the wrapper with its own kill delay, so
    it cannot observe BUILDX_TIMEOUT_KILL_AFTER at all: only the shipped wrapper can
    catch a non-positive delay. The delay is patched down to seconds because the
    escalated path is otherwise unobservable inside a pytest budget; the shipped 120
    is pinned by test_kill_escalation_diagnostic_is_enabled.
    """
    monkeypatch.setattr(docker_server, "BUILDX_TIMEOUT_KILL_AFTER", 2)
    wrapped = docker_server.with_timeout("bash -c 'trap \"\" TERM; sleep 30'", 1)
    assert "--kill-after=2" in wrapped, "the patch did not reach the shipped wrapper"

    code, attempts = _attempts_through_shell_run_cmd(wrapped, BUILDX_RETRY_ERRORS)
    assert code == 137, (
        f"expected the shipped wrapper to escalate to SIGKILL, got {code}; a "
        "non-positive kill delay leaves a TERM-ignoring build running to completion"
    )
    assert attempts == BUILDX_RETRIES, "the escalated expiry was not retried"
