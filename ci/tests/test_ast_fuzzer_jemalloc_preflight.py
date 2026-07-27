"""
Tests for the jemalloc safety-check preflight in `ci.jobs.ast_fuzzer_job`.

`ENABLE_JEMALLOC_SAFETY_CHECKS` is a compile-time cmake option, so the
`WeeklyJemallocSafety` lane's whole value depends on the consumed artifact really
having `config_opt_safety_checks` armed. A silently-ignored `-D` (option
refactored, build type renamed, a platform `jemalloc_internal_defs.h.in` gaining a
bare `#undef`, or the `ARCH_AMD64` guard turning the option off) would otherwise
leave the job running green as an ordinary `amd_debug` fuzz session with no signal
that the lane is vacuous.

`assert_jemalloc_safety_checks_armed` therefore reads the flag out of the binary
via `system.jemalloc_stats` and fails the job with `ERROR` (never `SKIPPED`, which
counts as success via `Result.is_ok()` and would be cached as one) when it is not
`true` or cannot be read at all.

The last tests pin the *guard*, not the helper: the four pre-existing AST fuzzer
paramsets must not run the probe, and the marker that selects the lane must be
derived from `BuildTypes.AMD_JEMALLOC_SAFETY` rather than hardcoded (a hardcoded
copy would stop matching after a build-type rename and disarm the lane silently).
"""

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
# `ci/defs/defs.py` does `from praktika import ...` rather than
# `from ci.praktika import ...`, so the `ci/` directory itself must be on the path
# for `import praktika` to resolve to `ci/praktika`. CI configures this via the
# praktika runner (`PYTHONPATH=./ci:.`); we replicate it here.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import ci.jobs.ast_fuzzer_job as m
from ci.defs.defs import BuildTypes
from ci.defs.job_configs import JobConfigs
from ci.praktika.result import Result
from ci.praktika.settings import Settings

# The check name the workflow really resolves for this lane, taken from the job
# config rather than hand-copied: `Job.Config.get_job_name_with_parameter` builds it
# from `BuildTypes.AMD_JEMALLOC_SAFETY`.
JEMALLOC_SAFETY_JOB_NAME = JobConfigs.jemalloc_safety_ast_fuzzer_job[0].name

# Every fuzzer paramset that existed before this lane, again from the configs, so the
# skip-list parametrization below cannot drift onto a name no job actually produces.
PRE_EXISTING_FUZZER_JOB_NAMES = frozenset(
    job.name
    for group in (
        JobConfigs.ast_fuzzer_jobs,
        JobConfigs.ast_fuzzer_targeted_pr_jobs,
        JobConfigs.buzz_fuzzer_jobs,
    )
    for job in group
)


# --- parse_jemalloc_safety_checks_flag ------------------------------------------------


@pytest.mark.parametrize(
    "output, expected",
    [
        ("config.opt_safety_checks: true", True),
        ("config.opt_safety_checks: false", False),
        # Trailing/leading noise as printed by `clickhouse local`.
        ("\nconfig.opt_safety_checks: true\n", True),
        ("config.opt_safety_checks:  false", False),
        # Nothing extractable.
        ("", None),
        ("\n", None),
        ("Code: 60. DB::Exception: Unknown table system.jemalloc_stats", None),
    ],
)
def test_parse_flag(output, expected):
    assert m.parse_jemalloc_safety_checks_flag(output) is expected


# --- assert_jemalloc_safety_checks_armed ----------------------------------------------


@pytest.fixture
def probe(monkeypatch, tmp_path):
    """Stub the shell probe; return a helper that runs the assertion.

    Returns `None` when the assertion passes, or the `Result` the job would have
    been completed with when it fails.
    """
    completed = []

    def _complete_job(self, *_a, **_k):
        completed.append(self)
        raise SystemExit(1)

    monkeypatch.setattr(Result, "complete_job", _complete_job, raising=True)

    # `Result.create_from` derives its name from `_Environment.get()`, which dumps a
    # dummy environment into Settings.TEMP_DIR outside CI. That directory is a job
    # artifact, not a test fixture, so create it here rather than depending on
    # whichever earlier test happened to make it (which made this file's outcome
    # depend on test ordering).
    Path(Settings.TEMP_DIR).mkdir(parents=True, exist_ok=True)

    def _run(stdout, exit_code=0, stderr=""):
        monkeypatch.setattr(
            m.Shell,
            "get_res_stdout_stderr",
            lambda *_a, **_k: (exit_code, stdout, stderr),
        )
        binary = tmp_path / "clickhouse"
        binary.write_text("", encoding="utf-8")
        completed.clear()
        try:
            m.assert_jemalloc_safety_checks_armed(binary)
        except SystemExit:
            assert completed, "job exited without completing a Result"
            return completed[0]
        assert not completed, "assertion passed but still completed a Result"
        return None

    return _run


def test_armed_binary_passes(probe):
    assert probe("config.opt_safety_checks: true") is None


def test_unarmed_binary_errors(probe):
    result = probe("config.opt_safety_checks: false")
    assert result is not None, "an unarmed binary must fail the job"
    assert result.status == Result.Status.ERROR
    assert "config.opt_safety_checks: false" in result.info


def test_empty_probe_output_errors(probe):
    result = probe("")
    assert result is not None, "an unreadable probe must fail the job"
    assert result.status == Result.Status.ERROR


def test_unparseable_probe_output_errors(probe):
    result = probe(
        "Code: 60. DB::Exception: Unknown table expression identifier "
        "'system.jemalloc_stats'",
        exit_code=60,
        stderr="Code: 60",
    )
    assert result is not None, "an unparseable probe must fail the job"
    assert result.status == Result.Status.ERROR


# --- the check_name guard -------------------------------------------------------------


class _Stop(Exception):
    """Sentinel raised right after the preflight decision point."""


@pytest.fixture
def guard(monkeypatch, tmp_path):
    """Run `run_fuzz_job` up to just past the preflight; report whether it ran."""
    calls = []

    monkeypatch.setattr(m, "cwd", str(tmp_path))
    binary = tmp_path / "ci" / "tmp" / "clickhouse"
    binary.parent.mkdir(parents=True, exist_ok=True)
    binary.write_text("", encoding="utf-8")

    monkeypatch.setattr(
        m, "assert_jemalloc_safety_checks_armed", lambda b: calls.append(b)
    )

    def _no_docker(*_a, **_k):
        raise _Stop()

    monkeypatch.setattr(m.DockerImage, "get_docker_image", _no_docker)

    def _run(check_name):
        calls.clear()
        with pytest.raises(_Stop):
            m.run_fuzz_job(check_name)
        return len(calls)

    return _run


def test_guard_runs_preflight_for_the_jemalloc_safety_paramset(guard):
    # Drive the guard with the check name the workflow ACTUALLY resolves, not a
    # hand-copied literal: `Job.Config.get_job_name_with_parameter` derives it from
    # `BuildTypes.AMD_JEMALLOC_SAFETY`, so a rename of the build type must not leave
    # this test green while the lane runs unguarded.
    assert guard(JEMALLOC_SAFETY_JOB_NAME) == 1


def test_marker_tracks_the_build_type():
    """The marker must follow `BuildTypes.AMD_JEMALLOC_SAFETY`, not drift from it.

    `JEMALLOC_SAFETY_CHECK_MARKER` is kept a literal in the job script (importing
    `ci.defs.defs` there would require `./ci` itself on `sys.path`, which the
    developer-facing entry points do not all set), so the coupling to the build type
    is asserted here instead. Without it the constant fails *open*: after a rename
    the guard stops matching, the preflight never runs, and the lane silently
    degrades into an ordinary `amd_debug` fuzz session - exactly one of the
    degradation modes the preflight exists to catch.
    """
    assert m.JEMALLOC_SAFETY_CHECK_MARKER in BuildTypes.AMD_JEMALLOC_SAFETY.lower()
    # ... and reaches the resolved check name, which is what the guard actually sees.
    assert m.JEMALLOC_SAFETY_CHECK_MARKER in JEMALLOC_SAFETY_JOB_NAME.lower()


def test_marker_does_not_match_the_pre_existing_build_types():
    """The marker must select ONLY the jemalloc-safety lane.

    A marker that also matched another build type would make every AST fuzzer
    paramset run the preflight and fail those jobs on an ordinary `amd_debug`
    artifact.
    """
    others = [
        v
        for k, v in vars(BuildTypes).items()
        if not k.startswith("_")
        and isinstance(v, str)
        and v != BuildTypes.AMD_JEMALLOC_SAFETY
    ]
    assert others, "no other build types found - the reflection above broke"
    assert [v for v in others if m.JEMALLOC_SAFETY_CHECK_MARKER in v.lower()] == []


@pytest.mark.parametrize(
    "check_name",
    [
        "AST fuzzer (amd_debug)",
        "AST fuzzer (amd_debug, targeted)",
        "AST fuzzer (amd_debug, targeted, old_compatibility)",
        "BuzzHouse (amd_debug)",
    ],
)
def test_guard_skips_preflight_for_the_pre_existing_paramsets(guard, check_name):
    # Each literal must be a name the real job configs still produce, otherwise this
    # test would keep passing against a paramset that no longer exists while the one
    # that does goes uncovered.
    assert check_name in PRE_EXISTING_FUZZER_JOB_NAMES, (
        f"{check_name!r} is not a real fuzzer paramset; the pre-existing names are "
        f"{sorted(PRE_EXISTING_FUZZER_JOB_NAMES)}"
    )
    assert guard(check_name) == 0
