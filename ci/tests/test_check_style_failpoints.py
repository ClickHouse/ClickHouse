"""
Tests for the `SYSTEM DISABLE ALL FAILPOINTS` exemption of
`check_failpoints_are_disabled` (ci/jobs/check_style.py).

That statement disarms the fail points of whatever else is running, so it stands in for
the per-name disables only in a test the runner schedules alone. `clickhouse-test`
schedules a test alone when its parsed tag set holds `no-parallel`, so the exemption has
to be decided on that same tag set: `no-parallel-replicas` is a different tag, it changes
nothing about scheduling, and a test carrying it runs in the parallel pass.
"""

import os
import sys

import pytest

_REPO_ROOT = os.path.join(os.path.dirname(__file__), "../..")
sys.path.insert(0, _REPO_ROOT)
# `check_style.py` imports praktika unqualified.
sys.path.insert(0, os.path.join(_REPO_ROOT, "ci"))

from ci.jobs.check_style import check_failpoints_are_disabled

# A fail point armed and cleared only by the server-wide disable-all, in both test forms.
_BODIES = {
    ".sql": (
        "SYSTEM ENABLE FAILPOINT dummy_failpoint;\n"
        "SELECT 1;\n"
        "SYSTEM DISABLE ALL FAILPOINTS;\n"
    ),
    ".sh": (
        "#!/usr/bin/env bash\n"
        "$CLICKHOUSE_CLIENT -q 'SYSTEM ENABLE FAILPOINT dummy_failpoint'\n"
        "$CLICKHOUSE_CLIENT -q 'SELECT 1'\n"
        "$CLICKHOUSE_CLIENT -q 'SYSTEM DISABLE ALL FAILPOINTS'\n"
    ),
}


def _check(tmp_path, tags_line, suffix=".sql"):
    suite = tmp_path / "queries" / "0_stateless"
    suite.mkdir(parents=True, exist_ok=True)
    test_case = suite / f"00001_failpoint{suffix}"
    comment_sign = "--" if suffix == ".sql" else "#"
    test_case.write_text(f"{comment_sign} Tags: {tags_line}\n{_BODIES[suffix]}")
    return check_failpoints_are_disabled([str(test_case)])


@pytest.mark.parametrize("suffix", [".sql", ".sh"])
@pytest.mark.parametrize(
    "tags_line",
    [
        "no-parallel-replicas",
        "no-parallel-replicas, no-fasttest",
        "no-fasttest, no-parallel-replicas",
    ],
)
def test_no_parallel_replicas_does_not_grant_the_exemption(tmp_path, tags_line, suffix):
    errors = _check(tmp_path, tags_line, suffix)
    assert "dummy_failpoint" in errors and "DISABLE ALL FAILPOINTS" in errors, (
        f"a {suffix} test tagged `{tags_line}` runs in the parallel pass, so its "
        f"disable-all must not be accepted as the cleanup; got: {errors!r}"
    )


@pytest.mark.parametrize("suffix", [".sql", ".sh"])
@pytest.mark.parametrize(
    "tags_line",
    [
        "no-parallel",
        "no-parallel, no-fasttest",
        "no-fasttest, no-parallel",
        "no-parallel-replicas, no-parallel",
    ],
)
def test_no_parallel_grants_the_exemption(tmp_path, tags_line, suffix):
    errors = _check(tmp_path, tags_line, suffix)
    assert errors == "", f"a {suffix} test tagged `{tags_line}` runs alone"


@pytest.mark.parametrize("suffix", [".sql", ".sh"])
def test_fixture_is_flagged_without_a_scheduling_tag(tmp_path, suffix):
    # Guards the cases above: the fixture must be one the check rejects on its own.
    errors = _check(tmp_path, "no-fasttest", suffix)
    assert "dummy_failpoint" in errors and "DISABLE ALL FAILPOINTS" in errors
