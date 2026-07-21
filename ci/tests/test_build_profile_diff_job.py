"""Fixture-backed tests for the "Build profile diff" consumer.

``ci/jobs/build_profile_diff_job.py`` compares a PR's ``arm_release`` build
profile against a master baseline by aggregating rows from the CI logs cluster.
Its digest includes the consumer file, so a change to it re-runs the diff job -
but ``Build (arm_release)`` is cacheable under a digest that does not include the
consumer, so a consumer-only change gets no fresh build and the job would return
a green "no data" result without ever exercising the changed SQL. These tests
close that blind spot: they run the real query builders against a fixture
database (``clickhouse local``) so a consumer change cannot merge unvalidated.

The central invariant is run scoping. A rerun of the build re-inserts the whole
dataset under the same pull_request_number/commit_sha/check_name with a fresh
(check_start_time, instance_id); mixing runs corrupts every aggregation. The
consumer must pin each side to one concrete run - chosen once and reused for
every table - so that a table missing on the newest run reads as absent instead
of silently falling back to an older run.
"""

import json
import os
import shutil
import subprocess
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs import build_profile_diff_job as job

pytestmark = pytest.mark.skipif(
    not shutil.which("clickhouse"), reason="no clickhouse binary for clickhouse local"
)

_MAIN = job.MAIN_BINARY  # ./ci/tmp/build/programs/clickhouse

# Two PR reruns: the newest (T2/I2) changed the main binary and, like a real LTO
# build, uploaded no symbols; the older (T1/I1) has both a different size and
# symbol data. A consumer that does not pin to one run would read the older
# run's symbols and the wrong size.
_PR = 111164
_PR_SHA = "prsha"
_BASE_SHA = "basesha"
_SCHEMA = f"""
CREATE TABLE binary_sizes
(
    date Date DEFAULT today(), pull_request_number Int64, commit_sha String,
    check_start_time DateTime, check_name String, instance_type String,
    instance_id String, file String, size UInt64
) ENGINE = Memory;

CREATE TABLE binary_symbols
(
    date Date DEFAULT today(), pull_request_number Int64, commit_sha String,
    check_start_time DateTime, check_name String, instance_type String,
    instance_id String, file String, address UInt64, size Int64, type String,
    symbol String
) ENGINE = Memory;

CREATE TABLE build_time_trace
(
    date Date DEFAULT today(), pull_request_number Int64, commit_sha String,
    check_start_time DateTime, check_name String, instance_type String,
    instance_id String, file String, name String, detail String, dur UInt64,
    time DateTime64(6) DEFAULT now64()
) ENGINE = Memory;

INSERT INTO binary_sizes (pull_request_number, commit_sha, check_start_time, check_name, instance_id, file, size) VALUES
    ({_PR}, '{_PR_SHA}', '2026-07-01 00:00:00', 'arm_release', 'I1', '{_MAIN}', 999999),
    ({_PR}, '{_PR_SHA}', '2026-07-02 00:00:00', 'arm_release', 'I2', '{_MAIN}', 1500),
    (0, '{_BASE_SHA}', '2026-06-30 00:00:00', 'arm_release', 'I0', '{_MAIN}', 1400);

-- Symbols exist only for the OLDER PR run and for master, never for the newest
-- PR run. Correct pinning must therefore find no comparable PR symbols.
INSERT INTO binary_symbols (pull_request_number, commit_sha, check_start_time, check_name, instance_id, file, address, size, type, symbol) VALUES
    ({_PR}, '{_PR_SHA}', '2026-07-01 00:00:00', 'arm_release', 'I1', '{_MAIN}', 0, 500000, 't', 'stale_symbol'),
    (0, '{_BASE_SHA}', '2026-06-30 00:00:00', 'arm_release', 'I0', '{_MAIN}', 0, 100000, 't', 'stale_symbol');
"""


class FixtureDb:
    """A job.Db drop-in that runs each query against a fresh clickhouse local.

    The schema and fixtures are prepended to every query in one invocation so
    ``today()`` is evaluated consistently on both the inserted ``date`` and the
    ``date >= today() - N`` predicates.
    """

    def query(self, query):
        script = _SCHEMA + "\n" + query + " FORMAT JSON"
        out = subprocess.run(
            ["clickhouse", "local", "--multiquery"],
            input=script,
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        return json.loads(out)["data"]


def test_resolve_run_picks_newest_run():
    """The pinned PR run is the newest (check_start_time, instance_id)."""
    side = job.resolve_run(FixtureDb(), job.PR_DAYS, _PR, _PR_SHA)
    assert side is not None
    assert side.instance_id == "I2"
    assert side.check_start_time == "2026-07-02 00:00:00"


def test_resolve_run_none_for_absent_data():
    """A consumer-only change with no fresh build produces no rows -> no run.

    This is the "cache hit / no PR data" case: the job must fall through to its
    skip result rather than compare against a stale run.
    """
    assert job.resolve_run(FixtureDb(), job.PR_DAYS, _PR, "no-such-sha") is None
    assert not job.has_pr_data(FixtureDb(), _PR, "no-such-sha")


def test_binary_sizes_read_from_the_pinned_run_only():
    """Binary size comes from the newest run, not the older/larger rerun."""
    db = FixtureDb()
    pr_side = job.resolve_run(db, job.PR_DAYS, _PR, _PR_SHA)
    base_side = job.resolve_run(db, job.BASE_DAYS, 0, _BASE_SHA)
    section = job.compare_binaries(db, pr_side, base_side)
    # 1500 (newest run) vs 1400 (base); the stale 999999 from run I1 is ignored.
    assert "1.46 KiB" in section.body  # 1500 B
    assert "999999" not in section.body
    assert "999.99" not in section.body


def test_symbols_do_not_fall_back_to_an_older_run():
    """The newest run has no symbols, so no symbol comparison is produced.

    Reading the older run's symbols here would be exactly the cross-run mixing
    the check must avoid: it would compare a symbol from run I1 against master
    while every other section reflects run I2.
    """
    db = FixtureDb()
    pr_side = job.resolve_run(db, job.PR_DAYS, _PR, _PR_SHA)
    base_side = job.resolve_run(db, job.BASE_DAYS, 0, _BASE_SHA)
    section = job.compare_symbols(db, pr_side, base_side)
    assert "stale_symbol" not in section.body
    assert not section.significant
