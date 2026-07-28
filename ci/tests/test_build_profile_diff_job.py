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
_KEEPER = job.FINAL_BINARIES[1]  # ./ci/tmp/build/programs/clickhouse-keeper
_STRIPPED = job.HEADLINE_BINARIES[0]  # ./ci/tmp/build/programs/clickhouse-stripped
_OBJ = f"{job.BUILD_DIR}/src/CMakeFiles/dbms.dir/foo.cpp.o"

# Two PR reruns: the newest (T2/I2) changed the main binary and, like a real LTO
# build, uploaded no symbols; the older (T1/I1) has both a different size and
# symbol data. A consumer that does not pin to one run would read the older
# run's symbols and the wrong size.
_PR = 111164
_PR_SHA = "prsha"
_BASE_SHA = "basesha"
_OLD_WARMUP_SHA = "oldwarmupsha"

# Section-wide compile-time shift. Twelve translation units that the master
# warmup build compiled at 30 s each, recompiled by two PR runs:
#   * ``prsha-uniform-slowdown`` takes 65 s for every one of them - the kind of
#     shift a heavy common header produces. It moves the median ratio itself,
#     so every per-TU delta measured against that ratio is exactly zero and the
#     section can only catch it on its own level;
#   * ``prsha-machine-skew`` takes 33 s - the few percent the runners really do
#     differ by, which must stay an informational note.
_SKEW_TUS = [f"skew_tu{i}.cpp" for i in range(12)]


def _skew_rows(pr_number, sha, check_start_time, check_name, instance_id, dur_us):
    return ",\n    ".join(
        f"({pr_number}, '{sha}', '{check_start_time}', '{check_name}', '{instance_id}', '{tu}', 'ExecuteCompiler', '', {dur_us})"
        for tu in _SKEW_TUS
    )


_SKEW_BASE_ROWS = _skew_rows(0, _BASE_SHA, "2026-06-30 00:00:00", "arm_release_pr_cache_warmup", "W0", 30000000)
_SKEW_SLOW_ROWS = _skew_rows(_PR, "prsha-uniform-slowdown", "2026-07-02 00:00:00", "arm_release", "I11", 65000000)
_SKEW_NOISE_ROWS = _skew_rows(_PR, "prsha-machine-skew", "2026-07-02 00:00:00", "arm_release", "I12", 33000000)
# Half of the same twelve translation units doubled (30 s -> 60 s), half
# unchanged: the two middle ratios are 1.0 and 2.0, so the median is 1.5 and
# the upper middle element (2.0) would erase the regressed half.
_SKEW_HALF_ROWS = ",\n    ".join(
    f"({_PR}, 'prsha-half-slowdown', '2026-07-02 00:00:00', 'arm_release', 'I13', '{tu}', 'ExecuteCompiler', '', {60000000 if i % 2 else 30000000})"
    for i, tu in enumerate(_SKEW_TUS)
)

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
    instance_id String, file String, library LowCardinality(String) DEFAULT '',
    name String, detail String, dur UInt64,
    time DateTime64(6) DEFAULT now64()
) ENGINE = Memory;

-- The object-size baseline (last row) comes from the master warmup build
-- (compiled with the PR's flags); the official master build's objects carry
-- debug info and must not be compared.
INSERT INTO binary_sizes (pull_request_number, commit_sha, check_start_time, check_name, instance_id, file, size) VALUES
    ({_PR}, '{_PR_SHA}', '2026-07-01 00:00:00', 'arm_release', 'I1', '{_MAIN}', 999999),
    ({_PR}, '{_PR_SHA}', '2026-07-01 00:00:00', 'arm_release', 'I1', '{_STRIPPED}', 999999),
    ({_PR}, '{_PR_SHA}', '2026-07-02 00:00:00', 'arm_release', 'I2', '{_MAIN}', 1500),
    ({_PR}, '{_PR_SHA}', '2026-07-02 00:00:00', 'arm_release', 'I2', '{_STRIPPED}', 1500),
    ({_PR}, '{_PR_SHA}', '2026-07-02 00:00:00', 'arm_release', 'I2', '{_OBJ}', 819200),
    (0, '{_BASE_SHA}', '2026-06-30 00:00:00', 'arm_release', 'I0', '{_MAIN}', 1400),
    (0, '{_BASE_SHA}', '2026-06-30 00:00:00', 'arm_release', 'I0', '{_STRIPPED}', 1400),
    (0, '{_BASE_SHA}', '2026-06-30 00:00:00', 'arm_release_pr_cache_warmup', 'W0', '{_OBJ}', 262144);

-- Symbols exist only for the OLDER PR run and for master, never for the newest
-- PR run. Correct pinning must therefore find no comparable PR symbols.
INSERT INTO binary_symbols (pull_request_number, commit_sha, check_start_time, check_name, instance_id, file, address, size, type, symbol) VALUES
    ({_PR}, '{_PR_SHA}', '2026-07-01 00:00:00', 'arm_release', 'I1', '{_MAIN}', 0, 500000, 't', 'stale_symbol'),
    (0, '{_BASE_SHA}', '2026-06-30 00:00:00', 'arm_release', 'I0', '{_MAIN}', 0, 100000, 't', 'stale_symbol');

-- Compile-time fixture. The PR (pinned run I2) recompiles two TUs:
--   * ``tu.cpp`` (50 s) has a warmup baseline (20 s) and a per-entity breakdown;
--   * ``new_tu.cpp`` (40 s) has no warmup baseline at all.
-- The compile-time baseline comes from the master *warmup* build (compiled
-- with the PR's flags). Master commit ``basesha`` has TWO warmup runs: the
-- OLDER one (W0/2026-06-30) recompiled ``tu.cpp`` with entity rows; the NEWER
-- one (W0b/2026-07-01) recompiled only ``other_tu.cpp`` and never touched
-- ``tu.cpp``. The drill-down must read ``tu.cpp`` entities from the run that
-- actually compiled it, not re-resolve to the newer run (which would report
-- every entity as new).
INSERT INTO build_time_trace (pull_request_number, commit_sha, check_start_time, check_name, instance_id, file, name, detail, dur) VALUES
    ({_PR}, '{_PR_SHA}', '2026-07-02 00:00:00', 'arm_release', 'I2', 'tu.cpp', 'ExecuteCompiler', '', 50000000),
    ({_PR}, '{_PR_SHA}', '2026-07-02 00:00:00', 'arm_release', 'I2', 'tu.cpp', 'InstantiateFunction', 'foo', 30000000),
    ({_PR}, '{_PR_SHA}', '2026-07-02 00:00:00', 'arm_release', 'I2', 'new_tu.cpp', 'ExecuteCompiler', '', 40000000),
    (0, '{_BASE_SHA}', '2026-06-30 00:00:00', 'arm_release_pr_cache_warmup', 'W0', 'tu.cpp', 'ExecuteCompiler', '', 20000000),
    (0, '{_BASE_SHA}', '2026-06-30 00:00:00', 'arm_release_pr_cache_warmup', 'W0', 'tu.cpp', 'InstantiateFunction', 'foo', 5000000),
    (0, '{_BASE_SHA}', '2026-07-01 00:00:00', 'arm_release_pr_cache_warmup', 'W0b', 'other_tu.cpp', 'ExecuteCompiler', '', 10000000);

-- One source compiled into two targets: with BUILD_STANDALONE_KEEPER=1,
-- ``programs/keeper/Keeper.cpp`` is built into both ``clickhouse-keeper-lib``
-- and the standalone ``clickhouse-keeper``; ``prepare-time-trace.sh`` strips
-- the ``CMakeFiles/<target>.dir`` path component and keeps the target only in
-- ``library``. The PR's standalone compile regressed (10 s -> 60 s) while the
-- library compile is unchanged (12 s): keyed by file alone, the two jobs
-- collapse into one pseudo-TU and the comparison mixes them up.
INSERT INTO build_time_trace (pull_request_number, commit_sha, check_start_time, check_name, instance_id, file, library, name, detail, dur) VALUES
    ({_PR}, '{_PR_SHA}', '2026-07-02 00:00:00', 'arm_release', 'I2', 'programs/keeper/Keeper.cpp', 'clickhouse-keeper', 'ExecuteCompiler', '', 60000000),
    ({_PR}, '{_PR_SHA}', '2026-07-02 00:00:00', 'arm_release', 'I2', 'programs/keeper/Keeper.cpp', 'clickhouse-keeper-lib', 'ExecuteCompiler', '', 12000000),
    (0, '{_BASE_SHA}', '2026-06-30 00:00:00', 'arm_release_pr_cache_warmup', 'W0', 'programs/keeper/Keeper.cpp', 'clickhouse-keeper', 'ExecuteCompiler', '', 10000000),
    (0, '{_BASE_SHA}', '2026-06-30 00:00:00', 'arm_release_pr_cache_warmup', 'W0', 'programs/keeper/Keeper.cpp', 'clickhouse-keeper-lib', 'ExecuteCompiler', '', 12000000);

-- A master commit whose only warmup profile data is 8-14 days old: too old for
-- the BASE_DAYS object-size baseline, but well inside the TU_BASE_DAYS window
-- the per-TU compile-time comparison advertises. The compile-time section must
-- still compare against it instead of degrading to the catch-up note.
INSERT INTO binary_sizes (date, pull_request_number, commit_sha, check_start_time, check_name, instance_id, file, size) VALUES
    (today() - 10, 0, '{_OLD_WARMUP_SHA}', '2026-06-20 00:00:00', 'arm_release_pr_cache_warmup', 'W9', '{_OBJ}', 262144);
INSERT INTO build_time_trace (date, pull_request_number, commit_sha, check_start_time, check_name, instance_id, file, name, detail, dur) VALUES
    (today() - 10, 0, '{_OLD_WARMUP_SHA}', '2026-06-20 00:00:00', 'arm_release_pr_cache_warmup', 'W9', 'tu.cpp', 'ExecuteCompiler', '', 10000000);

-- ThinLTO link-trace fixture: both final binaries have OptFunction rows on
-- both sides. ``main_fn`` (clickhouse) is unchanged; ``keeper_fn``
-- (clickhouse-keeper) regresses 5 s -> 30 s, above the significance bar.
INSERT INTO build_time_trace (pull_request_number, commit_sha, check_start_time, check_name, instance_id, file, name, detail, dur) VALUES
    ({_PR}, '{_PR_SHA}', '2026-07-02 00:00:00', 'arm_release', 'I2', '{_MAIN}', 'OptFunction', 'main_fn', 10000000),
    ({_PR}, '{_PR_SHA}', '2026-07-02 00:00:00', 'arm_release', 'I2', '{_KEEPER}', 'OptFunction', 'keeper_fn', 30000000),
    (0, '{_BASE_SHA}', '2026-06-30 00:00:00', 'arm_release', 'I0', '{_MAIN}', 'OptFunction', 'main_fn', 10000000),
    (0, '{_BASE_SHA}', '2026-06-30 00:00:00', 'arm_release', 'I0', '{_KEEPER}', 'OptFunction', 'keeper_fn', 5000000);

-- A PR run whose producer lost the keeper link trace: OptFunction rows exist
-- for ``clickhouse`` only, while the master baseline has both binaries.
INSERT INTO build_time_trace (pull_request_number, commit_sha, check_start_time, check_name, instance_id, file, name, detail, dur) VALUES
    ({_PR}, 'prsha-lost-keeper', '2026-07-02 00:00:00', 'arm_release', 'I5', '{_MAIN}', 'OptFunction', 'main_fn', 10000000);

-- A PR run whose producer lost the stripped binary's size row: the run still
-- resolves via the main binary, so the headline size comparison would come up
-- empty. And a master run missing the same row, for the baseline direction.
INSERT INTO binary_sizes (pull_request_number, commit_sha, check_start_time, check_name, instance_id, file, size) VALUES
    ({_PR}, 'prsha-lost-stripped', '2026-07-02 00:00:00', 'arm_release', 'I6', '{_MAIN}', 1500),
    (0, 'basesha-no-stripped', '2026-06-30 00:00:00', 'arm_release', 'I7', '{_MAIN}', 1400);

-- A PR run that adds one large object file (bar.cpp.o, 512 KiB, absent from
-- the warmup baseline) and no longer builds the baseline's foo.cpp.o: both
-- one-sided rows are above OBJECT_SIG_BYTES and must drive the verdict.
INSERT INTO binary_sizes (pull_request_number, commit_sha, check_start_time, check_name, instance_id, file, size) VALUES
    ({_PR}, 'prsha-new-object', '2026-07-02 00:00:00', 'arm_release', 'I8', '{_MAIN}', 1500),
    ({_PR}, 'prsha-new-object', '2026-07-02 00:00:00', 'arm_release', 'I8', '{_STRIPPED}', 1500),
    ({_PR}, 'prsha-new-object', '2026-07-02 00:00:00', 'arm_release', 'I8', '{job.BUILD_DIR}/src/CMakeFiles/dbms.dir/bar.cpp.o', 524288);

-- A PR run that adds two translation units with no master baseline at all:
-- ``mid_tu.cpp`` (25 s) sits between TU_REPORT_SECONDS and the old hard-wired
-- 30 s report cutoff, yet above TU_SIG_SECONDS, so it must be listed AND drive
-- the verdict; ``small_tu.cpp`` (7 s) is reportable but not significant.
INSERT INTO binary_sizes (pull_request_number, commit_sha, check_start_time, check_name, instance_id, file, size) VALUES
    ({_PR}, 'prsha-midsize-new-tu', '2026-07-02 00:00:00', 'arm_release', 'I10', '{_MAIN}', 1500),
    ({_PR}, 'prsha-midsize-new-tu', '2026-07-02 00:00:00', 'arm_release', 'I10', '{_STRIPPED}', 1500);
INSERT INTO build_time_trace (pull_request_number, commit_sha, check_start_time, check_name, instance_id, file, name, detail, dur) VALUES
    ({_PR}, 'prsha-midsize-new-tu', '2026-07-02 00:00:00', 'arm_release', 'I10', 'mid_tu.cpp', 'ExecuteCompiler', '', 25000000),
    ({_PR}, 'prsha-midsize-new-tu', '2026-07-02 00:00:00', 'arm_release', 'I10', 'small_tu.cpp', 'ExecuteCompiler', '', 7000000);

-- A PR run whose keeper link trace is intact but entirely below the 50 ms
-- reporting cutoff: it must read as present, not as a lost upload.
INSERT INTO build_time_trace (pull_request_number, commit_sha, check_start_time, check_name, instance_id, file, name, detail, dur) VALUES
    ({_PR}, 'prsha-tiny-keeper', '2026-07-02 00:00:00', 'arm_release', 'I9', '{_MAIN}', 'OptFunction', 'main_fn', 10000000),
    ({_PR}, 'prsha-tiny-keeper', '2026-07-02 00:00:00', 'arm_release', 'I9', '{_KEEPER}', 'OptFunction', 'keeper_fn', 40000);

-- Section-wide compile-time shift (see _SKEW_TUS): the same twelve baseline
-- translation units recompiled twice as slowly by one PR run and marginally
-- more slowly by another.
INSERT INTO binary_sizes (pull_request_number, commit_sha, check_start_time, check_name, instance_id, file, size) VALUES
    ({_PR}, 'prsha-uniform-slowdown', '2026-07-02 00:00:00', 'arm_release', 'I11', '{_MAIN}', 1500),
    ({_PR}, 'prsha-uniform-slowdown', '2026-07-02 00:00:00', 'arm_release', 'I11', '{_STRIPPED}', 1500),
    ({_PR}, 'prsha-machine-skew', '2026-07-02 00:00:00', 'arm_release', 'I12', '{_MAIN}', 1500),
    ({_PR}, 'prsha-machine-skew', '2026-07-02 00:00:00', 'arm_release', 'I12', '{_STRIPPED}', 1500),
    ({_PR}, 'prsha-half-slowdown', '2026-07-02 00:00:00', 'arm_release', 'I13', '{_MAIN}', 1500),
    ({_PR}, 'prsha-half-slowdown', '2026-07-02 00:00:00', 'arm_release', 'I13', '{_STRIPPED}', 1500);
INSERT INTO build_time_trace (pull_request_number, commit_sha, check_start_time, check_name, instance_id, file, name, detail, dur) VALUES
    {_SKEW_BASE_ROWS};
INSERT INTO build_time_trace (pull_request_number, commit_sha, check_start_time, check_name, instance_id, file, name, detail, dur) VALUES
    {_SKEW_HALF_ROWS};
INSERT INTO build_time_trace (pull_request_number, commit_sha, check_start_time, check_name, instance_id, file, name, detail, dur) VALUES
    {_SKEW_SLOW_ROWS};
INSERT INTO build_time_trace (pull_request_number, commit_sha, check_start_time, check_name, instance_id, file, name, detail, dur) VALUES
    {_SKEW_NOISE_ROWS};
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
    # Only the stripped binary is comparable against the official master build
    # (master keeps debug symbols, PR builds strip them).
    assert "clickhouse-stripped" in section.body
    assert f"`{job.strip_build_dir(_MAIN)}`" not in section.body


def test_binary_sizes_missing_pr_side_stripped_is_flagged():
    """A lost PR-side stripped size row is an incomplete comparison, not green.

    The run resolves via the main binary's rows, so losing only the
    ``clickhouse-stripped`` row would silently empty the headline size
    section while the job still renders "no significant changes".
    """
    db = FixtureDb()
    pr_side = job.resolve_run(db, job.PR_DAYS, _PR, "prsha-lost-stripped")
    base_side = job.resolve_run(db, job.BASE_DAYS, 0, _BASE_SHA)
    assert pr_side is not None and base_side is not None
    section = job.compare_binaries(db, pr_side, base_side)
    assert section.significant
    assert "clickhouse-stripped" in section.body
    assert "incomplete" in section.body
    assert "missing PR-side size data" in section.summary


def test_binary_sizes_missing_baseline_is_called_out():
    """A baseline without the stripped size row shows a note, not silence."""
    db = FixtureDb()
    pr_side = job.resolve_run(db, job.PR_DAYS, _PR, _PR_SHA)
    base_side = job.resolve_run(db, job.BASE_DAYS, 0, "basesha-no-stripped")
    assert pr_side is not None and base_side is not None
    section = job.compare_binaries(db, pr_side, base_side)
    assert not section.significant
    assert "No master baseline size data yet" in section.body
    assert "clickhouse-stripped" in section.body


def test_objects_compared_against_the_warmup_build():
    """Object sizes are baselined on the flag-identical master warmup build.

    The official master build's object files carry debug info that PR builds
    strip (-DDISABLE_ALL_DEBUG_SYMBOLS=1), so comparing against it would show
    every object as massively changed on every PR. The warmup build compiles
    master with the PR's exact flags.
    """
    db = FixtureDb()
    pr_side = job.resolve_run(db, job.PR_DAYS, _PR, _PR_SHA)
    assert job.find_warmup_baseline(db, [_BASE_SHA], _PR_SHA) == _BASE_SHA
    warmup_side = job.resolve_run(db, job.BASE_DAYS, 0, _BASE_SHA, check_name=job.WARMUP_CHECK_NAME)
    assert warmup_side is not None
    section = job.compare_objects(db, pr_side, warmup_side)
    # 800 KiB (PR) vs 256 KiB (warmup baseline): above the significance bar.
    assert "foo.cpp.o" in section.body
    assert section.significant


def test_objects_degrade_to_catchup_note_without_warmup_baseline():
    """No warmup data yet -> a catch-up note, not a bogus comparison."""
    db = FixtureDb()
    pr_side = job.resolve_run(db, job.PR_DAYS, _PR, _PR_SHA)
    assert job.find_warmup_baseline(db, ["sha-without-warmup-data"], _PR_SHA) is None
    section = job.compare_objects(db, pr_side, None)
    assert section.body == job.WARMUP_CATCHUP_NOTE
    assert not section.significant
    section = job.compare_compile_times(db, pr_side, ["sha-without-warmup-data"])
    assert section.body == job.WARMUP_CATCHUP_NOTE
    assert not section.significant


def test_symbols_do_not_fall_back_to_an_older_run():
    """The newest run has no symbols: no fallback, and the loss is flagged.

    Reading the older run's symbols here would be exactly the cross-run mixing
    the check must avoid: it would compare a symbol from run I1 against master
    while every other section reflects run I2. And since the master baseline
    does have symbol data, a PR run without any means the profile producer
    lost it - the section must say the comparison is incomplete instead of
    rendering an all-green empty body.
    """
    db = FixtureDb()
    pr_side = job.resolve_run(db, job.PR_DAYS, _PR, _PR_SHA)
    base_side = job.resolve_run(db, job.BASE_DAYS, 0, _BASE_SHA)
    section = job.compare_symbols(db, pr_side, base_side)
    assert "stale_symbol" not in section.body
    assert "uploaded none" in section.body
    assert section.significant
    assert "missing PR-side symbol data" in section.summary


def test_drill_down_reads_the_run_that_compiled_the_tu():
    """The per-TU drill-down uses the master run that actually recompiled the TU.

    ``basesha`` has a newer run (I0b) that never touched ``tu.cpp``. Re-resolving
    the drill-down from the commit sha alone would pin it to that newer run and
    find no baseline entities, marking every PR entity as new. The pinned run
    (I0, carried out of ``compare_compile_times``) does have ``tu.cpp`` entities,
    so ``foo`` is compared as a real slowdown (5 s -> 30 s), not reported as new.
    """
    db = FixtureDb()
    pr_side = job.resolve_run(db, job.PR_DAYS, _PR, _PR_SHA)
    section = job.compare_compile_times(db, pr_side, [_BASE_SHA])
    assert "foo" in section.body
    # The baseline entity time is present, so it is a change, not a "new" entity.
    assert "5.0 s" in section.body


def test_expensive_new_tu_without_baseline_is_significant():
    """A large new TU with no master baseline still drives the top-level verdict.

    ``new_tu.cpp`` (40 s) has no baseline to diff against; a missing baseline
    must not silence the section - it is a real, significant compile-time cost.
    """
    db = FixtureDb()
    pr_side = job.resolve_run(db, job.PR_DAYS, _PR, _PR_SHA)
    section = job.compare_compile_times(db, pr_side, [_BASE_SHA])
    assert "new_tu.cpp" in section.body
    assert section.significant
    assert "without a master baseline" in (section.summary or "")


def test_new_tu_report_threshold_matches_the_significance_threshold():
    """A 25 s new TU is reported and significant; a 7 s one is only reported.

    The no-baseline path used to list translation units from a hard-wired 30 s
    while judging significance from ``TU_SIG_SECONDS`` (20 s), so a new TU in
    the 20-30 s range appeared in neither the body, the summary nor the verdict.
    Both thresholds of the section now apply to it: ``TU_REPORT_SECONDS`` for
    the listing, ``TU_SIG_SECONDS`` for the verdict.
    """
    db = FixtureDb()
    pr_side = job.resolve_run(db, job.PR_DAYS, _PR, "prsha-midsize-new-tu")
    assert pr_side is not None
    section = job.compare_compile_times(db, pr_side, [_BASE_SHA])
    assert "mid_tu.cpp" in section.body
    assert "small_tu.cpp" in section.body
    assert section.significant
    # Only the TU above TU_SIG_SECONDS is counted as a significant new cost.
    assert "1 new translation units without a master baseline" in (section.summary or "")


def test_opt_functions_cover_the_keeper_binary():
    """A keeper-only ThinLTO regression shows up, keyed by (binary, function).

    The section used to hard-wire ``programs/clickhouse``, so a PR could
    regress ``clickhouse-keeper``'s ThinLTO time without ever appearing in
    the report. Both final binaries must be diffed, and the row must be
    attributed to the binary it comes from.
    """
    db = FixtureDb()
    pr_side = job.resolve_run(db, job.PR_DAYS, _PR, _PR_SHA)
    base_side = job.resolve_run(db, job.BASE_DAYS, 0, _BASE_SHA)
    section = job.compare_opt_functions(db, pr_side, base_side)
    # keeper_fn regressed 5 s -> 30 s: reported, significant, and attributed
    # to clickhouse-keeper.
    assert "keeper_fn" in section.body
    assert f"`{job.strip_build_dir(_KEEPER)}`" in section.body
    assert section.significant
    # main_fn is unchanged: below the report threshold.
    assert "main_fn" not in section.body


def test_opt_functions_missing_baseline_is_called_out():
    """A PR-side link trace without a master baseline is noted, not dropped.

    With no baseline rows at all, no binary is comparable: the section must
    say so explicitly instead of rendering an empty (all-green) body while
    the advertised ThinLTO comparison silently did not happen.
    """
    db = FixtureDb()
    pr_side = job.resolve_run(db, job.PR_DAYS, _PR, _PR_SHA)
    no_data_base = job.Side(job.BASE_DAYS, 0, "sha-without-any-data", "2026-06-30 00:00:00", "I0")
    section = job.compare_opt_functions(db, pr_side, no_data_base)
    assert "No master baseline link trace" in section.body
    assert f"`{job.strip_build_dir(_MAIN)}`" in section.body
    assert f"`{job.strip_build_dir(_KEEPER)}`" in section.body
    assert not section.significant


def test_compile_times_use_warmup_traces_older_than_the_object_baseline():
    """An 8-14 day old warmup trace still yields a real compile-time comparison.

    The object-size baseline (``find_warmup_baseline``) looks back only
    BASE_DAYS = 7 days, but the per-TU comparison advertises TU_BASE_DAYS = 14.
    Gating the compile-time section on the 7-day baseline would replace a
    perfectly usable comparison with the catch-up note whenever the newest
    warmup data is 8-14 days old. ``oldwarmupsha``'s only warmup data is 10
    days old: no object baseline, yet ``tu.cpp`` (50 s vs 10 s) must be
    compared, not suppressed.
    """
    db = FixtureDb()
    pr_side = job.resolve_run(db, job.PR_DAYS, _PR, _PR_SHA)
    assert job.find_warmup_baseline(db, [_OLD_WARMUP_SHA], _PR_SHA) is None
    section = job.compare_compile_times(db, pr_side, [_OLD_WARMUP_SHA])
    assert section.body != job.WARMUP_CATCHUP_NOTE
    assert "tu.cpp" in section.body
    assert "10.0 s" in section.body
    assert section.significant


def test_opt_functions_missing_pr_side_binary_is_flagged():
    """A binary whose PR-side link trace was lost is an incomplete comparison.

    On ``arm_release`` the keeper is built standalone, so both sides link it:
    a master baseline with keeper OptFunction rows while the PR run has none
    means the PR-side producer lost them. Silently omitting the binary would
    make a keeper-only ThinLTO regression false-green; the section must call
    the loss out and refuse to render as all-green.
    """
    db = FixtureDb()
    pr_side = job.Side(job.PR_DAYS, _PR, "prsha-lost-keeper", "2026-07-02 00:00:00", "I5")
    base_side = job.resolve_run(db, job.BASE_DAYS, 0, _BASE_SHA)
    section = job.compare_opt_functions(db, pr_side, base_side)
    assert f"`{job.strip_build_dir(_KEEPER)}`" in section.body
    assert "uploaded none" in section.body
    assert section.significant
    assert "missing PR-side link trace" in section.summary


def test_objects_added_and_removed_are_significant():
    """A one-sided object row above the significance bar drives the verdict.

    OBJECT_FILTER keeps only compile-stage .o files and both sides compile the
    full object set with the same flags, so an object present on one side only
    is a real addition or removal. Gating significance on ``pr_size and
    base_size`` let a PR add or drop a multi-MiB object while the top-level
    comment still said "No significant changes".
    """
    db = FixtureDb()
    pr_side = job.Side(job.PR_DAYS, _PR, "prsha-new-object", "2026-07-02 00:00:00", "I8")
    warmup_side = job.resolve_run(db, job.BASE_DAYS, 0, _BASE_SHA, check_name=job.WARMUP_CHECK_NAME)
    section = job.compare_objects(db, pr_side, warmup_side)
    # bar.cpp.o (512 KiB) is added, foo.cpp.o (256 KiB) is removed: both
    # one-sided and both above OBJECT_SIG_BYTES.
    assert "bar.cpp.o" in section.body
    assert "new" in section.body
    assert "foo.cpp.o" in section.body
    assert "removed" in section.body
    assert section.significant


def test_opt_functions_trace_below_report_cutoff_is_not_a_lost_upload():
    """An intact link trace entirely below 50 ms is present, not missing.

    Per-binary presence used to be derived from rows already filtered by the
    ``dur >= 50000`` reporting cutoff, so a binary whose complete OptFunction
    trace stays below 50 ms on the PR side (realistic for
    ``clickhouse-keeper``) was indistinguishable from a lost upload and the
    section reported "uploaded none". Presence must come from unfiltered
    OptFunction rows; the cutoff only bounds the diff aggregation.
    """
    db = FixtureDb()
    pr_side = job.Side(job.PR_DAYS, _PR, "prsha-tiny-keeper", "2026-07-02 00:00:00", "I9")
    base_side = job.resolve_run(db, job.BASE_DAYS, 0, _BASE_SHA)
    section = job.compare_opt_functions(db, pr_side, base_side)
    assert "uploaded none" not in section.body
    assert not section.significant
    assert not section.summary


def test_compile_times_key_baselines_by_file_and_library():
    """A source compiled into two targets stays two distinct compile jobs.

    ``programs/keeper/Keeper.cpp`` is compiled into both the standalone
    ``clickhouse-keeper`` (10 s -> 60 s, a real regression) and
    ``clickhouse-keeper-lib`` (12 s, unchanged). Keyed by file alone the two
    jobs merge into one pseudo-TU - the PR's standalone compile gets compared
    against whichever baseline wins the aggregation, and the unchanged library
    compile can mask or garble the regression. Keyed by (file, library), the
    standalone regression is reported (labelled with its library, since the
    file name alone is ambiguous) and the library compile stays quiet.
    """
    db = FixtureDb()
    pr_side = job.resolve_run(db, job.PR_DAYS, _PR, _PR_SHA)
    section = job.compare_compile_times(db, pr_side, [_BASE_SHA])
    # The standalone compile regressed 10 s -> 60 s: reported and labelled.
    assert "`programs/keeper/Keeper.cpp (clickhouse-keeper)`" in section.body
    assert "| 10.0 s | 60.0 s |" in section.body
    assert section.significant
    # The unchanged library compile is not reported - neither as a finding nor
    # as a "new TU" (it has a baseline under its own (file, library) key).
    assert "(clickhouse-keeper-lib)" not in section.body


def test_extend_master_shas_pages_past_the_anchored_chain():
    """The per-TU candidate set is extended beyond the ~100-commit chain.

    ``master_track_commits_sha`` spans only a day or two of master, while the
    per-TU compile baseline advertises TU_BASE_DAYS: without extension a
    warmup trace inside that window but older than the chain's tail is
    invisible. Paging is anchored at the chain's oldest commit and stops once
    the fetched history is older than the window.
    """
    import datetime

    recent = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    pages = {
        1: [{"sha": f"c{i}", "date": recent} for i in range(100)],
        # The second page crosses the TU_BASE_DAYS cutoff: paging must stop.
        2: [{"sha": f"d{i}", "date": recent} for i in range(99)]
        + [{"sha": "ancient", "date": "2000-01-01T00:00:00Z"}],
        3: [{"sha": "never-fetched", "date": "2000-01-01T00:00:00Z"}],
    }
    calls = []

    def fake_list_page(anchor, page):
        calls.append((anchor, page))
        return pages[page]

    shas = job.extend_master_shas(["a", "b", "c0"], list_page=fake_list_page)
    # Anchored at the chain's oldest commit; the chain itself stays in front.
    assert calls[0] == ("c0", 1)
    assert shas[:3] == ["a", "b", "c0"]
    # Deduplicated (the anchor reappears on page 1) and stopped at the cutoff.
    assert shas.count("c0") == 1
    assert "d42" in shas and "ancient" in shas
    assert [page for _, page in calls] == [1, 2]

    # Fail-close: a GitHub API failure must propagate and fail the (allow_failure)
    # job, not silently degrade to the un-extended chain - the shallow chain hides
    # valid 8-14 day warmup baselines behind a false-green comparison.
    def failing_list_page(anchor, page):
        raise RuntimeError("api down")

    with pytest.raises(RuntimeError, match="api down"):
        job.extend_master_shas(["a"], list_page=failing_list_page)
    # An empty chain never reaches the API: nothing to anchor the extension on.
    assert job.extend_master_shas([], list_page=failing_list_page) == []


def test_seed_master_shas_anchors_the_local_chain_on_the_baseline():
    """A local run seeds the master chain from ``--base-sha``.

    ``LocalInfo`` carries no ``master_track_commits_sha`` kv metadata, so
    without seeding the warmup and per-TU baseline lookups see an empty
    candidate set and ``compare_compile_times`` builds ``commit_sha IN ()``.
    The seed is the first page of the baseline's ancestors - anchored, like
    the CI chain, so it never contains commits the PR does not have.
    """
    calls = []

    def fake_list_page(anchor, page):
        calls.append((anchor, page))
        return [{"sha": "basesha", "date": "2026-01-01T00:00:00Z"}] + [
            {"sha": f"c{i}", "date": "2026-01-01T00:00:00Z"} for i in range(99)
        ]

    shas = job.seed_master_shas("basesha", list_page=fake_list_page)
    assert calls == [("basesha", 1)]
    # The baseline itself leads the chain, so `find_warmup_baseline` and the
    # per-TU baseline consider it and everything behind it.
    assert shas[0] == "basesha"
    assert len(shas) == 100


def test_comment_attributes_only_object_sizes_to_the_warmup_sha():
    """The header must not pin compile times to the object-size warmup commit.

    ``warmup_sha`` names the object-size baseline only; compile times are
    resolved per translation unit against the most recent warmup build that
    recompiled it, which can be a different (even older) commit.
    """
    body = job.build_comment(
        job.LocalInfo(),
        "prsha",
        "basesha",
        [job.Section(title="t", body="b")],
        warmup_sha="warmupsha",
    )
    assert "object sizes against the warmup build of" in body
    assert "compile times per translation unit against the most recent warmup build" in body
    assert "object sizes and compile times against" not in body


def test_get_master_shas_uses_only_the_anchored_track():
    """Only ``master_track_commits_sha`` (the PR's master parent chain) is used.

    ``master_commits`` is the global master tip, not anchored to this PR's
    merge base, so a commit from it can be newer than the master the PR was
    built on - comparing against it attributes unrelated master changes to the
    PR. A missing anchored chain must therefore come back empty (the caller
    fails the job) rather than fall back to the tip.
    """

    class FakeInfo:
        def get_kv_data(self, key):
            return {
                "master_track_commits_sha": ["a", "b", "c"],
                "master_commits": ["x", "y"],
            }.get(key)

    assert job.get_master_shas(FakeInfo()) == ["a", "b", "c"]

    class TipOnlyInfo:
        def get_kv_data(self, key):
            return {"master_commits": ["x", "y"]}.get(key)

    assert job.get_master_shas(TipOnlyInfo()) == []


def test_uniform_compile_time_slowdown_is_significant():
    """A change that slows every translation unit down is caught section-wide.

    Per-TU deltas are measured against the median PR/baseline ratio, which
    estimates the machine-speed difference between the two runs. When every
    matched TU moves by the same factor, that factor *is* the median: each
    delta becomes zero, no per-TU finding survives, and the section would
    report "no significant changes" while total compile time doubled.
    """
    db = FixtureDb()
    pr_side = job.resolve_run(db, job.PR_DAYS, _PR, "prsha-uniform-slowdown")
    assert pr_side is not None
    section = job.compare_compile_times(db, pr_side, [_BASE_SHA])
    # 12 TUs, 30 s -> 65 s each: x2.17 and +420 s in total.
    assert section.significant
    assert "slower" in section.summary
    assert "+420.0 s" in section.body
    # No per-TU finding: relative to the median every TU is unchanged.
    assert "| Translation unit |" not in section.body


def test_machine_speed_skew_alone_is_not_significant():
    """A few percent of runner difference stays an informational note."""
    db = FixtureDb()
    pr_side = job.resolve_run(db, job.PR_DAYS, _PR, "prsha-machine-skew")
    assert pr_side is not None
    section = job.compare_compile_times(db, pr_side, [_BASE_SHA])
    # 12 TUs, 30 s -> 33 s each: x1.1, below TU_SKEW_SIG_RATIO.
    assert not section.significant
    assert not section.summary
    assert "×1.10" in section.body


def test_skew_is_the_true_median_on_an_even_translation_unit_count():
    """A regression in half of the translation units survives normalization.

    The per-TU deltas are relative to the median PR/baseline ratio. With an
    even number of matched TUs the upper middle element is not the median: for
    six unchanged and six doubled TUs it is the doubled ratio itself, which
    normalizes the whole regressed half to zero and turns the unchanged half
    into an equally large fake speedup.
    """
    db = FixtureDb()
    pr_side = job.resolve_run(db, job.PR_DAYS, _PR, "prsha-half-slowdown")
    assert pr_side is not None
    section = job.compare_compile_times(db, pr_side, [_BASE_SHA])
    # Ratios are six x1 and six x2: the median is x1.5, not x2.
    assert "×1.50" in section.body
    # Baseline 30 s adjusted to 45 s: the doubled TUs are +15 s over it.
    assert "| `skew_tu1.cpp` | 30.0 s | 60.0 s | +15.0 s" in section.body
    assert "| `skew_tu0.cpp` | 30.0 s | 30.0 s | -15.0 s" in section.body
    # 6 x 30 s of real regression stays below the section-wide bar.
    assert not section.significant
