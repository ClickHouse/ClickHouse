"""
Guards the duration table that `get_optimal_test_batch` packs integration test shards with.

Shards are bin-packed by estimated duration and then run under a pytest `--session-timeout`
of two hours. A shard handed materially more work than its siblings runs out of that budget
and the job fails with `xdist.dsession.Interrupted: session-timeout`, dropping whatever tests
had not started yet.

The estimate comes from `TEST_DURATIONS`, a table refreshed by hand from CIDB with the query
kept next to it. Only modules above the query's floor get an entry; everything else is packed
at weight 0 and merely round-robin distributed, so the packer is blind to it. That blind spot
is what broke the shard balance: with a 60000ms floor the table had grown stale enough to
model only ~69% of the wall-clock mass, and the unmodelled remainder landed unevenly enough
to push one shard past the session timeout while its siblings finished with 20 minutes to
spare. Coverage, not the packing algorithm, is the thing that has to be kept up.

See ClickHouse/ClickHouse#116596.
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.scripts.integration_tests_configs import (
    TEST_DURATIONS,
    get_optimal_test_batch,
)

TOTAL_BATCHES = 6
NUM_WORKERS = 3

# Share of test modules that must carry a duration estimate. The table covered ~27% of them
# when a shard first timed out and ~71% right after the refresh that fixed it, so this sits
# far enough below the refreshed value to absorb a year of new tests, and far enough above
# the broken one to fail before the packer goes blind again.
MIN_TABLE_COVERAGE = 0.55

REFRESH_HINT = (
    "Refresh RAW_TEST_DURATIONS in ci/jobs/scripts/integration_tests_configs.py by running "
    "the query kept above it on play.clickhouse.com."
)


def _all_test_files():
    """Every integration test module, discovered the way the job discovers them."""
    root = Path(os.path.join(os.path.dirname(__file__), "../../tests/integration"))
    return sorted(str(p.relative_to(root)) for p in root.glob("test_*/test*.py"))


def _pack(tests, total_batches=TOTAL_BATCHES, num_workers=NUM_WORKERS):
    """Return the (parallel, sequential) module lists for every batch."""
    return [
        get_optimal_test_batch(
            tests, total_batches, batch, num_workers, "amd_asan_ubsan, db disk"
        )
        for batch in range(1, total_batches + 1)
    ]


def test_duration_table_covers_most_test_modules():
    """
    The packer can only balance what it can see. A module missing from the table weighs
    nothing, so once enough of them accumulate the shards drift apart no matter how good
    the bin packing is.
    """
    tests = _all_test_files()
    assert len(tests) > 100

    modelled = [t for t in tests if t in TEST_DURATIONS]
    coverage = len(modelled) / len(tests)
    assert coverage >= MIN_TABLE_COVERAGE, (
        f"only {len(modelled)}/{len(tests)} ({coverage:.0%}) integration test modules have a "
        f"duration estimate, below the {MIN_TABLE_COVERAGE:.0%} floor. {REFRESH_HINT}"
    )


def test_shards_are_balanced():
    """No shard may be planned with a load that stands out from its siblings."""
    tests = _all_test_files()

    # Model a shard's wall-clock the way the job spends it: the parallel bucket is shared by
    # NUM_WORKERS xdist workers, the sequential bucket runs on one.
    walls = [
        sum(TEST_DURATIONS.get(t, 0) for t in par) / NUM_WORKERS
        + sum(TEST_DURATIONS.get(t, 0) for t in seq)
        for par, seq in _pack(tests)
    ]
    assert max(walls) <= 1.1 * (sum(walls) / len(walls)), [round(w) for w in walls]


def test_every_test_is_assigned_exactly_once():
    tests = _all_test_files()
    assigned = [t for par, seq in _pack(tests) for t in par + seq]
    assert sorted(assigned) == sorted(tests)


def test_packing_is_deterministic():
    """Batches are computed independently on each runner and must agree."""
    tests = _all_test_files()
    assert _pack(tests) == _pack(tests)
