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
model only ~67% of the wall-clock mass, and the unmodelled remainder landed unevenly enough to
push one shard past the session timeout while its siblings finished with 20 minutes to spare.

What has to be guarded is therefore runtime mass, not entry count: a handful of heavy modules
dropping out of the table barely moves the share of modules that carry an estimate, and moves
the table's own view of the shard weights not at all, because the packer scores the shards
with the very table under test. So the assertions below are scored against
`ci/tests/data/integration_test_durations_sample.tsv` instead - an unfloored CIDB sample of
every module's measured duration, checked in as a fixture and refreshed alongside the table.
It plays the part of the real suite: coverage is measured as the share of *its* mass the
packer can see, and balance as the spread of the shards' *measured* wall-clock under the
packing the table produces. `test_hidden_mass_unbalances_the_shards` keeps that scoring
honest by checking that dropping a heavy prefix out of the table does trip the balance
assertion.

See ClickHouse/ClickHouse#116591.
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.scripts import integration_tests_configs
from ci.jobs.scripts.integration_tests_configs import (
    TEST_DURATIONS,
    get_optimal_test_batch,
)

TOTAL_BATCHES = 6
NUM_WORKERS = 3

MEASURED_DURATIONS_PATH = (
    Path(__file__).parent / "data" / "integration_test_durations_sample.tsv"
)

# Share of the measured wall-clock mass the packer must be able to see. The table modelled
# 67% of it when a shard first timed out and 96% right after the refresh that fixed it, so
# this sits far enough below the refreshed value to absorb a year of new tests, and far
# enough above the broken one to fail before the packer goes blind again.
MIN_MASS_COVERAGE = 0.85

# How far the heaviest shard's measured wall-clock may stand out from the average. The stale
# table spread the shards to 1.08 of the average; the refreshed one holds them to 1.04.
MAX_SHARD_IMBALANCE = 1.06

# The sample only describes the modules that existed when it was taken. If the suite has
# moved on far enough that it no longer describes most of them, the two assertions above stop
# meaning anything and the fixture is what needs refreshing.
MIN_SAMPLE_FRESHNESS = 0.85

REFRESH_HINT = (
    "Refresh RAW_TEST_DURATIONS in ci/jobs/scripts/integration_tests_configs.py, and the "
    f"{MEASURED_DURATIONS_PATH.name} fixture next to this test, by running the query kept "
    "above RAW_TEST_DURATIONS on play.clickhouse.com (the fixture is the same query without "
    "its HAVING floor)."
)


def _all_test_files():
    """Every integration test module, discovered the way the job discovers them."""
    root = Path(os.path.join(os.path.dirname(__file__), "../../tests/integration"))
    return sorted(str(p.relative_to(root)) for p in root.glob("test_*/test*.py"))


def _measured_durations(tests):
    """The checked-in CIDB sample, restricted to modules that still exist."""
    known = set(tests)
    measured = {}
    for line in MEASURED_DURATIONS_PATH.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        path, duration = line.rsplit(maxsplit=1)
        if path in known:
            measured[path] = int(duration)
    assert measured, f"{MEASURED_DURATIONS_PATH} holds no usable rows"
    return measured


def _pack(tests, total_batches=TOTAL_BATCHES, num_workers=NUM_WORKERS):
    """Return the (parallel, sequential) module lists for every batch."""
    return [
        get_optimal_test_batch(
            tests, total_batches, batch, num_workers, "amd_asan_ubsan, db disk"
        )
        for batch in range(1, total_batches + 1)
    ]


def _shard_walls(packing, durations, num_workers=NUM_WORKERS):
    """
    Model each shard's wall-clock the way the job spends it: the parallel bucket is shared by
    num_workers xdist workers, the sequential bucket runs on one.
    """
    return [
        sum(durations.get(t, 0) for t in par) / num_workers
        + sum(durations.get(t, 0) for t in seq)
        for par, seq in packing
    ]


def _imbalance(walls):
    return max(walls) / (sum(walls) / len(walls))


def test_sample_still_describes_the_suite():
    tests = _all_test_files()
    assert len(tests) > 100

    measured = _measured_durations(tests)
    freshness = len(measured) / len(tests)
    assert freshness >= MIN_SAMPLE_FRESHNESS, (
        f"the measured-duration sample covers only {len(measured)}/{len(tests)} "
        f"({freshness:.0%}) of the integration test modules. {REFRESH_HINT}"
    )


def test_duration_table_covers_most_measured_mass():
    """
    The packer can only balance what it can see. A module missing from the table weighs
    nothing, so once enough wall-clock hides behind such modules the shards drift apart no
    matter how good the bin packing is.
    """
    tests = _all_test_files()
    measured = _measured_durations(tests)

    total = sum(measured.values())
    modelled = sum(v for k, v in measured.items() if k in TEST_DURATIONS)
    coverage = modelled / total
    assert coverage >= MIN_MASS_COVERAGE, (
        f"the duration table models only {coverage:.0%} of the measured integration test "
        f"wall-clock, below the {MIN_MASS_COVERAGE:.0%} floor. {REFRESH_HINT}"
    )


def test_shards_are_balanced():
    """
    No shard may be planned with a measured load that stands out from its siblings. Scored
    against the sample, not against the table that produced the packing.
    """
    tests = _all_test_files()
    measured = _measured_durations(tests)

    walls = _shard_walls(_pack(tests), measured)
    assert _imbalance(walls) <= MAX_SHARD_IMBALANCE, [round(w) for w in walls]


def test_hidden_mass_unbalances_the_shards(monkeypatch):
    """
    The balance assertion is only worth anything if hiding runtime mass from the packer trips
    it. Drop the heaviest prefix out of the table - the exact way the table went stale - and
    the shards must come out visibly uneven when scored against their measured durations.
    """
    tests = _all_test_files()
    measured = _measured_durations(tests)

    heaviest_prefix = max(
        {t.split("/", 1)[0] for t in measured},
        key=lambda prefix: sum(
            v for k, v in measured.items() if k.split("/", 1)[0] == prefix
        ),
    )
    blinded = {
        k: v for k, v in TEST_DURATIONS.items() if k.split("/", 1)[0] != heaviest_prefix
    }
    assert len(blinded) < len(TEST_DURATIONS)
    monkeypatch.setattr(integration_tests_configs, "TEST_DURATIONS", blinded)

    walls = _shard_walls(_pack(tests), measured)
    assert _imbalance(walls) > MAX_SHARD_IMBALANCE, [round(w) for w in walls]


def test_every_test_is_assigned_exactly_once():
    tests = _all_test_files()
    assigned = [t for par, seq in _pack(tests) for t in par + seq]
    assert sorted(assigned) == sorted(tests)


def test_packing_is_deterministic():
    """Batches are computed independently on each runner and must agree."""
    tests = _all_test_files()
    assert _pack(tests) == _pack(tests)
