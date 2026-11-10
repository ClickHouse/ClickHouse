"""The commit read-ahead must not be reset by its own timeout fallback.

`drainReader` serves a commit read directly when the fill misses the serve deadline. It used
to also reset the reader afterwards, which discarded everything the fill had decoded. On
storage where a read costs more than the deadline the fill can never win the first race, so
that reset made every following entry lose too: one fallback, one file read and two object
reads per entry, forever.

This test puts every commit read on the fallback path without needing slow storage, by setting
`log_readahead_serve_wait_timeout_ms` to 0, whose existing meaning is "do not wait for the fill at
all". That makes it deterministic on a local disk: it does not depend on the fill losing a timing
race -- on a page-cached local file it would not lose -- only on whether the fallback then destroys
the fill's progress. The assertions are ratios rather than timings: with the reset,
`KeeperLogsReadAheadTimeoutFallbacks` tracks the number of replayed entries one-for-one; without it,
one fallback is enough to hand the rest of the run to the read-ahead deque.

Shape:
  1. Write enough znodes to fill several log files, with a cache too small to hold any of them.
  2. Restart the node. `create_snapshot_on_exit` is off and `snapshot_distance` is far away, so
     the whole log is an unreplayed tail and the commit loop reads all of it from disk.
  3. Assert the node came back with its data intact.
  4. Assert the replay was served mostly from the read-ahead, not from per-entry fallbacks.
"""

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/enable_keeper1.xml"],
    stay_alive=True,
    with_zookeeper=False,
)

ZNODE_ROOT = "/commit_readahead_no_reset"
NUM_ZNODES = 4000
ZNODE_VALUE = b"x" * 200


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_zk(node):
    return keeper_utils.get_fake_zk(cluster, node.name, timeout=30.0)


def test_fallback_does_not_reset_the_commit_readahead(started_cluster):
    keeper_utils.wait_until_connected(cluster, node1)

    zk = get_zk(node1)
    try:
        zk.create(ZNODE_ROOT)
        for i in range(NUM_ZNODES):
            zk.create(f"{ZNODE_ROOT}/node_{i:06d}", ZNODE_VALUE)
    finally:
        zk.stop()
        zk.close()

    # The replay only exercises the commit read path if the entries are on disk, which the
    # 1-byte cache thresholds guarantee, and if there are several files to read from.
    log_files = node1.exec_in_container(
        ["bash", "-c", "ls /var/lib/clickhouse/coordination/log"]
    ).split()
    assert len(log_files) >= 3, f"Expected at least 3 log files, got {log_files}"

    def profile_event(name):
        return keeper_utils.get_profile_events(cluster, node1).get(name, 0)

    # --- Restart: everything written above is now an unreplayed tail ---
    node1.restart_clickhouse()
    keeper_utils.wait_until_connected(cluster, node1)

    # Correctness first: a fast replay that loses data is not the goal.
    zk = get_zk(node1)
    try:
        children = zk.get_children(ZNODE_ROOT)
        assert len(children) == NUM_ZNODES, (
            f"After the restart node1 has {len(children)} children, expected {NUM_ZNODES}"
        )
        for i in range(0, NUM_ZNODES, NUM_ZNODES // 20):
            data, _ = zk.get(f"{ZNODE_ROOT}/node_{i:06d}")
            assert data == ZNODE_VALUE, f"Wrong value at node_{i:06d}: {data!r}"
    finally:
        zk.stop()
        zk.close()

    # Counters are cumulative and the restart zeroed them, so these are the replay's own totals.
    from_readahead = profile_event("KeeperLogsEntryReadFromCommitReadAhead")
    from_file = profile_event("KeeperLogsEntryReadFromFile")
    fallbacks = profile_event("KeeperLogsReadAheadTimeoutFallbacks")
    fill_decoded = profile_event("KeeperLogsReadAheadFillDecodedEntries")
    replayed = from_readahead + from_file

    # Printed unconditionally: a pass is only meaningful if the replay really went through the
    # commit read path, and these are the numbers that say whether it did.
    print(
        f"replay counters: commits_readahead={from_readahead} from_file={from_file} "
        f"fallbacks={fallbacks} fill_decoded={fill_decoded} replayed={replayed}"
    )

    assert replayed >= NUM_ZNODES // 2, (
        "The replay did not go through the commit read path at all, so this test proves nothing. "
        f"from_readahead={from_readahead}, from_file={from_file}, expected at least "
        f"{NUM_ZNODES // 2} entries read outside the cache"
    )

    # Without this the ratio assertions below pass vacuously: a build where commit read-ahead is
    # inert serves every entry through serveCommitEntry's plain direct read, which records no
    # fallback at all, so `fallbacks` would be 0 against a large `replayed`.
    assert from_readahead > replayed // 2, (
        "The commit read-ahead served less than half the replay, so the ratios below would not be "
        f"testing it: from_readahead={from_readahead}, from_file={from_file}, replayed={replayed}"
    )

    # With the reset, fallbacks == from_file == replayed. Without it, one fallback hands the
    # rest of the run to the deque. A tenth leaves room for a few re-arms without admitting
    # the per-entry regime.
    assert fallbacks * 10 < replayed, (
        "The commit read-ahead fell back once per entry, which means the reader is being reset "
        f"by its own fallback: fallbacks={fallbacks}, replayed={replayed} "
        f"(from_readahead={from_readahead}, from_file={from_file})"
    )

    # Same conclusion from the other side: the fill's output has to be consumed, not discarded.
    assert fill_decoded < replayed * 10, (
        "The fill decoded far more entries than were served, so its work is being thrown away: "
        f"fill_decoded={fill_decoded}, replayed={replayed}"
    )
