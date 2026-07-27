import concurrent.futures
import threading

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# `src` holds the parts to fetch, `dst` is where FETCH publishes into detached/.
src = cluster.add_instance("src", with_zookeeper=True, stay_alive=True)
dst = cluster.add_instance(
    "dst",
    main_configs=["configs/config.d/storage_configuration.xml"],
    with_zookeeper=True,
    stay_alive=True,
    tmpfs=[
        "/test_fetch_detached_jbod1:size=100M",
        "/test_fetch_detached_jbod2:size=100M",
    ],
)

FETCH_PAUSE = "rmt_fetch_pause_before_publish_to_detached"
DETACH_PAUSE = "merge_tree_pause_before_clone_to_detached"

# Deterministic part names, and no background merge that could rename them.
INSERT_SETTINGS = {"insert_keeper_fault_injection_probability": "0"}


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_tables(name, dst_settings=""):
    """Two INDEPENDENT tables (distinct ZooKeeper paths), so `dst` only ever receives
    parts of `src` through the explicit FETCH under test - not through replication."""
    for node, suffix, settings in ((src, "src", ""), (dst, "dst", dst_settings)):
        node.query(
            f"""
            DROP TABLE IF EXISTS {name} SYNC;
            CREATE TABLE {name} (k UInt64, v String)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/{name}_{suffix}', 'r1')
            ORDER BY k
            SETTINGS old_parts_lifetime = 100000{settings}
        """
        )


def wait_for_pause(node, failpoint, timeout=60):
    """Block until some thread paused at `failpoint`, without hanging forever."""
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = pool.submit(node.query, f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE")
    done, _ = concurrent.futures.wait([future], timeout=timeout)
    if not done:
        pool.shutdown(wait=False, cancel_futures=True)
        raise AssertionError(f"failpoint {failpoint} was not hit within {timeout}s")
    pool.shutdown(wait=False)
    future.result()


def detached_rows(node, name, columns="name, disk", skip_staging=False):
    """Rows of system.detached_parts for `name`.

    While a fetch is in flight its own staging directory legitimately lives inside
    detached/, so assertions taken mid-statement pass skip_staging=True.
    """
    staging = " AND name NOT LIKE 'tmp-fetch\\_%'" if skip_staging else ""
    return node.query(
        f"SELECT {columns} FROM system.detached_parts"
        f" WHERE database = currentDatabase() AND table = '{name}'{staging}"
        f" ORDER BY name, disk"
    ).strip()


def part_fingerprint(node, name, part):
    """Identity of a detached part: its size plus the data it yields when attached."""
    return node.query(
        f"SELECT bytes_on_disk FROM system.detached_parts WHERE database = currentDatabase()"
        f" AND table = '{name}' AND name = '{part}'"
    ).strip()


def run_fetch_in_background(node, query, results):
    def run():
        _, error = node.query_and_get_answer_with_error(query)
        results.append(error)

    thread = threading.Thread(target=run)
    thread.start()
    return thread


def test_fetch_does_not_clobber_finished_detached_part(started_cluster):
    """W2: detached/<part> is already there, complete, on our own disk."""
    name = "t_clobber"
    create_tables(name)

    src.query(f"INSERT INTO {name} VALUES (1, 'from_src')", settings=INSERT_SETTINGS)
    # A different value, so an overwrite is visible in the data and not only in the size.
    dst.query(
        f"INSERT INTO {name} VALUES (2, 'from_dst_detached')", settings=INSERT_SETTINGS
    )
    part = dst.query(f"SELECT name FROM system.parts WHERE database = currentDatabase()"
                     f" AND table = '{name}' AND active").strip()
    assert part == src.query(
        f"SELECT name FROM system.parts WHERE database = currentDatabase()"
        f" AND table = '{name}' AND active"
    ).strip(), "both replicas must hold the same part name for the conflict to exist"

    dst.query(f"SYSTEM ENABLE FAILPOINT {FETCH_PAUSE}")
    results = []
    fetch = run_fetch_in_background(
        dst,
        f"ALTER TABLE {name} FETCH PARTITION tuple() FROM '/clickhouse/tables/{name}_src'",
        results,
    )
    try:
        wait_for_pause(dst, FETCH_PAUSE)
        # The fetch is staged but has not published yet: the canonical name is still free,
        # so DETACH takes it (no `_tryN` suffix) and releases its own claim right after.
        dst.query(f"ALTER TABLE {name} DETACH PART '{part}'")
        assert detached_rows(dst, name, "name", skip_staging=True) == part
        detached_bytes = part_fingerprint(dst, name, part)
        assert detached_bytes != ""
    finally:
        dst.query(f"SYSTEM DISABLE FAILPOINT {FETCH_PAUSE}")
    fetch.join()

    # 1. the fetch must fail rather than replace the foreign directory
    assert len(results) == 1
    assert "TOO_MANY_RETRIES_TO_FETCH_PARTS" in results[0], results[0]

    # 2. the detached part created by DETACH is untouched
    assert detached_rows(dst, name, "name") == part
    assert part_fingerprint(dst, name, part) == detached_bytes

    # 3. no staging leftovers
    assert "tmp-fetch" not in detached_rows(dst, name, "name")
    assert "tmp_clone" not in detached_rows(dst, name, "name")

    # 4. the surviving part is the one DETACH produced, and it is still attachable
    dst.query(f"ALTER TABLE {name} ATTACH PART '{part}'")
    assert dst.query(f"SELECT k, v FROM {name} ORDER BY k").strip() == "2\tfrom_dst_detached"

    dst.query(f"DROP TABLE {name} SYNC")
    src.query(f"DROP TABLE {name} SYNC")


def test_fetch_waits_for_concurrent_detach(started_cluster):
    """W1: DETACH is mid-clone, holding lockParts. The fetch must WAIT, not clobber.

    lockParts blocks, it does not fail, so the observable is an ORDERING: while DETACH is
    paused inside its locked region the fetch cannot reach its rename, and only after
    DETACH completes does the fetch see the finished directory and fail.
    """
    name = "t_wait"
    create_tables(name)

    src.query(f"INSERT INTO {name} VALUES (1, 'from_src')", settings=INSERT_SETTINGS)
    dst.query(
        f"INSERT INTO {name} VALUES (2, 'from_dst_detached')", settings=INSERT_SETTINGS
    )
    part = dst.query(f"SELECT name FROM system.parts WHERE database = currentDatabase()"
                     f" AND table = '{name}' AND active").strip()

    # Let the fetch stage its download first, then park it right before the publish step.
    dst.query(f"SYSTEM ENABLE FAILPOINT {FETCH_PAUSE}")
    dst.query(f"SYSTEM ENABLE FAILPOINT {DETACH_PAUSE}")
    fetch_results = []
    detach_results = []
    try:
        fetch = run_fetch_in_background(
            dst,
            f"ALTER TABLE {name} FETCH PARTITION tuple() FROM '/clickhouse/tables/{name}_src'",
            fetch_results,
        )
        wait_for_pause(dst, FETCH_PAUSE)

        # DETACH now enters its clone, takes lockParts and parks inside the locked region.
        detach = run_fetch_in_background(
            dst, f"ALTER TABLE {name} DETACH PART '{part}'", detach_results
        )
        wait_for_pause(dst, DETACH_PAUSE)

        # Release the fetch: it must now block acquiring lockParts, which DETACH holds
        # across its clone. Sample the wait counter first so the block is measurable.
        locks_before = int(
            dst.query(
                "SELECT value FROM system.events WHERE event = 'PartsLockWaitMicroseconds'"
            ).strip()
            or 0
        )
        dst.query(f"SYSTEM NOTIFY FAILPOINT {FETCH_PAUSE}")

        # Give the fetch a chance to misbehave. While DETACH is still paused the clone is
        # partial/empty at the canonical name, so a fetch that ignored the lock would
        # replace it here.
        dst.query("SELECT sleep(3)")
        assert not fetch_results, "the fetch published while DETACH still held lockParts"

        dst.query(f"SYSTEM NOTIFY FAILPOINT {DETACH_PAUSE}")
    finally:
        dst.query(f"SYSTEM DISABLE FAILPOINT {DETACH_PAUSE}")
        dst.query(f"SYSTEM DISABLE FAILPOINT {FETCH_PAUSE}")

    detach.join()
    fetch.join()

    # The fetch really waited on the lock rather than racing past it.
    locks_after = int(
        dst.query(
            "SELECT value FROM system.events WHERE event = 'PartsLockWaitMicroseconds'"
        ).strip()
        or 0
    )
    assert locks_after > locks_before, (
        "the fetch never blocked on lockParts (wait counter did not move):"
        f" {locks_before} -> {locks_after}"
    )

    assert detach_results == [""], detach_results
    # Only after DETACH finished does the fetch resume, see the complete directory and fail.
    assert len(fetch_results) == 1
    assert "TOO_MANY_RETRIES_TO_FETCH_PARTS" in fetch_results[0], fetch_results[0]

    # DETACH's part survived intact and is attachable.
    assert detached_rows(dst, name, "name") == part
    dst.query(f"ALTER TABLE {name} ATTACH PART '{part}'")
    assert dst.query(f"SELECT k, v FROM {name} ORDER BY k").strip() == "2\tfrom_dst_detached"

    dst.query(f"DROP TABLE {name} SYNC")
    src.query(f"DROP TABLE {name} SYNC")


def test_fetch_partition_multi_part_still_works(started_cluster):
    """The ordinary path must stay green: a multi-part partition fetches cleanly."""
    name = "t_multi"
    create_tables(name)

    for i in range(4):
        src.query(f"INSERT INTO {name} VALUES ({i}, 'v{i}')", settings=INSERT_SETTINGS)
    parts = src.query(
        f"SELECT name FROM system.parts WHERE database = currentDatabase()"
        f" AND table = '{name}' AND active ORDER BY name"
    ).strip()
    assert len(parts.split("\n")) == 4

    dst.query(f"ALTER TABLE {name} FETCH PARTITION tuple() FROM '/clickhouse/tables/{name}_src'")
    assert detached_rows(dst, name, "name") == parts

    for part in parts.split("\n"):
        dst.query(f"ALTER TABLE {name} ATTACH PART '{part}'")
    assert dst.query(f"SELECT count() FROM {name}").strip() == "4"

    dst.query(f"DROP TABLE {name} SYNC")
    src.query(f"DROP TABLE {name} SYNC")


def test_fetch_does_not_duplicate_detached_part_across_disks(started_cluster):
    """W3: the detached copy sits on another disk of the same policy.

    `rename`'s own guard only looks at the fetch's disk, while the detached namespace is
    table-wide, so without the table-wide check the rename succeeds and two logical
    detached/<part> exist - one of which ATTACH then demotes to ignored_*.
    """
    name = "t_disks"
    create_tables(name, dst_settings=", storage_policy = 'two_disks'")

    src.query(f"INSERT INTO {name} VALUES (1, 'from_src')", settings=INSERT_SETTINGS)
    dst.query(
        f"INSERT INTO {name} VALUES (2, 'from_dst_detached')", settings=INSERT_SETTINGS
    )
    part = dst.query(f"SELECT name FROM system.parts WHERE database = currentDatabase()"
                     f" AND table = '{name}' AND active").strip()

    # Put the local part - and therefore its detached copy - on the second disk, so that
    # the fetch's own reservation (first volume) provably cannot see it.
    dst.query(f"ALTER TABLE {name} MOVE PART '{part}' TO DISK 'jbod2'")
    assert dst.query(
        f"SELECT disk_name FROM system.parts WHERE database = currentDatabase()"
        f" AND table = '{name}' AND active"
    ).strip() == "jbod2"

    # The conflict has to appear AFTER the statement's own detached-partition pre-check,
    # which is table-wide and would otherwise reject the FETCH before the publish step.
    dst.query(f"SYSTEM ENABLE FAILPOINT {FETCH_PAUSE}")
    results = []
    fetch = run_fetch_in_background(
        dst,
        f"ALTER TABLE {name} FETCH PARTITION tuple() FROM '/clickhouse/tables/{name}_src'",
        results,
    )
    try:
        wait_for_pause(dst, FETCH_PAUSE)
        dst.query(f"ALTER TABLE {name} DETACH PART '{part}'")
        assert detached_rows(dst, name, "name, disk", skip_staging=True) == f"{part}\tjbod2"
        detached_bytes = part_fingerprint(dst, name, part)
        # The two sides really are on different disks - otherwise this degenerates into
        # the single-disk case and the table-wide check is not what rejects the fetch.
        staging_disk = dst.query(
            f"SELECT disk FROM system.detached_parts WHERE database = currentDatabase()"
            f" AND table = '{name}' AND name LIKE 'tmp-fetch\\_%'"
        ).strip()
        assert staging_disk == "jbod1", staging_disk
    finally:
        dst.query(f"SYSTEM DISABLE FAILPOINT {FETCH_PAUSE}")
    fetch.join()

    assert len(results) == 1
    assert "TOO_MANY_RETRIES_TO_FETCH_PARTS" in results[0], results[0]

    # No staging leftovers once the statement is over.
    assert "tmp-fetch" not in detached_rows(dst, name, "name")

    # Exactly one detached/<part> across all disks, and it is the pre-existing one.
    assert dst.query(
        f"SELECT count() FROM system.detached_parts WHERE database = currentDatabase()"
        f" AND table = '{name}' AND name = '{part}'"
    ).strip() == "1"
    assert detached_rows(dst, name, "name, disk") == f"{part}\tjbod2"
    assert part_fingerprint(dst, name, part) == detached_bytes

    dst.query(f"ALTER TABLE {name} ATTACH PART '{part}'")
    assert "ignored_" not in detached_rows(dst, name, "name")
    assert dst.query(f"SELECT k, v FROM {name} ORDER BY k").strip() == "2\tfrom_dst_detached"

    dst.query(f"DROP TABLE {name} SYNC")
    src.query(f"DROP TABLE {name} SYNC")
