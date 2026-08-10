import concurrent.futures
import threading
import time
import uuid

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

# Keeper fault injection would retry an insert and change the block numbers the part names
# are derived from. Merges are suppressed separately, by SYSTEM STOP MERGES in create_tables.
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
        # Every successful replicated insert schedules merge selection, and the tests assert
        # exact part names and counts, so keep merges off for the life of the table. The tables
        # are dropped at the end of each test, so there is no SYSTEM START MERGES.
        node.query(f"SYSTEM STOP MERGES {name}")


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


def run_fetch_in_background(node, query, results, query_id=None):
    def run():
        _, error = node.query_and_get_answer_with_error(query, query_id=query_id)
        results.append(error)

    thread = threading.Thread(target=run)
    thread.start()
    return thread


def assert_query_stays_blocked(node, query_id, results, seconds, message, timeout=30):
    """Positive signal that `query_id` is stuck rather than merely unscheduled: wait for it to
    appear in system.processes, then require it to still be there `seconds` later. `results` is
    the list the query's own thread appends to, so a query that already returned is reported as
    `message` instead of as a missing process."""
    deadline = time.monotonic() + timeout
    while True:
        assert not results, message
        running = node.query(
            f"SELECT count() FROM system.processes WHERE query_id = '{query_id}'"
        ).strip()
        if running == "1":
            break
        assert time.monotonic() < deadline, f"query {query_id} never started"
        time.sleep(0.2)

    until = time.monotonic() + seconds
    while time.monotonic() < until:
        time.sleep(0.2)
        assert not results, message
        running = node.query(
            f"SELECT count() FROM system.processes WHERE query_id = '{query_id}'"
        ).strip()
        assert running == "1", f"query {query_id} stopped running before it was released"


def parts_lock_wait_us(node, query_id):
    """Microseconds THIS query spent waiting for the data parts lock. Per-query, unlike
    system.events, which aggregates every parts-lock wait in the server."""
    node.query("SYSTEM FLUSH LOGS query_log")
    value = node.query(
        "SELECT ProfileEvents['PartsLockWaitMicroseconds'] FROM system.query_log"
        f" WHERE query_id = '{query_id}' AND type != 'QueryStart'"
        " ORDER BY event_time_microseconds DESC LIMIT 1"
    ).strip()
    assert value != "", f"no query_log row with profile events for {query_id}"
    return int(value)


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
    fetch_qid = f"fetch_{uuid.uuid4()}"
    try:
        fetch = run_fetch_in_background(
            dst,
            f"ALTER TABLE {name} FETCH PARTITION tuple() FROM '/clickhouse/tables/{name}_src'",
            fetch_results,
            query_id=fetch_qid,
        )
        wait_for_pause(dst, FETCH_PAUSE)

        # DETACH now enters its clone, takes lockParts and parks inside the locked region.
        detach = run_fetch_in_background(
            dst, f"ALTER TABLE {name} DETACH PART '{part}'", detach_results
        )
        wait_for_pause(dst, DETACH_PAUSE)

        # Release the fetch: it must now block acquiring lockParts, which DETACH holds
        # across its clone. While DETACH is still paused the clone at the canonical name is
        # partial/empty, so a fetch that ignored the lock would replace it right here.
        dst.query(f"SYSTEM NOTIFY FAILPOINT {FETCH_PAUSE}")

        assert_query_stays_blocked(
            dst,
            fetch_qid,
            fetch_results,
            seconds=3,
            message="the fetch published while DETACH still held lockParts",
        )
        assert not detach_results, "DETACH finished before the fetch was even released"

        dst.query(f"SYSTEM NOTIFY FAILPOINT {DETACH_PAUSE}")
    finally:
        dst.query(f"SYSTEM DISABLE FAILPOINT {DETACH_PAUSE}")
        dst.query(f"SYSTEM DISABLE FAILPOINT {FETCH_PAUSE}")

    detach.join()
    fetch.join()

    # The fetch was blocked ON THE PARTS LOCK specifically: this counter is read from the
    # fetch query's own query_log row, so no other parts-lock wait in the server can supply it.
    waited_us = parts_lock_wait_us(dst, fetch_qid)
    assert waited_us > 0, (
        f"the fetch never waited for the parts lock (PartsLockWaitMicroseconds = {waited_us})"
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


def test_fetch_partition_retry_deduplicates_covering_parts(started_cluster):
    """A retry round where two missing names resolve to the SAME covering part.

    Both parts of the partition fail to publish in round 1 (their canonical names got taken
    while the fetch was parked), so both land in missing_parts. Meanwhile the source merged
    them, so in round 2 `getContainingPart` maps both names onto one covering part. Without
    deduplication that part is enqueued twice, the second attempt collides with the first
    attempt's own output, and the statement never converges.
    """
    name = "t_dedup"
    create_tables(name)

    for i in range(2):
        src.query(f"INSERT INTO {name} VALUES ({i}, 'v{i}')", settings=INSERT_SETTINGS)
        dst.query(f"INSERT INTO {name} VALUES ({i}, 'v{i}')", settings=INSERT_SETTINGS)
    parts = src.query(
        f"SELECT name FROM system.parts WHERE database = currentDatabase()"
        f" AND table = '{name}' AND active ORDER BY name"
    ).strip().split("\n")
    assert len(parts) == 2, parts
    assert parts == dst.query(
        f"SELECT name FROM system.parts WHERE database = currentDatabase()"
        f" AND table = '{name}' AND active ORDER BY name"
    ).strip().split("\n"), "both tables must hold the same part names"

    dst.query(f"SYSTEM ENABLE FAILPOINT {FETCH_PAUSE}")
    results = []
    fetch = run_fetch_in_background(
        dst,
        f"ALTER TABLE {name} FETCH PARTITION tuple() FROM '/clickhouse/tables/{name}_src'",
        results,
    )
    try:
        # The failpoint stays enabled until the setup below is complete, so every publishing
        # thread of round 1 parks - waiting for the first one is enough to know the statement
        # is past its detached-partition pre-check.
        wait_for_pause(dst, FETCH_PAUSE)

        # Take both canonical names, so both parts of round 1 fail and are retried.
        for part in parts:
            dst.query(f"ALTER TABLE {name} DETACH PART '{part}'")
        assert detached_rows(dst, name, "name", skip_staging=True) == "\n".join(parts)

        # Merge the source, so the two retried names now resolve to one covering part. The
        # retry builds its ActiveDataPartSet from the source replica's ZooKeeper `parts` list,
        # and that set resolves covering parts on its own, so it is enough for the covering
        # znode to be there - the covered ones may still linger.
        src.query(f"SYSTEM START MERGES {name}")
        src.query(f"OPTIMIZE TABLE {name} FINAL")
        zk_parts_path = f"/clickhouse/tables/{name}_src/replicas/r1/parts"
        covering = None
        for _ in range(300):
            znodes = src.query(
                f"SELECT name FROM system.zookeeper WHERE path = '{zk_parts_path}' ORDER BY name"
            ).strip().split("\n")
            new_names = [n for n in znodes if n and n not in parts]
            if len(new_names) == 1:
                covering = new_names[0]
                break
            time.sleep(0.2)
        assert covering is not None, (
            f"source did not produce exactly one covering part: {znodes}"
        )
    finally:
        dst.query(f"SYSTEM DISABLE FAILPOINT {FETCH_PAUSE}")
    fetch.join()

    # With deduplication the covering part is enqueued once, is fetched once, and the
    # statement converges. Without it the duplicate attempt collides with the first one's
    # output and every remaining round fails the same way, up to the retry limit.
    assert len(results) == 1
    assert results[0] == "", results[0]

    # Exactly the two names DETACH created, plus the covering part fetched once.
    assert detached_rows(dst, name, "name") == "\n".join(sorted(parts + [covering]))
    assert "tmp-fetch" not in detached_rows(dst, name, "name")
    assert "ignored_" not in detached_rows(dst, name, "name")

    dst.query(f"ALTER TABLE {name} ATTACH PART '{covering}'")
    assert dst.query(f"SELECT count() FROM {name}").strip() == "2"

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
