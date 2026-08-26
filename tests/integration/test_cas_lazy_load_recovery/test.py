import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

STORAGE_POLICY = "cas_shared"


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    cluster.add_instance(
        "node1",
        main_configs=["configs/storage_conf.xml", "configs/server_root_id_node1.xml"],
        macros={"replica": "node1"},
        with_rustfs=True,
        with_zookeeper=True,
        stay_alive=True,
    )
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _create(node):
    # lazy_load_tables=1: the CAS table attaches as a proxy and its real storage is built on first
    # access. A transient object-store outage during that build is ridden out / retried on a later
    # access instead of being cached as a permanently-FAILED AsyncLoader job (which, for a non-lazy
    # database, would strand the table until a full server restart).
    node.query("CREATE DATABASE IF NOT EXISTS lazy_db ENGINE = Atomic SETTINGS lazy_load_tables = 1")
    node.query(
        "CREATE TABLE IF NOT EXISTS lazy_db.t (k UInt64, v UInt64) "
        "ENGINE = ReplicatedMergeTree('/clickhouse/tables/lazy_t', '{replica}') "
        "ORDER BY k SETTINGS storage_policy = '%s', min_bytes_for_wide_part = 0" % STORAGE_POLICY
    )


def test_lazy_cas_table_self_heals_after_s3_recovery(start_cluster):
    node = cluster.instances["node1"]
    _create(node)
    node.query("INSERT INTO lazy_db.t SELECT number, number FROM numbers(100)")
    assert node.query("SELECT count() FROM lazy_db.t").strip() == "100"

    # Restart so the table re-attaches as a lazy proxy (its real storage is not yet constructed; the
    # disk mounts at startup while S3 is up, the storage is built only on first access below).
    node.restart_clickhouse()

    # Touch the table while S3 is unreachable: the lazy first-access build (its CAS ref-recovery LIST
    # over the object store) cannot complete, so the client query fails within its bounded timeout.
    # Note: the build does NOT fail fast server-side -- it blocks on the object store's own retry until
    # S3 returns (see the BACKLOG "block-until-recovered" note); the client-side timeout is what makes
    # this probe short. We assert the probe DID hit the outage (raised): the build needs several object-
    # store round-trips, so the freezer (effective within milliseconds of `pause_container` returning)
    # reliably catches it -- if this ever flakes, the pause raced a sub-millisecond full build, not a
    # real self-heal regression.
    with cluster.pause_container("rustfs1", wait_for_paused=False):
        probe_raised = False
        try:
            node.query("SELECT count() FROM lazy_db.t", timeout=30)
        except Exception:
            probe_raised = True  # expected while the object store is unreachable
        assert probe_raised, "the probe should have failed while S3 was unreachable (did the pause race the build?)"

    # S3 is back (context exit unpaused rustfs). WITHOUT a server restart and WITHOUT any DETACH, a
    # later access must make the table usable again. This proves the key Layer 2 property: a transient
    # object-store outage during a lazy CAS table's first-access build leaves NO permanently-cached
    # AsyncLoader FAILED state (a non-lazy table whose load failed would stay FAILED until a full server
    # restart). What actually recovers here is the original in-flight build completing once S3 returns
    # (the block-until-recovered path), which is sufficient for "usable again without restart"; this
    # test does not (and, given block-until-recovered, cannot) assert a proxy retry of a THROWN build.
    deadline = time.time() + 180
    last = None
    while time.time() < deadline:
        try:
            last = node.query("SELECT count() FROM lazy_db.t").strip()
        except Exception as e:
            last = "err: " + str(e)
        if last == "100":
            break
        time.sleep(3)
    assert last == "100", (
        "lazy CAS table must become usable again on a later access after S3 returns, with no server "
        "restart (last=%r)" % last
    )
