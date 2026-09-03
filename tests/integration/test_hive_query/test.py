import logging
import os
import time

import pytest

from helpers.cluster import ClickHouseCluster, is_arm

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

# The Hive test cluster runs the `lgboustc/hive_test` image, which is amd64-only. On ARM runners it
# can only start under (very slow) QEMU emulation, so the Hadoop + Hive metastore + HiveServer2 JVM
# stack never becomes reachable within the test budget and the `started_cluster` fixture times out
# regardless of how generous the timeout is. Skip on ARM, matching the other tests that depend on
# amd64-only images (e.g. `test_mysql57_database_engine`).
if is_arm():
    pytestmark = pytest.mark.skip
else:
    # Even on amd64 the Hive cluster brings up a full Hadoop + Hive metastore + HiveServer2 JVM
    # stack, which on a loaded (cpus=3) CI runner can take longer than the default per-test timeout
    # of 900s to become reachable - the `started_cluster` fixture then exhausts the budget while
    # polling the metastore in `prepare_hive_data.sh` and the test fails before any assertion runs.
    # Give the whole module a more generous budget (matching other heavy external-service tests such
    # as `test_storage_rabbitmq` and `test_backup_restore_new`).
    pytestmark = pytest.mark.timeout(1800)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "h0_0_0",
            main_configs=["configs/config.xml"],
            extra_configs=["configs/hdfs-site.xml"],
            with_hive=True,
        )

        logging.info("Starting cluster ...")
        cluster.start()

        hive_container = cluster.get_instance_docker_id("hdfs1")
        cluster.copy_file_to_container(
            hive_container,
            os.path.join(SCRIPT_DIR, "data", "prepare_hive_data.sh"),
            "/prepare_hive_data.sh",
        )
        # The Hive metastore / HiveServer2 may still be starting up; retry the data
        # preparation (the script is idempotent) until the hive CLI can talk to it.
        last_error = None
        for _ in range(20):
            try:
                cluster.exec_in_container(hive_container, ["bash", "-c", "bash /prepare_hive_data.sh"])
                last_error = None
                break
            except Exception as e:  # noqa: BLE001
                last_error = e
                time.sleep(15)
        if last_error is not None:
            raise last_error
        yield cluster
    finally:
        cluster.shutdown()


def query_with_retry(node, query, retries=20, sleep=10):
    # The Hive metastore can take a while to accept connections after the cluster
    # starts; StorageHive connects lazily on the first read, so retry until it succeeds.
    last_error = None
    for _ in range(retries):
        try:
            return node.query(query)
        except Exception as e:  # noqa: BLE001
            last_error = e
            time.sleep(sleep)
    raise last_error


def test_select_without_where(started_cluster):
    # Regression test for https://github.com/ClickHouse/ClickHouse/issues/102636:
    # `SELECT * FROM hive_table` without a WHERE clause passed a null filter to
    # StorageHive, which dereferenced it and terminated the server with a
    # segmentation fault. A query with a WHERE clause happened to avoid the crash.
    node = started_cluster.instances["h0_0_0"]

    node.query("DROP TABLE IF EXISTS default.hive_demo")
    node.query(
        """
        CREATE TABLE default.hive_demo (`id` Nullable(String), `score` Nullable(Int32), `day` Nullable(String))
        ENGINE = Hive('thrift://hivetest:9083', 'test', 'demo') PARTITION BY(day)
        """
    )

    # No WHERE clause: this used to crash the server.
    assert query_with_retry(node, "SELECT count(*) FROM default.hive_demo").strip() == "4"
    assert node.query("SELECT id, score, day FROM default.hive_demo ORDER BY day, id") == "a\t1\t2021-11-01\nb\t2\t2021-11-05\nc\t3\t2021-11-05\nd\t4\t2021-11-11\n"

    # A read with a filter exercises partition (and split) pruning ...
    assert node.query("SELECT count(*) FROM default.hive_demo WHERE day = '2021-11-05'").strip() == "2"

    # ... and must not leave stale pruning state on the process-wide cached hive file:
    # a subsequent unfiltered read of the same table still returns all rows.
    assert node.query("SELECT count(*) FROM default.hive_demo").strip() == "4"
    assert node.query("SELECT count(*) FROM default.hive_demo WHERE day = '2021-11-01'").strip() == "1"
    assert node.query("SELECT count(*) FROM default.hive_demo").strip() == "4"


def test_orc_split_skip_state_is_query_local(started_cluster):
    # Regression test for the per-query split-skip set (ORC stripe pruning).
    #
    # The set of splits (Parquet row groups / ORC stripes) to skip while reading a file is
    # derived from the query filter, but it used to be stored on the `IHiveFile` objects served
    # from the process-wide `hive_files_cache`. Because those objects are shared between queries,
    # a filtered read could leave a stale split-skip set on a cached file, so a later unfiltered
    # read - or a concurrent query with a different filter - would silently drop the stripes
    # excluded by the earlier predicate. The fix carries the skip set per query instead.
    #
    # `test.demo_orc` has two ORC files in the same partition with disjoint `score` ranges
    # ([1, 2] and [10, 11]). The table enables stripe-level pruning (`enable_orc_stripe_minmax_index = 1`)
    # and disables file-level pruning (`enable_orc_file_minmax_index = 0`, which is on by default): with
    # file-level pruning on, a filter on `score` would discard a whole file before stripe pruning runs, so
    # no per-query split-skip set is ever built. With it off, a filter on the non-partition `score` column
    # prunes one file's stripe - a non-empty per-query split-skip set - while keeping the other. The split
    # filtering only matters here because the ORC reader honours `orc.skip_stripes`.
    node = started_cluster.instances["h0_0_0"]

    node.query("DROP TABLE IF EXISTS default.hive_demo_orc")
    node.query(
        """
        CREATE TABLE default.hive_demo_orc (`id` Nullable(String), `score` Nullable(Int32), `day` Nullable(String))
        ENGINE = Hive('thrift://hivetest:9083', 'test', 'demo_orc') PARTITION BY(day)
        SETTINGS enable_orc_stripe_minmax_index = 1, enable_orc_file_minmax_index = 0
        """
    )

    # Warm the file cache and establish the unfiltered baseline.
    assert query_with_retry(node, "SELECT count(*) FROM default.hive_demo_orc").strip() == "4"

    # A selective read on the non-partition `score` column prunes the [10, 11] file's stripe
    # (a non-empty per-query split-skip set), returning only the [1, 2] file's rows.
    assert node.query("SELECT count(*) FROM default.hive_demo_orc WHERE score < 5").strip() == "2"

    # The split-skip set must not persist on the cached file: a later unfiltered read still
    # returns all rows (before the fix this returned 2, because the stale skip set dropped the
    # second file's stripe).
    assert node.query("SELECT count(*) FROM default.hive_demo_orc").strip() == "4"

    # The opposite filter prunes the other file; an unfiltered read must again return all rows.
    assert node.query("SELECT count(*) FROM default.hive_demo_orc WHERE score > 5").strip() == "2"
    assert node.query("SELECT count(*) FROM default.hive_demo_orc").strip() == "4"
    assert node.query("SELECT id, score FROM default.hive_demo_orc ORDER BY score") == "a\t1\nb\t2\nc\t10\nd\t11\n"


def test_hive_files_cache_is_table_local(started_cluster):
    # Regression test for https://github.com/ClickHouse/ClickHouse/pull/108094#discussion_r3507709555.
    #
    # The `hive_files_cache` lives on the per-remote-table Hive metadata, so it is shared by every
    # ClickHouse `Hive` table over the same remote Hive table (it is keyed only by the remote file
    # path). A cached `IHiveFile` bakes in table-specific state: the minmax-index column layout
    # (`index_names_and_types`), the file format, the partition values, and the `enable_orc_*` minmax
    # settings. `Hive` explicitly allows different ClickHouse schemas/settings over one remote table,
    # so two such tables must not share one cached file - otherwise the second table would prune and
    # interpret the cached minmax index against the first table's layout and could silently keep or
    # skip the wrong data. The fix keys the cache by the remote path plus a signature of that
    # table-specific state, giving mismatched tables independent cache entries.
    #
    # Both tables below point at the same remote `test.demo_orc` (two ORC files, `score` ranges
    # [1, 2] and [10, 11]) but differ in column order and in the minmax settings, so with the fix they
    # occupy separate cache entries. We warm the shared cache through the first table with a selective
    # read (which builds and caches file objects, and loads the stripe minmax, under its own layout and
    # settings), then assert the second table still returns its own correct rows - and that the first
    # table is unaffected by the second's reads.
    node = started_cluster.instances["h0_0_0"]

    node.query("DROP TABLE IF EXISTS default.hive_orc_a")
    node.query("DROP TABLE IF EXISTS default.hive_orc_b")

    # Table A: (id, score) column order, stripe-level pruning on, file-level pruning off.
    node.query(
        """
        CREATE TABLE default.hive_orc_a (`id` Nullable(String), `score` Nullable(Int32), `day` Nullable(String))
        ENGINE = Hive('thrift://hivetest:9083', 'test', 'demo_orc') PARTITION BY(day)
        SETTINGS enable_orc_stripe_minmax_index = 1, enable_orc_file_minmax_index = 0
        """
    )
    # Table B: (score, id) column order and all minmax pruning off - a different cached layout/settings.
    node.query(
        """
        CREATE TABLE default.hive_orc_b (`score` Nullable(Int32), `id` Nullable(String), `day` Nullable(String))
        ENGINE = Hive('thrift://hivetest:9083', 'test', 'demo_orc') PARTITION BY(day)
        SETTINGS enable_orc_stripe_minmax_index = 0, enable_orc_file_minmax_index = 0
        """
    )

    # Warm the shared cache through table A with a selective read (builds the file objects and stripe
    # minmax under A's layout and settings).
    assert query_with_retry(node, "SELECT count(*) FROM default.hive_orc_a WHERE score < 5").strip() == "2"

    # Table B must return its own correct results, evaluated against its own layout and settings - not
    # table A's cached ones (before the fix both tables shared one cached file keyed only by the path).
    assert node.query("SELECT count(*) FROM default.hive_orc_b").strip() == "4"
    assert node.query("SELECT count(*) FROM default.hive_orc_b WHERE score > 5").strip() == "2"
    assert node.query("SELECT id, score FROM default.hive_orc_b ORDER BY score") == "a\t1\nb\t2\nc\t10\nd\t11\n"

    # Table A is likewise unaffected by table B's reads (independent cache entries).
    assert node.query("SELECT count(*) FROM default.hive_orc_a").strip() == "4"
    assert node.query("SELECT id, score FROM default.hive_orc_a ORDER BY score") == "a\t1\nb\t2\nc\t10\nd\t11\n"
