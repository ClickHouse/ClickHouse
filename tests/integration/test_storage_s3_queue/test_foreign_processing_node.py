import json
import logging
import time
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.s3_queue_common import (
    generate_random_files,
    create_table,
    create_mv,
    generate_random_string,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "instance",
            user_configs=["configs/users.xml"],
            with_minio=True,
            with_zookeeper=True,
            main_configs=[
                "configs/zookeeper.xml",
                "configs/s3queue_log.xml",
            ],
            stay_alive=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def run_with_retry(check_result, func, retries=120):
    for _ in range(retries):
        last = func()
        if check_result(last):
            return last
        time.sleep(1)
    raise RuntimeError(f"{last} did not match expectations in {retries} retries")


def test_file_is_retried_after_foreign_processing_node_disappears(started_cluster):
    """A file whose `processing` node belongs to another processor must stay retryable.

    When `trySetProcessing` loses the race for the `processing` node, the local `FileStatus`
    is updated to `Processing`. Unlike `Processed` and `Failed`, that state is not backed by
    a persistent keeper node: the foreign processor can release the file without committing
    it (it can die, or fail the file and reset it). Treating the cached `Processing` state as
    terminal made this table skip the file until the file status was evicted from the cache
    or the server was restarted.

    The foreign processor is emulated by a real `processing` node in keeper (there is no
    difference from the point of view of this table - the node just does not carry our own
    processor info), and its disappearance by removing that node.

    The cached observation of the foreign `processing` node is trusted for
    `s3queue_foreign_processing_node_cache_ttl_seconds` (so that the table does not probe keeper
    for the file on every polling pass), therefore the file is picked up within that timeout
    after the node disappears, not immediately.
    """
    node = started_cluster.instances["instance"]

    table_name = f"test_foreign_processing_node_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"

    files_to_generate = 5
    generate_random_files(started_cluster, files_path, files_to_generate, start_ind=0, row_num=1)

    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_processing_threads_num": 1,
            "s3queue_loading_retries": 100,
            # Do not trust the observation of the foreign `processing` node for long,
            # so that the file is retried quickly after the node disappears.
            "s3queue_foreign_processing_node_cache_ttl_seconds": 5,
        },
    )

    # The node name is the SipHash64 of the file path, which is what
    # `ObjectStorageQueueIFileMetadata::getNodeName` uses.
    conflict_file = f"{files_path}/test_2.csv"
    conflict_node = node.query(f"SELECT sipHash64('{conflict_file}')").strip()
    zk = started_cluster.get_kazoo_client("zoo1")
    zk.ensure_path(f"{keeper_path}/processing")
    zk.create(f"{keeper_path}/processing/{conflict_node}", b"another processor")

    try:
        # The configured TTL must be visible to operators.
        assert (
            node.query(
                f"SELECT value FROM system.s3_queue_settings "
                f"WHERE table = '{table_name}' AND name = 'foreign_processing_node_cache_ttl_seconds'"
            ).strip()
            == "5"
        )

        create_mv(node, table_name, dst_table_name)

        def get_count():
            return int(node.query(f"SELECT count() FROM {dst_table_name}").strip())

        # Every file except the one held by the foreign processor is processed.
        run_with_retry(lambda x: x == files_to_generate - 1, get_count)

        # The file must not be committed while the foreign `processing` node is there.
        assert node.query(f"SELECT count() FROM {dst_table_name} WHERE _path LIKE '%test_2.csv'").strip() == "0"

        # The foreign processor released the file without committing it.
        zk.delete(f"{keeper_path}/processing/{conflict_node}")

        # It must now be picked up: the cached `Processing` state is only a hint.
        run_with_retry(lambda x: x == files_to_generate, get_count)
    finally:
        node.query(
            f"""
        DROP TABLE IF EXISTS {dst_table_name};
        DROP TABLE IF EXISTS {table_name};
        """
        )


def test_file_is_retried_in_ordered_mode(started_cluster):
    """Same as above, but for `ordered` mode.

    `Ordered` mode tracks a max processed file name instead of per-file `processed` nodes,
    but the `processing` nodes are per-file, so losing the race for one is handled the same
    way: the cached foreign `Processing` observation must stay a retryable hint.

    The foreign processor holds the lexicographically greatest file: committing the other
    files must not advance the max processed path past a file which was never committed.
    """
    node = started_cluster.instances["instance"]

    table_name = f"test_foreign_processing_node_ordered_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"

    files_to_generate = 5
    generate_random_files(started_cluster, files_path, files_to_generate, start_ind=0, row_num=1)

    create_table(
        started_cluster,
        node,
        table_name,
        "ordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_processing_threads_num": 1,
            "s3queue_loading_retries": 100,
            "s3queue_foreign_processing_node_cache_ttl_seconds": 5,
        },
    )

    # `test_4.csv` sorts after every other generated file, so the max processed path
    # of this table never reaches it while it is held by the foreign processor.
    conflict_file = f"{files_path}/test_4.csv"
    conflict_node = node.query(f"SELECT sipHash64('{conflict_file}')").strip()
    zk = started_cluster.get_kazoo_client("zoo1")
    zk.ensure_path(f"{keeper_path}/processing")
    zk.create(f"{keeper_path}/processing/{conflict_node}", b"another processor")

    try:
        create_mv(node, table_name, dst_table_name)

        def get_count():
            return int(node.query(f"SELECT count() FROM {dst_table_name}").strip())

        # Every file except the one held by the foreign processor is processed.
        run_with_retry(lambda x: x == files_to_generate - 1, get_count)
        assert node.query(f"SELECT count() FROM {dst_table_name} WHERE _path LIKE '%test_4.csv'").strip() == "0"

        # The foreign processor released the file without committing it.
        zk.delete(f"{keeper_path}/processing/{conflict_node}")

        # The file is retried when the cached observation expires: it was not
        # swallowed by the max processed path of the files committed around it.
        run_with_retry(lambda x: x == files_to_generate, get_count)
    finally:
        node.query(
            f"""
        DROP TABLE IF EXISTS {dst_table_name};
        DROP TABLE IF EXISTS {table_name};
        """
        )


def test_cached_state_updated_when_foreign_processor_commits(started_cluster):
    """A file committed by another processor must not stay cached as `Processing`.

    The listing pre-filter (`filterProcessableFiles`) drops a file as soon as its
    `processed` (or `failed`) node appears in keeper. That discovery is written back
    into the local file status cache, so `system.s3queue_metadata_cache` reports the
    terminal state instead of keeping the stale foreign `Processing` observation
    until cache eviction. The whole cached record follows keeper: a file failed by
    another processor carries the exception from the `failed` node.
    """
    node = started_cluster.instances["instance"]

    table_name = f"test_foreign_commit_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"

    files_to_generate = 3
    generate_random_files(started_cluster, files_path, files_to_generate, start_ind=0, row_num=1)

    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_processing_threads_num": 1,
            "s3queue_loading_retries": 100,
            # Always check keeper, so that the terminal node is discovered promptly.
            "s3queue_foreign_processing_node_cache_ttl_seconds": 0,
        },
    )

    # `test_1.csv` will be committed as processed by the foreign processor,
    # `test_2.csv` will be failed by it.
    processed_file = f"{files_path}/test_1.csv"
    failed_file = f"{files_path}/test_2.csv"
    zk = started_cluster.get_kazoo_client("zoo1")
    zk.ensure_path(f"{keeper_path}/processing")

    def keeper_node_of(file_path):
        return node.query(f"SELECT sipHash64('{file_path}')").strip()

    for conflict_file in (processed_file, failed_file):
        zk.create(f"{keeper_path}/processing/{keeper_node_of(conflict_file)}", b"another processor")

    def foreign_node_metadata(file_path, exception, retries):
        return json.dumps(
            {
                "file_path": file_path,
                "last_processed_timestamp": int(time.time()),
                "last_exception": exception,
                "retries": retries,
                "processor_id": "0",
            }
        ).encode()

    try:
        create_mv(node, table_name, dst_table_name)

        def get_count():
            return int(node.query(f"SELECT count() FROM {dst_table_name}").strip())

        run_with_retry(lambda x: x == files_to_generate - 2, get_count)

        def get_cached_status(file_path):
            return node.query(
                f"SELECT status FROM system.s3queue_metadata_cache "
                f"WHERE file_path LIKE '%{file_path}'"
            ).strip()

        # The cached states of the files are observations of the foreign `processing` nodes.
        run_with_retry(lambda x: x == "Processing", lambda: get_cached_status(processed_file))
        run_with_retry(lambda x: x == "Processing", lambda: get_cached_status(failed_file))

        # The foreign processor commits one file and fails the other.
        zk.create(
            f"{keeper_path}/processed/{keeper_node_of(processed_file)}",
            foreign_node_metadata(processed_file, exception="", retries=0),
        )
        zk.delete(f"{keeper_path}/processing/{keeper_node_of(processed_file)}")
        zk.create(
            f"{keeper_path}/failed/{keeper_node_of(failed_file)}",
            foreign_node_metadata(failed_file, exception="Cannot parse the file", retries=100),
        )
        zk.delete(f"{keeper_path}/processing/{keeper_node_of(failed_file)}")

        # The cached states follow keeper instead of staying `Processing`.
        run_with_retry(lambda x: x == "Processed", lambda: get_cached_status(processed_file))
        run_with_retry(lambda x: x == "Failed", lambda: get_cached_status(failed_file))

        # The failed file carries the exception of the processor which failed it.
        assert (
            node.query(
                f"SELECT exception FROM system.s3queue_metadata_cache "
                f"WHERE file_path LIKE '%{failed_file}'"
            ).strip()
            == "Cannot parse the file"
        )

        # Neither file was ingested by this table.
        assert get_count() == files_to_generate - 2
        assert node.query(f"SELECT count() FROM {dst_table_name} WHERE _path LIKE '%test_1.csv'").strip() == "0"
        assert node.query(f"SELECT count() FROM {dst_table_name} WHERE _path LIKE '%test_2.csv'").strip() == "0"

        # The write-back must not touch the cached record of a file processed by THIS
        # server: `test_0.csv` has been relisted many times by now (the TTL is zero),
        # and its `processed` node is discovered by the pre-filter on every pass.
        assert (
            node.query(
                f"SELECT status, rows_processed FROM system.s3queue_metadata_cache "
                f"WHERE file_path LIKE '%{files_path}/test_0.csv'"
            ).strip()
            == "Processed\t1"
        )
    finally:
        node.query(
            f"""
        DROP TABLE IF EXISTS {dst_table_name};
        DROP TABLE IF EXISTS {table_name};
        """
        )


def test_foreign_processing_node_cache_ttl_is_per_table(started_cluster):
    """`foreign_processing_node_cache_ttl_seconds` must be honored per table.

    `ObjectStorageQueueMetadataFactory` shares one `ObjectStorageQueueMetadata` between all
    tables with the same `keeper_path`, so a setting kept in that shared object would be
    silently fixed by the table which was created first. This setting belongs to the table:
    both introspection and the actual retry window must use the value from its own DDL.
    """
    node = started_cluster.instances["instance"]

    suffix = generate_random_string()
    first_table_name = f"test_foreign_ttl_first_{suffix}"
    second_table_name = f"test_foreign_ttl_second_{suffix}"
    dst_table_name = f"test_foreign_ttl_dst_{suffix}"
    keeper_path = f"/clickhouse/test_foreign_ttl_{suffix}"
    files_path = f"test_foreign_ttl_{suffix}_data"

    files_to_generate = 3
    generate_random_files(started_cluster, files_path, files_to_generate, start_ind=0, row_num=1)

    # The first table trusts an observation of a foreign `processing` node for an hour.
    create_table(
        started_cluster,
        node,
        first_table_name,
        "unordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_processing_threads_num": 1,
            "s3queue_foreign_processing_node_cache_ttl_seconds": 3600,
        },
    )

    # The second table shares the keeper path, but always checks keeper.
    create_table(
        started_cluster,
        node,
        second_table_name,
        "unordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_processing_threads_num": 1,
            "s3queue_loading_retries": 100,
            "s3queue_foreign_processing_node_cache_ttl_seconds": 0,
        },
    )

    conflict_file = f"{files_path}/test_1.csv"
    conflict_node = node.query(f"SELECT sipHash64('{conflict_file}')").strip()
    zk = started_cluster.get_kazoo_client("zoo1")
    zk.ensure_path(f"{keeper_path}/processing")
    zk.create(f"{keeper_path}/processing/{conflict_node}", b"another processor")

    try:
        # Each table reports the value from its own DDL, not the value of the first table.
        def get_setting(table):
            return node.query(
                f"SELECT value FROM system.s3_queue_settings "
                f"WHERE table = '{table}' AND name = 'foreign_processing_node_cache_ttl_seconds'"
            ).strip()

        assert get_setting(first_table_name) == "3600"
        assert get_setting(second_table_name) == "0"

        # Only the second table streams, so it is its own TTL which is in effect.
        create_mv(node, second_table_name, dst_table_name)

        def get_count():
            return int(node.query(f"SELECT count() FROM {dst_table_name}").strip())

        run_with_retry(lambda x: x == files_to_generate - 1, get_count)

        # With the TTL of the first table (an hour) the file would not be retried in time.
        zk.delete(f"{keeper_path}/processing/{conflict_node}")
        run_with_retry(lambda x: x == files_to_generate, get_count, retries=60)
    finally:
        node.query(
            f"""
        DROP TABLE IF EXISTS {dst_table_name};
        DROP TABLE IF EXISTS {second_table_name};
        DROP TABLE IF EXISTS {first_table_name};
        """
        )


def test_foreign_processing_node_cache_ttl_is_alterable(started_cluster):
    """`foreign_processing_node_cache_ttl_seconds` must be changeable on a live table.

    Shortening the TTL with `ALTER TABLE ... MODIFY SETTING` is how an operator gets a
    stuck file retried immediately. The running streaming task reads the value through
    a reference to the storage, so the new value applies without recreating the table
    or its file iterator.
    """
    node = started_cluster.instances["instance"]

    table_name = f"test_foreign_ttl_alter_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"

    files_to_generate = 3
    generate_random_files(started_cluster, files_path, files_to_generate, start_ind=0, row_num=1)

    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_processing_threads_num": 1,
            "s3queue_loading_retries": 100,
            # Without the ALTER below the file would not be retried for an hour.
            "s3queue_foreign_processing_node_cache_ttl_seconds": 3600,
        },
    )

    conflict_file = f"{files_path}/test_1.csv"
    conflict_node = node.query(f"SELECT sipHash64('{conflict_file}')").strip()
    zk = started_cluster.get_kazoo_client("zoo1")
    zk.ensure_path(f"{keeper_path}/processing")
    zk.create(f"{keeper_path}/processing/{conflict_node}", b"another processor")

    try:
        create_mv(node, table_name, dst_table_name)

        def get_count():
            return int(node.query(f"SELECT count() FROM {dst_table_name}").strip())

        run_with_retry(lambda x: x == files_to_generate - 1, get_count)

        # The foreign `processing` node has been observed; its owner releases the file
        # without committing it. The observation is trusted for an hour.
        def get_cached_status():
            return node.query(
                f"SELECT status FROM system.s3queue_metadata_cache "
                f"WHERE file_path LIKE '%{conflict_file}'"
            ).strip()

        run_with_retry(lambda x: x == "Processing", get_cached_status)
        zk.delete(f"{keeper_path}/processing/{conflict_node}")

        node.query(
            f"ALTER TABLE {table_name} MODIFY SETTING s3queue_foreign_processing_node_cache_ttl_seconds = 0"
        )

        # The new value is reported and, more importantly, in effect: the file is
        # retried by the already-running streaming task.
        assert (
            node.query(
                f"SELECT value FROM system.s3_queue_settings "
                f"WHERE table = '{table_name}' AND name = 'foreign_processing_node_cache_ttl_seconds'"
            ).strip()
            == "0"
        )
        run_with_retry(lambda x: x == files_to_generate, get_count)
    finally:
        node.query(
            f"""
        DROP TABLE IF EXISTS {dst_table_name};
        DROP TABLE IF EXISTS {table_name};
        """
        )


def test_ttl_bounds_retry_latency_on_idle_queue(started_cluster):
    """`foreign_processing_node_cache_ttl_seconds` bounds the retry latency on an idle queue.

    With a single foreign-held file every streaming cycle processes zero rows, so the
    polling backoff grows far beyond the TTL. The streaming task must wake up no later
    than the earliest pending foreign-processing recheck instead of sleeping through
    the backoff, otherwise the TTL is not a bound on the retry latency at all.
    """
    node = started_cluster.instances["instance"]

    table_name = f"test_foreign_idle_queue_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"

    # The only file of the queue is held by the foreign processor: the queue is idle.
    generate_random_files(started_cluster, files_path, 1, start_ind=0, row_num=1)

    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_processing_threads_num": 1,
            "s3queue_loading_retries": 100,
            "s3queue_foreign_processing_node_cache_ttl_seconds": 5,
            # The backoff after an empty cycle far exceeds the TTL: without the
            # recheck-based wake-up the retry would take more than two minutes.
            "s3queue_polling_min_timeout_ms": 1000,
            "s3queue_polling_backoff_ms": 120000,
            "s3queue_polling_max_timeout_ms": 600000,
        },
    )

    conflict_file = f"{files_path}/test_0.csv"
    conflict_node = node.query(f"SELECT sipHash64('{conflict_file}')").strip()
    zk = started_cluster.get_kazoo_client("zoo1")
    zk.ensure_path(f"{keeper_path}/processing")
    zk.create(f"{keeper_path}/processing/{conflict_node}", b"another processor")

    try:
        create_mv(node, table_name, dst_table_name)

        # The first cycle observes the foreign `processing` node and processes nothing.
        def get_cached_status():
            return node.query(
                f"SELECT status FROM system.s3queue_metadata_cache "
                f"WHERE file_path LIKE '%{conflict_file}'"
            ).strip()

        run_with_retry(lambda x: x == "Processing", get_cached_status)

        # The foreign processor releases the file without committing it.
        zk.delete(f"{keeper_path}/processing/{conflict_node}")

        def get_count():
            return int(node.query(f"SELECT count() FROM {dst_table_name}").strip())

        # The file is picked up within the TTL plus a couple of streaming cycles,
        # well before the two-minute polling backoff.
        run_with_retry(lambda x: x == 1, get_count, retries=45)
    finally:
        node.query(
            f"""
        DROP TABLE IF EXISTS {dst_table_name};
        DROP TABLE IF EXISTS {table_name};
        """
        )
