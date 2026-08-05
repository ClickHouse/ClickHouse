import logging
import time
import math
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.s3_queue_common import (
    generate_random_files,
    put_s3_file_content,
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
            user_configs=[
                "configs/users.xml",
                "configs/insert_deduplication.xml",
                ],
            with_minio=True,
            with_azurite=True,
            with_zookeeper=True,
            main_configs=[
                "configs/zookeeper.xml",
                "configs/s3queue_log.xml",
                "configs/remote_servers.xml",
            ],
            stay_alive=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def run_with_retry(check_result, func, retries=300):
    for _ in range(retries):
        last = func()
        if check_result(last):
            return last
        time.sleep(1)
    raise RuntimeError(f"{last} did not match expectations in {retries} retries")


@pytest.mark.parametrize("parallel_inserts", [0, 1])
def test_parallel_inserts_generated_parts(started_cluster, parallel_inserts):
    """Ensure that per-thread INSERTs does not affect on the number of INSERTs, i.e. it still depends on the max_processed_*_before_commit instead"""
    node = started_cluster.instances["instance"]

    # A unique table name is necessary for repeatable tests
    table_name = f"test_parallel_inserts_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    dst_table_name = f"{table_name}_dst"
    files_to_generate = 40

    processing_threads_num = 4
    max_processed_files_before_commit = 2
    # Ensure that w/ and w/o parallel_inserts will generate different number of parts
    assert processing_threads_num != max_processed_files_before_commit

    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "parallel_inserts": parallel_inserts,
            "s3queue_processing_threads_num": processing_threads_num,
            "s3queue_loading_retries": 100,
            "s3queue_max_processed_files_before_commit": max_processed_files_before_commit,
        },
    )
    generate_random_files(
        started_cluster, files_path, files_to_generate, start_ind=0, row_num=1
    )
    create_mv(node, table_name, dst_table_name)

    expected_processed = ["test_" + str(i) + ".csv" for i in range(files_to_generate)]

    def get_count():
        return int(node.query(f"select count() from {dst_table_name}"))

    def get_processed_files():
        return set(
            node.query(
                f"SELECT file_name FROM system.s3queue_metadata_cache WHERE zookeeper_path ilike '%{table_name}%' and status = 'Processed' and rows_processed > 0 "
            )
            .strip()
            .split("\n")
        )

    run_with_retry(lambda x: x == len(expected_processed), get_count)
    run_with_retry(lambda x: x == set(expected_processed), get_processed_files)

    def get_new_parts_in_dst():
        return int(
            node.query(
                "SYSTEM FLUSH LOGS system.part_log;"
                f"SELECT count() FROM system.part_log WHERE table = '{dst_table_name}' and event_type = 'NewPart'"
            ).strip()
        )

    new_parts = get_new_parts_in_dst()
    expected_parts = math.ceil(
        len(expected_processed) / max_processed_files_before_commit
    )
    if not parallel_inserts:
        # Note, due to parallel processing (not inserts) in this case it is
        # possible to have less parts
        assert new_parts <= expected_parts
        # But not too less
        assert new_parts >= expected_parts*0.5
    else:
        # Note, in case of parallel inserts due to parallelism we can have more parts
        assert new_parts >= expected_parts
        # But let's ensure that not too much more
        assert new_parts < len(expected_processed)

    node.query(
        f"""
    DROP TABLE {dst_table_name};
    DROP TABLE {table_name};
    """
    )


@pytest.mark.parametrize("parallel_inserts", [0, 1])
def test_parallel_inserts_with_failures(started_cluster, parallel_inserts):
    """Ensure that in case of errors, files won't be inserted multiple times w/ and w/o parallel_inserts"""
    node = started_cluster.instances["instance"]

    # A unique table name is necessary for repeatable tests
    table_name = f"test_parallel_inserts_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    dst_table_name = f"{table_name}_dst"
    files_to_generate = 40
    max_processed_files_before_commit = 10

    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "parallel_inserts": parallel_inserts,
            "s3queue_processing_threads_num": 16,
            "s3queue_loading_retries": 100,
            "s3queue_max_processed_files_before_commit": max_processed_files_before_commit,
        },
    )
    generate_random_files(
        started_cluster, files_path, files_to_generate, start_ind=0, row_num=1
    )

    incorrect_values_csv = (
        "\n".join((",".join(map(str, row)) for row in [["failed", 1, 1]])) + "\n"
    ).encode()

    correct_values_csv = (
        "\n".join((",".join(map(str, row)) for row in [[1, 1, 1]])) + "\n"
    ).encode()

    # Ensure that in case of INSERT failures it will mark only this file as failed not the whole batch
    # NOTE: generate_random_files() uses randint(0, 1000), so 10000 does not overlaps
    failed_on_insert_values_csv = (
        "\n".join((",".join(map(str, row)) for row in [[10000, 10000, 10000]])) + "\n"
    ).encode()

    put_s3_file_content(
        started_cluster, f"{files_path}/test_99.csv", correct_values_csv
    )
    put_s3_file_content(
        started_cluster, f"{files_path}/test_999.csv", failed_on_insert_values_csv
    )
    put_s3_file_content(
        started_cluster, f"{files_path}/test_9999.csv", incorrect_values_csv
    )
    put_s3_file_content(
        started_cluster, f"{files_path}/test_99999.csv", correct_values_csv
    )
    put_s3_file_content(
        started_cluster, f"{files_path}/test_999999.csv", correct_values_csv
    )

    create_mv(
        node,
        table_name,
        dst_table_name,
        # Make failed_on_insert_values_csv fail via INSERT
        extra_dst_format="CONSTRAINT column1_constraint CHECK column1 != 10000",
    )

    def get_count():
        return int(node.query(f"select count() from {dst_table_name}"))

    def get_processed_files():
        return set(
            node.query(
                f"SELECT file_name FROM system.s3queue_metadata_cache WHERE zookeeper_path ilike '%{table_name}%' and status = 'Processed' and rows_processed > 0 "
            )
            .strip()
            .split("\n")
        )

    def get_failed_files():
        return set(
            node.query(
                f"SELECT file_name FROM system.s3queue_metadata_cache WHERE zookeeper_path ilike '%{table_name}%' and status = 'Failed'"
            )
            .strip()
            .split("\n")
        )

    # wait until both files will be retries, there can be more failures due to batching
    run_with_retry(
        lambda x: set(["test_9999.csv", "test_999.csv"]).issubset(x), get_failed_files
    )
    # and then, remove the constraint to unblock the queue
    node.query(f"ALTER TABLE {dst_table_name} DROP CONSTRAINT column1_constraint")

    expected_processed = ["test_" + str(i) + ".csv" for i in range(files_to_generate)]
    expected_processed.extend(
        ["test_99.csv", "test_99999.csv", "test_999999.csv", "test_999.csv"]
    )

    run_with_retry(lambda x: x == len(expected_processed), get_count)
    run_with_retry(lambda x: x == set(expected_processed), get_processed_files)

    def get_new_parts_in_dst():
        return int(
            node.query(
                "SYSTEM FLUSH LOGS system.part_log;"
                f"SELECT count() FROM system.part_log WHERE table = '{dst_table_name}' and event_type = 'NewPart'"
            ).strip()
        )

    node.query(
        f"""
    DROP TABLE {dst_table_name};
    DROP TABLE {table_name};
    """
    )


def test_batch_set_processing_failure_does_not_crash(started_cluster):
    """Regression for the out-of-bounds crash in FileIterator::next.

    The unordered hash-ring batch path aborts the server when two things happen for one
    batch:
      1) at least one file is non-processable, so num_successful_objects < new_batch.size()
         and the compaction block runs;
      2) the keeper multi that sets the batch as processing fails, so file_metadatas is
         cleared before that compaction runs.
    The compaction then subscripted the now-empty file_metadatas.

    Both conditions are the ones that happen in production when several consumers share a
    keeper path: another consumer grabs one file first (making it non-processable) and has
    already created a processing node for another file in the batch (making this consumer's
    multi fail). Here condition 2 is reproduced by creating a real processing node in keeper
    for one of the batch files, so the engine's own keeper multi fails against it (no faked
    keeper response). Condition 1 is reproduced with a small failpoint that marks the first
    file of the batch non-processable through the same std::nullopt path the engine takes
    when a file is already being processed elsewhere.
    """
    node = started_cluster.instances["instance"]

    table_name = f"test_batch_set_processing_failure_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"

    # A handful of files, all listed in a single batch. File 0 is forced non-processable by
    # the failpoint; a different file gets a pre-created processing node so the multi fails.
    files_to_generate = 10
    generate_random_files(
        started_cluster, files_path, files_to_generate, start_ind=0, row_num=1
    )

    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "enable_hash_ring_filtering": 1,
            "s3queue_processing_threads_num": 1,
            "s3queue_loading_retries": 100,
            # Both conditions must land in the SAME batch, so pin the listing batch size instead
            # of relying on the engine default (1000) happening to exceed files_to_generate.
            "list_objects_batch_size": files_to_generate,
        },
    )

    # Pre-create a real processing node for one file (not file 0, which the failpoint skips)
    # so the engine's keeper multi fails against it exactly as it would if another consumer
    # had set that file as processing first. The node name is the SipHash64 of the file path,
    # which is what ObjectStorageQueueIFileMetadata::getNodeName uses.
    conflict_file = f"{files_path}/test_1.csv"
    conflict_node = node.query(f"SELECT sipHash64('{conflict_file}')").strip()
    zk = started_cluster.get_kazoo_client("zoo1")
    zk.ensure_path(f"{keeper_path}/processing")
    zk.create(f"{keeper_path}/processing/{conflict_node}", b"conflict")

    def batch_set_processing_failures():
        node.query("SELECT 1")  # fails loudly if the server aborted
        node.query("SYSTEM FLUSH LOGS")
        return int(
            node.query(
                "SELECT value FROM system.events"
                " WHERE event = 'ObjectStorageQueueFailedToBatchSetProcessing'"
            ).strip()
            or 0
        )

    # system.events counts for the lifetime of the server process, which is shared with the
    # other tests of this module, so snapshot it before anything can consume from this table
    # (the MV below is what starts the streaming task) and later wait for an increase. An
    # absolute "> 0" would already be satisfied by an earlier increment and the test would
    # then delete the conflict node without ever exercising the failing batch.
    failures_before = batch_set_processing_failures()

    node.query(
        "SYSTEM ENABLE FAILPOINT object_storage_queue_skip_one_file_in_batch"
    )
    try:
        create_mv(node, table_name, dst_table_name)

        # Wait until the batch that hits both conditions has actually been attempted: the failed
        # keeper multi against the pre-created processing node bumps this profile event. This is
        # the batch where the server aborted without the fix, so observing it guarantees the
        # fixed path was exercised (a delayed CI worker cannot skip it). The server must stay
        # alive while we wait.
        run_with_retry(
            lambda x: x > failures_before, batch_set_processing_failures, retries=120
        )

        # Remove the artificial conflict and confirm the queue keeps making progress after the
        # failed batch (the iterator recovered rather than getting stuck or having crashed).
        zk.delete(f"{keeper_path}/processing/{conflict_node}")

        def get_count():
            return int(node.query(f"SELECT count() FROM {dst_table_name}"))

        # All files except the one left in an in-memory Processing state by the aborted
        # multi are processed; the point is that the server survived and the queue drains.
        run_with_retry(lambda x: x >= files_to_generate - 1, get_count)

        # The server must still be alive and responsive.
        assert node.query("SELECT 1").strip() == "1"
    finally:
        node.query(
            "SYSTEM DISABLE FAILPOINT object_storage_queue_skip_one_file_in_batch"
        )
        node.query(
            f"""
        DROP TABLE IF EXISTS {dst_table_name};
        DROP TABLE IF EXISTS {table_name};
        """
        )
