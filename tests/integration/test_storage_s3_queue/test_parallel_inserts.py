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
            user_configs=["configs/users.xml"],
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


def run_with_retry(check_result, func, retries=100):
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
                f"SELECT file_name FROM system.s3queue WHERE zookeeper_path ilike '%{table_name}%' and status = 'Processed' and rows_processed > 0 "
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
            "s3queue_loading_retries": 20,
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
                f"SELECT file_name FROM system.s3queue WHERE zookeeper_path ilike '%{table_name}%' and status = 'Processed' and rows_processed > 0 "
            )
            .strip()
            .split("\n")
        )

    def get_failed_files():
        return set(
            node.query(
                f"SELECT file_name FROM system.s3queue WHERE zookeeper_path ilike '%{table_name}%' and status = 'Failed'"
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
