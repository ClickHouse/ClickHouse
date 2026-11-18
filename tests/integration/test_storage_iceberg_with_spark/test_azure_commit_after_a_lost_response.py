#!/usr/bin/env python3

from helpers.iceberg_utils import create_iceberg_table, get_uuid_str

FAILPOINT = "azure_inject_lost_response_of_conditional_write_once"


def test_azure_commit_after_a_lost_response(started_cluster_iceberg_with_spark):
    """Azure stores the new metadata file of a commit, and the answer is lost. The retry gets 409, which can be the
    answer to the applied attempt, so the outcome of the commit is unknown: the commit fails, keeps its files, and the
    table shows the new version. Stateless tests cannot cover it, because their Azure emulator ignores `If-None-Match`."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    table_name = "test_azure_commit_after_a_lost_response_" + get_uuid_str()
    # The writer sees a retry only when the SDK does not retry inside.
    settings = {"allow_insert_into_iceberg": 1, "azure_sdk_max_retries": 0}

    create_iceberg_table("azure", instance, table_name, started_cluster_iceberg_with_spark, "(x Int32)", settings=settings)
    instance.query(f"INSERT INTO {table_name} VALUES (1)", settings=settings)

    instance.query(f"SYSTEM ENABLE FAILPOINT {FAILPOINT}")
    try:
        error = instance.query_and_get_error(f"INSERT INTO {table_name} VALUES (2)", settings=settings)
    finally:
        instance.query(f"SYSTEM DISABLE FAILPOINT {FAILPOINT}")

    assert "UNKNOWN_STATUS_OF_TRANSACTION" in error, error
    assert instance.query(f"SELECT x FROM {table_name} ORDER BY x") == "1\n2\n"
