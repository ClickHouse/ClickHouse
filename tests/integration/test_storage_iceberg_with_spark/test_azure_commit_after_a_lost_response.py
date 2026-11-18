#!/usr/bin/env python3

import pytest

from helpers.iceberg_utils import create_iceberg_table, get_uuid_str

FAILPOINT = "azure_inject_lost_response_of_conditional_write_once"


# 0: the retry comes from the writer. 10, the default: the SDK retries inside the call, out of sight of the writer.
@pytest.mark.parametrize("azure_sdk_max_retries", [0, 10])
def test_azure_commit_after_a_lost_response(started_cluster_iceberg_with_spark, azure_sdk_max_retries):
    """Azure stores the new metadata file of a commit, and the answer is lost. The retry gets 409, and the idempotency id
    in the metadata of the blob proves that it is the answer to the applied attempt, so the commit is published.
    Stateless tests cannot cover it, because their Azure emulator ignores `If-None-Match`."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    table_name = "test_azure_commit_after_a_lost_response_" + get_uuid_str()
    settings = {"allow_insert_into_iceberg": 1, "azure_sdk_max_retries": azure_sdk_max_retries}

    create_iceberg_table("azure", instance, table_name, started_cluster_iceberg_with_spark, "(x Int32)", settings=settings)
    instance.query(f"INSERT INTO {table_name} VALUES (1)", settings=settings)

    instance.query(f"SYSTEM ENABLE FAILPOINT {FAILPOINT}")
    try:
        instance.query(f"INSERT INTO {table_name} VALUES (2)", settings=settings)
    finally:
        instance.query(f"SYSTEM DISABLE FAILPOINT {FAILPOINT}")

    assert instance.query(f"SELECT x FROM {table_name} ORDER BY x") == "1\n2\n"
