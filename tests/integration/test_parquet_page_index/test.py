import os
import time

import pyarrow.parquet as pq
import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
path_to_userfiles = "/var/lib/clickhouse/user_files/"
node = cluster.add_instance("node", external_dirs=[path_to_userfiles])


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def check_page_index(file_path):
    metadata = pq.read_metadata(file_path)
    assert (
        metadata
    ), "pyarrow.parquet library can't read parquet file written by Clickhouse"
    return metadata.row_group(0).column(0).has_offset_index


def delete_if_exists(file_path):
    if os.path.exists(file_path):
        os.remove(file_path)


@pytest.mark.parametrize(
    "query, expected_result",
    {
        (
            "SELECT number, number+1 FROM system.numbers LIMIT 100 "
            "INTO OUTFILE '{file_name}' FORMAT Parquet "
            "SETTINGS output_format_parquet_use_custom_encoder = false, "
            "output_format_parquet_write_page_index = true;",
            True,
        ),
        (
            "SELECT number, number+1 FROM system.numbers LIMIT 100 "
            "INTO OUTFILE '{file_name}' FORMAT Parquet "
            "SETTINGS output_format_parquet_use_custom_encoder = false, "
            "output_format_parquet_write_page_index = false;",
            False,
        ),
        # # default settings:
        # # output_format_parquet_use_custom_encoder = true
        (
            "SELECT number, number+1 FROM system.numbers LIMIT 100 "
            "INTO OUTFILE '{file_name}' FORMAT Parquet;",
            True,
        ),
    },
)
def test_parquet_page_index_select_into_outfile(query, expected_result, start_cluster):
    file_name = f"export{time.time()}.parquet"
    query = query.format(file_name=file_name)
    delete_if_exists(file_name)
    assert node.query(query) == ""
    assert (
        check_page_index(file_name) == expected_result
    ), "Page offset index have wrong value"
    delete_if_exists(file_name)


@pytest.mark.parametrize(
    "query, expected_result",
    {
        (
            "INSERT INTO TABLE FUNCTION file('{file_name}') "
            "SELECT number, number+1 FROM system.numbers LIMIT 100 "
            "SETTINGS output_format_parquet_use_custom_encoder=false, "
            "output_format_parquet_write_page_index=true FORMAT Parquet",
            True,
        ),
        (
            "INSERT INTO TABLE FUNCTION file('{file_name}') "
            "SELECT number, number+1 FROM system.numbers LIMIT 100 "
            "SETTINGS output_format_parquet_use_custom_encoder=false, "
            "output_format_parquet_write_page_index=false FORMAT Parquet",
            False,
        ),
        # # default settings:
        # # output_format_parquet_use_custom_encoder = true
        (
            "INSERT INTO TABLE FUNCTION file('{file_name}') "
            "SELECT number, number+1 FROM system.numbers LIMIT 100 FORMAT Parquet",
            True,
        ),
    },
)
def test_parquet_page_index_insert_into_table_function_file(
    query, expected_result, start_cluster
):
    file_name = f"export{time.time()}.parquet"
    query = query.format(file_name=file_name)
    file_path = f"{cluster.instances_dir}{path_to_userfiles}{file_name}"
    delete_if_exists(file_path)
    assert node.query(query) == ""
    assert (
        check_page_index(file_path) == expected_result
    ), "Page offset index have wrong value"
    delete_if_exists(file_path)
