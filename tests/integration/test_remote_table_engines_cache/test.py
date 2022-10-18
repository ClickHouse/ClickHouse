import os
import io
import logging
import helpers.client
import pytest
from helpers.cluster import ClickHouseCluster, ClickHouseInstance
from helpers.test_tools import exec_query_with_retry
from pyhdfs import HdfsClient

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=[
                "configs/config.d/remote_table_engines_cache.xml",
                "configs/config.d/named_collections.xml",
                "configs/config.d/cache_log.xml",
            ],
            with_minio=True,
            with_hdfs=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


#@pytest.mark.parametrize("engine, max_download_threads", [("s3", 1), ("s3", 0), ("hdfs", 0)])
@pytest.mark.parametrize("engine, max_download_threads", [("hdfs", 0)])
def test_simple(cluster, engine, max_download_threads):
    node = cluster.instances["node"]  # type: ClickHouseInstance

    def get_table_function(args):
        additional_args = ", ".join([k + " = '" + v + "'" for k, v in args.items()])
        if len(additional_args) > 0:
            return f"{engine}({engine}, {additional_args})"
        else:
            return f"{engine}({engine})"

    args = {"filename" : "test", "structure" : "a UInt32"}
    table_function = get_table_function(args)

    node.query(
        f"""
        insert into table function {table_function} select number from numbers(100) settings s3_truncate_on_insert=1;
    """
    )

    def query_with_settings(query, additional_settings):
        settings = {
            "enable_filesystem_cache_log": 1,
            "enable_cache_for_s3_table_engine": 1,
            "enable_cache_for_hdfs_table_engine": 1,
        }
        settings.update(additional_settings)
        result_query = ";\n".join(
            ("set " + k + "=" + repr(v) for k, v in settings.items())
        )
        result_query += ";\n"
        result_query += query
        return node.query(result_query)

    def put_data(values):
        if engine == "s3":
            minio = cluster.minio_client
            bucket = cluster.minio_bucket
            name = "test"
            buf = io.BytesIO(values.encode())
            minio.put_object(bucket, name, buf, len(values))
        elif engine == "hdfs":
            hdfs_api = cluster.hdfs_api
            name = "test"
            hdfs_api.write_data("/" + name, values)

    result = node.query("show filesystem caches").strip()
    assert result == "_remote_table_engines_cache"

    query_id = query_with_settings(
        f"""
system drop filesystem cache;
select queryID() from (select * from {table_function}) limit 1;
    """,
        {"max_download_threads": max_download_threads},
    ).strip()

    result = node.query(
        f"""
system flush logs;
select read_type, size from system.filesystem_cache_log where query_id='{query_id}' order by size;
    """
    ).strip()

    assert result == "READ_FROM_FS_AND_DOWNLOADED_TO_CACHE\t290"

    query_id = query_with_settings(
        f"""
select queryID() from (select * from {table_function}) limit 1;
    """,
        {"max_download_threads": max_download_threads},
    ).strip()

    result = node.query(
        f"""
set enable_filesystem_cache_log = 1;
system flush logs;
select read_type, size from system.filesystem_cache_log where query_id='{query_id}' order by size;
    """
    ).strip()

    assert result == "READ_FROM_CACHE\t290"

    assert "1" == node.query("select count() from system.filesystem_cache").strip()

    values = "1,2,3\n3,2,1\n"
    put_data(values);

    args.update({"structure" : "a UInt32, b UInt32, c UInt32"})
    table_function = get_table_function(args)

    query_id = query_with_settings(
        f"""
select queryID() from (select * from {table_function}) limit 1;
    """,
        {"max_download_threads": max_download_threads},
    ).strip()

    result = node.query(
        f"""
system flush logs;
select read_type, size from system.filesystem_cache_log where query_id='{query_id}' order by read_type, size;
    """
    ).strip()

    assert result == "READ_FROM_FS_AND_DOWNLOADED_TO_CACHE\t12"

    assert "1" == node.query("select count() from system.filesystem_cache").strip()
    assert "12" == node.query("select sum(size) from system.filesystem_cache").strip()

    result = node.query(f"select * from {table_function}").strip()
    assert result == "1\t2\t3\n3\t2\t1"

    args.update({"structure" : "auto"})
    table_function = get_table_function(args)

    values = "5,4,3,2,1\n"
    put_data(values)

    query_id = query_with_settings(
        f"""
select queryID() from (select * from {table_function}) limit 1;
    """,
        {"max_download_threads": max_download_threads},
    ).strip()

    result = node.query(
        f"""
system flush logs;
select read_type, size from system.filesystem_cache_log where query_id='{query_id}' order by read_type, size;
    """
    ).strip()

    # first from schema inference and second from actual read.
    assert result == f"READ_FROM_CACHE\t{len(values)}\nREAD_FROM_FS_AND_DOWNLOADED_TO_CACHE\t{len(values)}"

    result = node.query(f"select * from {table_function}").strip()
    assert result == "5\t4\t3\t2\t1"
