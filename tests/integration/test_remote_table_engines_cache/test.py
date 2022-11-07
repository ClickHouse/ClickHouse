import os
import io
import logging
import helpers.client
import pytest
from helpers.cluster import ClickHouseCluster, ClickHouseInstance
from helpers.test_tools import exec_query_with_retry
from pyhdfs import HdfsClient
from helpers.s3_common import disable_auth_for_s3_bucket

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
CURRENT_TEST_DIR = os.path.dirname(os.path.abspath(__file__))

hive_instance = "roottestremotetableenginescache_hdfs1_1"


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        node = cluster.add_instance(
            "node",
            main_configs=[
                "configs/config.d/remote_table_engines_cache.xml",
                "configs/config.d/named_collections.xml",
                "configs/config.d/cache_log.xml",
            ],
            # extra_configs=["configs/hdfs-site.xml", "data/prepare_hive_data.sh"],
            with_minio=True,
            with_hdfs=True,
            # with_hive=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        disable_auth_for_s3_bucket(cluster)

        # for hive
        # prepare_file_name = "prepare_hive_data.sh"
        # cluster.copy_file_to_container(
        #    hive_instance,
        #    f"{CURRENT_TEST_DIR}/data/{prepare_file_name}",
        #    f"/{prepare_file_name}",
        # )
        # cluster.exec_in_container(
        #    hive_instance,
        #    ["bash", "-c", f"bash /{prepare_file_name}"],
        # )

        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize(
    "engine, max_download_threads",
    [("s3", 0), ("s3", 1), ("hdfs", 0), ("url", 0), ("url", 1)],
)
def test_simple(cluster, engine, max_download_threads):
    node = cluster.instances["node"]  # type: ClickHouseInstance
    name = "test"

    def get_table_function(args, source=engine):
        additional_args = ", ".join([k + " = '" + v + "'" for k, v in args.items()])
        if len(additional_args) > 0:
            return f"{source}({source}, {additional_args})"
        else:
            return f"{source}({source})"

    args = {"filename": "test", "structure": "a UInt32"}
    if engine == "url":
        args[
            "url"
        ] = f"http://{cluster.minio_host}:{cluster.minio_port}/{cluster.minio_bucket}/"

    insertion_engine = {"s3": "s3", "hdfs": "hdfs", "url": "s3", "hive": "hive"}
    table_function = get_table_function(args, insertion_engine[engine])
    node.query(
        f"""
        insert into table function {table_function} select number from numbers(100) settings s3_truncate_on_insert=1;
    """
    )

    table_function = get_table_function(args)

    def query_with_settings(query, additional_settings):
        settings = {
            "enable_filesystem_cache_log": 1,
            "enable_cache_for_s3_table_engine": 1,
            "enable_cache_for_hdfs_table_engine": 1,
            "enable_cache_for_url_table_engine": 1,
        }
        settings.update(additional_settings)
        result_query = ";\n".join(
            ("set " + k + "=" + repr(v) for k, v in settings.items())
        )
        result_query += ";\n"
        result_query += query
        return node.query(result_query)

    def put_data(values):
        if engine == "s3" or engine == "url":
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

    args.update({"structure": "a UInt32, b UInt32, c UInt32"})
    table_function = get_table_function(args)

    values = "1,2,3\n3,2,1\n"
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

    assert result == "READ_FROM_FS_AND_DOWNLOADED_TO_CACHE\t12"

    assert "1" == node.query("select count() from system.filesystem_cache").strip()
    assert "12" == node.query("select sum(size) from system.filesystem_cache").strip()

    result = node.query(f"select * from {table_function}").strip()
    assert result == "1\t2\t3\n3\t2\t1"

    args.update({"structure": "auto"})
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
    assert (
        result
        == f"READ_FROM_CACHE\t{len(values)}\nREAD_FROM_FS_AND_DOWNLOADED_TO_CACHE\t{len(values)}"
    )

    result = node.query(f"select * from {table_function}").strip()
    assert result == "5\t4\t3\t2\t1"
