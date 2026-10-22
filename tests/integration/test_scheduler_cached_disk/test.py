import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/storage_configuration.xml"],
    with_minio=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield
    finally:
        cluster.shutdown()


def dequeued_requests():
    return int(
        node.query(
            "select sum(dequeued_requests) from system.scheduler where resource = 'io_s3' and type = 'request_queue'"
        ).strip()
    )


def test_cached_disk_uses_inner_disk_resource():
    node.query(
        """
        create resource io_s3 (write disk s3_inner, read disk s3_inner);
        create workload all;
        create table data (key UInt64, value String)
        engine = MergeTree() order by key
        settings storage_policy = 'cached_s3';
        """
    )

    before_insert = dequeued_requests()
    node.query(
        "insert into data select number, randomString(10000) from numbers(100) settings workload = 'all'"
    )
    assert dequeued_requests() > before_insert

    node.query("system drop filesystem cache")
    before_select = dequeued_requests()
    node.query("select sum(length(value)) from data settings workload = 'all'")
    assert dequeued_requests() > before_select

    before_cached_select = dequeued_requests()
    node.query(
        "select sum(length(value)) from data settings workload = 'all'",
        query_id="cached_select",
    )
    assert dequeued_requests() == before_cached_select

    node.query("system flush logs")
    cache_read_bytes = int(
        node.query(
            "select ProfileEvents['CachedReadBufferReadFromCacheBytes'] from system.query_log"
            " where query_id = 'cached_select' and type = 'QueryFinish'"
        ).strip()
    )
    assert cache_read_bytes > 0


def bg_download_cost():
    return int(
        node.query(
            "select sum(dequeued_cost) from system.scheduler where resource = 'io_s3'"
            " and type = 'request_queue' and path like '%bg_download%'"
        ).strip()
    )


def test_background_download_after_query_end():
    node.query(
        """
        create resource if not exists io_s3 (write disk s3_inner, read disk s3_inner);
        create workload if not exists bg_download settings max_bytes_per_second = 500000, max_burst_bytes = 1500000;
        create table if not exists data_bg (s String) engine = MergeTree order by tuple()
        settings storage_policy = 'cached_s3', min_bytes_for_wide_part = 0, index_granularity = 64;
        """
    )
    node.query("truncate table data_bg")
    node.query("insert into data_bg select randomString(1000) from numbers(16000)")
    node.query("system drop filesystem cache")
    node.query(
        "select s from data_bg limit 1 format Null settings workload = 'bg_download', max_threads = 1,"
        " filesystem_cache_allow_background_download = 1",
        query_id="bg_download_select",
    )
    assert_eq_with_retry(
        node,
        "select count() from system.processes where query_id = 'bg_download_select'",
        "0",
    )

    # The cache finishes the file segment in the background through the finished query's workload.
    cost = bg_download_cost()
    deadline = time.monotonic() + 30
    while bg_download_cost() == cost:
        assert time.monotonic() < deadline, "no IO was scheduled after the query ended"
        time.sleep(0.1)
    deadline = time.monotonic() + 60
    while (
        node.query(
            "select count() = 0 or countIf(state != 'DOWNLOADED') > 0 from system.filesystem_cache"
        ).strip()
        == "1"
    ):
        assert time.monotonic() < deadline, "the background download did not finish"
        time.sleep(1)

    assert node.query("select 1").strip() == "1"
    node.query("drop table data_bg sync")
    node.query("drop workload bg_download")
    node.query("drop resource if exists io_s3")
