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


def workload_cost(workload):
    return int(
        node.query(
            "select sum(dequeued_cost) from system.scheduler where resource = 'io_s3'"
            f" and type = 'request_queue' and path like '%{workload}%'"
        ).strip()
    )


def wait_query_end(query_id):
    assert_eq_with_retry(
        node, f"select count() from system.processes where query_id = '{query_id}'", "0"
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
    # Without the background download the query is the last holder, so only its own `complete` unbinds the reader.
    node.query("system drop filesystem cache")
    node.query(
        "select s from data_bg limit 1 format Null settings workload = 'bg_download', max_threads = 1,"
        " filesystem_cache_allow_background_download = 0",
        query_id="bg_download_off_select",
    )
    wait_query_end("bg_download_off_select")
    assert node.query("select 1").strip() == "1"
    node.query("system drop filesystem cache")
    node.query(
        "select s from data_bg limit 1 format Null settings workload = 'bg_download', max_threads = 1,"
        " filesystem_cache_allow_background_download = 1",
        query_id="bg_download_select",
    )
    wait_query_end("bg_download_select")

    # The cache finishes the file segment in the background, but not through the finished query's workload.
    cost = workload_cost("bg_download")
    deadline = time.monotonic() + 60
    while (
        node.query(
            "select count() = 0 or countIf(state != 'DOWNLOADED') > 0 from system.filesystem_cache"
        ).strip()
        == "1"
    ):
        assert time.monotonic() < deadline, "the background download did not finish"
        time.sleep(1)
    assert workload_cost("bg_download") == cost

    assert node.query("select 1").strip() == "1"
    node.query("drop table data_bg sync")
    node.query("drop workload bg_download")
    node.query("drop resource if exists io_s3")


def test_takeover_after_query_end():
    node.query(
        """
        create resource if not exists io_s3 (write disk s3_inner, read disk s3_inner);
        create workload if not exists wl_a settings max_bytes_per_second = 200000, max_burst_bytes = 100000;
        create workload if not exists wl_b;
        create table if not exists data_takeover (s String) engine = MergeTree order by tuple()
        settings storage_policy = 'cached_s3', min_bytes_for_wide_part = 0, index_granularity = 64;
        """
    )
    node.query("truncate table data_takeover")
    node.query("insert into data_takeover select randomString(1000) from numbers(4000)")
    node.query("system drop filesystem cache")

    settings = "max_threads = 1, filesystem_cache_allow_background_download = 0"
    query_a = node.get_query_request(
        f"select s from data_takeover limit 1 format Null settings workload = 'wl_a', {settings}",
        query_id="takeover_a",
    )
    deadline = time.monotonic() + 30
    while (
        node.query(
            "select countIf(state = 'DOWNLOADING') from system.filesystem_cache"
        ).strip()
        == "0"
    ):
        assert time.monotonic() < deadline, "the first query did not start downloading"
        time.sleep(0.1)

    # The second query waits for the first one's download and then takes over its reader.
    query_b = node.get_query_request(
        f"select sum(length(s)) from data_takeover settings workload = 'wl_b', {settings},"
        " optimize_functions_to_subcolumns = 0,"
        " filesystem_cache_wait_for_concurrent_download_timeout_milliseconds = 30000",
        query_id="takeover_b",
    )
    query_a.get_answer()
    wait_query_end("takeover_a")
    cost_a = workload_cost("wl_a")
    assert query_b.get_answer().strip() == "4000000"
    # The rest of the segment, read through the reader the first query created, is charged to the second one.
    assert workload_cost("wl_a") == cost_a
    assert workload_cost("wl_b") > cost_a

    assert node.query("select 1").strip() == "1"
    node.query("drop table data_takeover sync")
    node.query("drop workload wl_a")
    node.query("drop workload wl_b")
    node.query("drop resource if exists io_s3")
