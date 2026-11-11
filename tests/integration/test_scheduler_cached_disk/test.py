import pytest

from helpers.cluster import ClickHouseCluster

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
            "select sum(dequeued_requests) from system.scheduler where resource = 'io_s3' and type = 'fifo'"
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
