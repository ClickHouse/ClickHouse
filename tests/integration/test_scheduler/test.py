# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long

import time
import threading
import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    stay_alive=True,
    main_configs=["configs/scheduler.xml"],
    with_minio=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield
    finally:
        cluster.shutdown()


def test_s3_resource_request_granularity():
    node.query(
        f"""
        drop table if exists data;
        create table data (key UInt64 CODEC(NONE), value String CODEC(NONE)) engine=MergeTree() order by key settings min_bytes_for_wide_part=1e9, storage_policy='s3';
    """
    )

    total_bytes = 50000000  # Approximate data size
    max_bytes_per_request = 2000000  # Should be ~1MB or less in general
    min_bytes_per_request = 6000  # Small requests are ok, but we don't want hurt performance with too often resource requests

    writes_before = int(
        node.query(
            f"select dequeued_requests from system.scheduler where resource='network_write' and path='/prio/admin'"
        ).strip()
    )
    write_bytes_before = int(
        node.query(
            f"select dequeued_cost from system.scheduler where resource='network_write' and path='/prio/admin'"
        ).strip()
    )
    node.query(
        f"insert into data select number, randomString(10000000) from numbers(5) SETTINGS workload='admin'"
    )
    writes_after = int(
        node.query(
            f"select dequeued_requests from system.scheduler where resource='network_write' and path='/prio/admin'"
        ).strip()
    )
    write_bytes_after = int(
        node.query(
            f"select dequeued_cost from system.scheduler where resource='network_write' and path='/prio/admin'"
        ).strip()
    )

    assert write_bytes_after - write_bytes_before > 1.0 * total_bytes
    assert write_bytes_after - write_bytes_before < 1.2 * total_bytes
    assert (write_bytes_after - write_bytes_before) / (
        writes_after - writes_before
    ) < max_bytes_per_request
    assert (write_bytes_after - write_bytes_before) / (
        writes_after - writes_before
    ) > min_bytes_per_request

    reads_before = int(
        node.query(
            f"select dequeued_requests from system.scheduler where resource='network_read' and path='/prio/admin'"
        ).strip()
    )
    read_bytes_before = int(
        node.query(
            f"select dequeued_cost from system.scheduler where resource='network_read' and path='/prio/admin'"
        ).strip()
    )
    node.query(
        f"select count() from data where not ignore(*) SETTINGS workload='admin'"
    )
    reads_after = int(
        node.query(
            f"select dequeued_requests from system.scheduler where resource='network_read' and path='/prio/admin'"
        ).strip()
    )
    read_bytes_after = int(
        node.query(
            f"select dequeued_cost from system.scheduler where resource='network_read' and path='/prio/admin'"
        ).strip()
    )

    assert read_bytes_after - read_bytes_before > 1.0 * total_bytes
    assert read_bytes_after - read_bytes_before < 1.2 * total_bytes
    assert (read_bytes_after - read_bytes_before) / (
        reads_after - reads_before
    ) < max_bytes_per_request
    assert (read_bytes_after - read_bytes_before) / (
        reads_after - reads_before
    ) > min_bytes_per_request


def test_s3_disk():
    node.query(
        f"""
        drop table if exists data;
        create table data (key UInt64 CODEC(NONE)) engine=MergeTree() order by tuple() settings min_bytes_for_wide_part=1e9, storage_policy='s3';
    """
    )

    def write_query(workload):
        try:
            node.query(
                f"insert into data select * from numbers(1e5) settings workload='{workload}'"
            )
        except QueryRuntimeException:
            pass

    thread1 = threading.Thread(target=write_query, args=["development"])
    thread2 = threading.Thread(target=write_query, args=["production"])
    thread3 = threading.Thread(target=write_query, args=["admin"])

    thread1.start()
    thread2.start()
    thread3.start()

    thread3.join()
    thread2.join()
    thread1.join()

    assert (
        node.query(
            f"select dequeued_requests>0 from system.scheduler where resource='network_write' and path='/prio/admin'"
        )
        == "1\n"
    )
    assert (
        node.query(
            f"select dequeued_requests>0 from system.scheduler where resource='network_write' and path='/prio/fair/dev'"
        )
        == "1\n"
    )
    assert (
        node.query(
            f"select dequeued_requests>0 from system.scheduler where resource='network_write' and path='/prio/fair/prod'"
        )
        == "1\n"
    )

    def read_query(workload):
        try:
            node.query(f"select sum(key*key) from data settings workload='{workload}'")
        except QueryRuntimeException:
            pass

    thread1 = threading.Thread(target=read_query, args=["development"])
    thread2 = threading.Thread(target=read_query, args=["production"])
    thread3 = threading.Thread(target=read_query, args=["admin"])

    thread1.start()
    thread2.start()
    thread3.start()

    thread3.join()
    thread2.join()
    thread1.join()

    assert (
        node.query(
            f"select dequeued_requests>0 from system.scheduler where resource='network_read' and path='/prio/admin'"
        )
        == "1\n"
    )
    assert (
        node.query(
            f"select dequeued_requests>0 from system.scheduler where resource='network_read' and path='/prio/fair/dev'"
        )
        == "1\n"
    )
    assert (
        node.query(
            f"select dequeued_requests>0 from system.scheduler where resource='network_read' and path='/prio/fair/prod'"
        )
        == "1\n"
    )
