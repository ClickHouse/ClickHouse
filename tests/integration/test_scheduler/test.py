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


def check_profile_event_for_query(workload, profile_event, amount=1):
    node.query("system flush logs")
    query_pattern = f"workload='{workload}'".replace("'", "\\'")
    assert (
        int(
            node.query(
                f"select ProfileEvents['{profile_event}'] from system.query_log where query ilike '%{query_pattern}%' and type = 'QueryFinish' order by query_start_time_microseconds desc limit 1"
            )
        )
        == amount
    )


def test_s3_resource_request_granularity():
    node.query(
        f"""
        drop table if exists data;
        create table data (key UInt64 CODEC(NONE), value String CODEC(NONE)) engine=MergeTree() order by key settings min_bytes_for_wide_part=1e9, storage_policy='s3';
    """
    )

    total_bytes = 50000000 # Approximate data size
    max_bytes_per_request = 2000000 # Should be ~1MB or less in general
    min_bytes_per_request = 6000 # Small requests are ok, but we don't want hurt performance with too often resource requests

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
    write_budget_before = int(
        node.query(
            f"select budget from system.scheduler where resource='network_write' and path='/prio/admin'"
        ).strip()
    )
    node.query(f"insert into data select number, randomString(10000000) from numbers(5) SETTINGS workload='admin'")
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
    write_budget_after = int(
        node.query(
            f"select budget from system.scheduler where resource='network_write' and path='/prio/admin'"
        ).strip()
    )

    write_requests = writes_after - writes_before
    write_bytes = (write_bytes_after - write_bytes_before) - (write_budget_after - write_budget_before)
    assert write_bytes > 1.0 * total_bytes
    assert write_bytes < 1.05 * total_bytes
    assert write_bytes / write_requests < max_bytes_per_request
    assert write_bytes / write_requests > min_bytes_per_request
    check_profile_event_for_query("admin", "SchedulerIOWriteRequests", write_requests)
    check_profile_event_for_query("admin", "SchedulerIOWriteBytes", write_bytes)

    node.query(f"optimize table data final")

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
    read_budget_before = int(
        node.query(
            f"select budget from system.scheduler where resource='network_read' and path='/prio/admin'"
        ).strip()
    )
    node.query(f"select count() from data where not ignore(*) SETTINGS workload='admin'")
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
    read_budget_after = int(
        node.query(
            f"select budget from system.scheduler where resource='network_read' and path='/prio/admin'"
        ).strip()
    )

    read_bytes = (read_bytes_after - read_bytes_before) - (read_budget_after - read_budget_before)
    read_requests = reads_after - reads_before
    assert read_bytes > 1.0 * total_bytes
    assert read_bytes < 1.05 * total_bytes
    assert read_bytes / read_requests < max_bytes_per_request
    assert read_bytes / read_requests > min_bytes_per_request
    check_profile_event_for_query("admin", "SchedulerIOReadRequests", read_requests)
    check_profile_event_for_query("admin", "SchedulerIOReadBytes", read_bytes)


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
