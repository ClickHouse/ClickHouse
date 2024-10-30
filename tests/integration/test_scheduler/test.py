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
    main_configs=[
        "configs/storage_configuration.xml",
        "configs/resources.xml",
        "configs/resources.xml.default",
        "configs/workloads.xml",
        "configs/workloads.xml.default",
    ],
    with_minio=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield
    finally:
        cluster.shutdown()


@pytest.fixture(scope="function", autouse=True)
def set_default_configs():
    node.exec_in_container(
        [
            "bash",
            "-c",
            "cp /etc/clickhouse-server/config.d/resources.xml.default /etc/clickhouse-server/config.d/resources.xml",
        ]
    )
    node.exec_in_container(
        [
            "bash",
            "-c",
            "cp /etc/clickhouse-server/config.d/workloads.xml.default /etc/clickhouse-server/config.d/workloads.xml",
        ]
    )
    node.query("system reload config")
    yield


def update_workloads_config(**settings):
    xml = ""
    for name in settings:
        xml += f"<{name}>{settings[name]}</{name}>"
    print(xml)
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"echo '<clickhouse>{xml}</clickhouse>' > /etc/clickhouse-server/config.d/workloads.xml",
        ]
    )
    node.query("system reload config")


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


def test_merge_workload():
    node.query(
        f"""
        drop table if exists data;
        create table data (key UInt64 CODEC(NONE)) engine=MergeTree() order by tuple() settings min_bytes_for_wide_part=1e9, storage_policy='s3';
    """
    )

    reads_before = int(
        node.query(
            f"select dequeued_requests from system.scheduler where resource='network_read' and path='/prio/fair/sys/merges'"
        ).strip()
    )
    writes_before = int(
        node.query(
            f"select dequeued_requests from system.scheduler where resource='network_write' and path='/prio/fair/sys/merges'"
        ).strip()
    )

    node.query(f"insert into data select * from numbers(1e4)")
    node.query(f"insert into data select * from numbers(2e4)")
    node.query(f"insert into data select * from numbers(3e4)")
    node.query(f"optimize table data final")

    reads_after = int(
        node.query(
            f"select dequeued_requests from system.scheduler where resource='network_read' and path='/prio/fair/sys/merges'"
        ).strip()
    )
    writes_after = int(
        node.query(
            f"select dequeued_requests from system.scheduler where resource='network_write' and path='/prio/fair/sys/merges'"
        ).strip()
    )

    assert reads_before < reads_after
    assert writes_before < writes_after


def test_merge_workload_override():
    node.query(
        f"""
        drop table if exists prod_data;
        drop table if exists dev_data;
        create table prod_data (key UInt64 CODEC(NONE)) engine=MergeTree() order by tuple() settings min_bytes_for_wide_part=1e9, storage_policy='s3', merge_workload='prod_merges';
        create table dev_data (key UInt64 CODEC(NONE)) engine=MergeTree() order by tuple() settings min_bytes_for_wide_part=1e9, storage_policy='s3', merge_workload='dev_merges';
    """
    )

    prod_reads_before = int(
        node.query(
            f"select dequeued_requests from system.scheduler where resource='network_read' and path='/prio/fair/prod_merges'"
        ).strip()
    )
    prod_writes_before = int(
        node.query(
            f"select dequeued_requests from system.scheduler where resource='network_write' and path='/prio/fair/prod_merges'"
        ).strip()
    )
    dev_reads_before = int(
        node.query(
            f"select dequeued_requests from system.scheduler where resource='network_read' and path='/prio/fair/dev_merges'"
        ).strip()
    )
    dev_writes_before = int(
        node.query(
            f"select dequeued_requests from system.scheduler where resource='network_write' and path='/prio/fair/dev_merges'"
        ).strip()
    )

    node.query(f"insert into prod_data select * from numbers(1e4)")
    node.query(f"insert into prod_data select * from numbers(2e4)")
    node.query(f"insert into prod_data select * from numbers(3e4)")
    node.query(f"insert into dev_data select * from numbers(1e4)")
    node.query(f"insert into dev_data select * from numbers(2e4)")
    node.query(f"insert into dev_data select * from numbers(3e4)")
    node.query(f"optimize table prod_data final")
    node.query(f"optimize table dev_data final")

    prod_reads_after = int(
        node.query(
            f"select dequeued_requests from system.scheduler where resource='network_read' and path='/prio/fair/prod_merges'"
        ).strip()
    )
    prod_writes_after = int(
        node.query(
            f"select dequeued_requests from system.scheduler where resource='network_write' and path='/prio/fair/prod_merges'"
        ).strip()
    )
    dev_reads_after = int(
        node.query(
            f"select dequeued_requests from system.scheduler where resource='network_read' and path='/prio/fair/dev_merges'"
        ).strip()
    )
    dev_writes_after = int(
        node.query(
            f"select dequeued_requests from system.scheduler where resource='network_write' and path='/prio/fair/dev_merges'"
        ).strip()
    )

    assert prod_reads_before < prod_reads_after
    assert prod_writes_before < prod_writes_after
    assert dev_reads_before < dev_reads_after
    assert dev_writes_before < dev_writes_after


def test_mutate_workload():
    node.query(
        f"""
        drop table if exists data;
        create table data (key UInt64 CODEC(NONE)) engine=MergeTree() order by tuple() settings min_bytes_for_wide_part=1e9, storage_policy='s3';
    """
    )

    node.query(f"insert into data select * from numbers(1e4)")
    node.query(f"optimize table data final")

    reads_before = int(
        node.query(
            f"select dequeued_requests from system.scheduler where resource='network_read' and path='/prio/fair/sys/mutations'"
        ).strip()
    )
    writes_before = int(
        node.query(
            f"select dequeued_requests from system.scheduler where resource='network_write' and path='/prio/fair/sys/mutations'"
        ).strip()
    )

    node.query(f"alter table data update key = 1 where key = 42")
    node.query(f"optimize table data final")

    reads_after = int(
        node.query(
            f"select dequeued_requests from system.scheduler where resource='network_read' and path='/prio/fair/sys/mutations'"
        ).strip()
    )
    writes_after = int(
        node.query(
            f"select dequeued_requests from system.scheduler where resource='network_write' and path='/prio/fair/sys/mutations'"
        ).strip()
    )

    assert reads_before < reads_after
    assert writes_before < writes_after


def test_mutation_workload_override():
    node.query(
        f"""
        drop table if exists prod_data;
        drop table if exists dev_data;
        create table prod_data (key UInt64 CODEC(NONE)) engine=MergeTree() order by tuple() settings min_bytes_for_wide_part=1e9, storage_policy='s3', mutation_workload='prod_mutations';
        create table dev_data (key UInt64 CODEC(NONE)) engine=MergeTree() order by tuple() settings min_bytes_for_wide_part=1e9, storage_policy='s3', mutation_workload='dev_mutations';
    """
    )

    node.query(f"insert into prod_data select * from numbers(1e4)")
    node.query(f"optimize table prod_data final")
    node.query(f"insert into dev_data select * from numbers(1e4)")
    node.query(f"optimize table dev_data final")

    prod_reads_before = int(
        node.query(
            f"select dequeued_requests from system.scheduler where resource='network_read' and path='/prio/fair/prod_mutations'"
        ).strip()
    )
    prod_writes_before = int(
        node.query(
            f"select dequeued_requests from system.scheduler where resource='network_write' and path='/prio/fair/prod_mutations'"
        ).strip()
    )
    dev_reads_before = int(
        node.query(
            f"select dequeued_requests from system.scheduler where resource='network_read' and path='/prio/fair/dev_mutations'"
        ).strip()
    )
    dev_writes_before = int(
        node.query(
            f"select dequeued_requests from system.scheduler where resource='network_write' and path='/prio/fair/dev_mutations'"
        ).strip()
    )

    node.query(f"alter table prod_data update key = 1 where key = 42")
    node.query(f"optimize table prod_data final")
    node.query(f"alter table dev_data update key = 1 where key = 42")
    node.query(f"optimize table dev_data final")

    prod_reads_after = int(
        node.query(
            f"select dequeued_requests from system.scheduler where resource='network_read' and path='/prio/fair/prod_mutations'"
        ).strip()
    )
    prod_writes_after = int(
        node.query(
            f"select dequeued_requests from system.scheduler where resource='network_write' and path='/prio/fair/prod_mutations'"
        ).strip()
    )
    dev_reads_after = int(
        node.query(
            f"select dequeued_requests from system.scheduler where resource='network_read' and path='/prio/fair/dev_mutations'"
        ).strip()
    )
    dev_writes_after = int(
        node.query(
            f"select dequeued_requests from system.scheduler where resource='network_write' and path='/prio/fair/dev_mutations'"
        ).strip()
    )

    assert prod_reads_before < prod_reads_after
    assert prod_writes_before < prod_writes_after
    assert dev_reads_before < dev_reads_after
    assert dev_writes_before < dev_writes_after


def test_merge_workload_change():
    node.query(
        f"""
        drop table if exists data;
        create table data (key UInt64 CODEC(NONE)) engine=MergeTree() order by tuple() settings min_bytes_for_wide_part=1e9, storage_policy='s3';
    """
    )

    for env in ["prod", "dev"]:
        update_workloads_config(merge_workload=f"{env}_merges")

        reads_before = int(
            node.query(
                f"select dequeued_requests from system.scheduler where resource='network_read' and path='/prio/fair/{env}_merges'"
            ).strip()
        )
        writes_before = int(
            node.query(
                f"select dequeued_requests from system.scheduler where resource='network_write' and path='/prio/fair/{env}_merges'"
            ).strip()
        )

        node.query(f"insert into data select * from numbers(1e4)")
        node.query(f"insert into data select * from numbers(2e4)")
        node.query(f"insert into data select * from numbers(3e4)")
        node.query(f"optimize table data final")

        reads_after = int(
            node.query(
                f"select dequeued_requests from system.scheduler where resource='network_read' and path='/prio/fair/{env}_merges'"
            ).strip()
        )
        writes_after = int(
            node.query(
                f"select dequeued_requests from system.scheduler where resource='network_write' and path='/prio/fair/{env}_merges'"
            ).strip()
        )

        assert reads_before < reads_after
        assert writes_before < writes_after


def test_mutation_workload_change():
    node.query(
        f"""
        drop table if exists data;
        create table data (key UInt64 CODEC(NONE)) engine=MergeTree() order by tuple() settings min_bytes_for_wide_part=1e9, storage_policy='s3';
    """
    )

    for env in ["prod", "dev"]:
        update_workloads_config(mutation_workload=f"{env}_mutations")

        node.query(f"insert into data select * from numbers(1e4)")
        node.query(f"optimize table data final")

        reads_before = int(
            node.query(
                f"select dequeued_requests from system.scheduler where resource='network_read' and path='/prio/fair/{env}_mutations'"
            ).strip()
        )
        writes_before = int(
            node.query(
                f"select dequeued_requests from system.scheduler where resource='network_write' and path='/prio/fair/{env}_mutations'"
            ).strip()
        )

        node.query(f"alter table data update key = 1 where key = 42")
        node.query(f"optimize table data final")

        reads_after = int(
            node.query(
                f"select dequeued_requests from system.scheduler where resource='network_read' and path='/prio/fair/{env}_mutations'"
            ).strip()
        )
        writes_after = int(
            node.query(
                f"select dequeued_requests from system.scheduler where resource='network_write' and path='/prio/fair/{env}_mutations'"
            ).strip()
        )

        assert reads_before < reads_after
        assert writes_before < writes_after
