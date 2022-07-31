# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

instance = cluster.add_instance(
    "instance",
    main_configs=[
        "configs/conf.xml",
        "configs/asynchronous_metrics_update_period_s.xml",
    ],
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


# max_memory_usage_for_user cannot be used, since the memory for user accounted
# correctly, only total is not (it is set via conf.xml)
def test_memory_tracking_total():
    instance.query("CREATE TABLE null (row String) ENGINE=Null")
    instance.exec_in_container(
        [
            "bash",
            "-c",
            "clickhouse local -q \"SELECT arrayStringConcat(arrayMap(x->toString(cityHash64(x)), range(1000)), ' ') from numbers(10000)\" > data.json",
        ]
    )
    for it in range(0, 20):
        # the problem can be triggered only via HTTP,
        # since clickhouse-client parses the data by itself.
        assert (
            instance.exec_in_container(
                [
                    "curl",
                    "--silent",
                    "--show-error",
                    "--data-binary",
                    "@data.json",
                    "http://127.1:8123/?query=INSERT%20INTO%20null%20FORMAT%20TSV",
                ]
            )
            == ""
        ), f"Failed on {it} iteration"
