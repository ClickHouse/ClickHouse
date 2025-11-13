import datetime
import logging
import random
import string
import time
from random import randint

import pytest

from helpers.cluster import ClickHouseCluster, QueryRuntimeException
from helpers.network import PartitionManager
from helpers.test_tools import TSV, assert_eq_with_retry, assert_logs_contain

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.xml"],
    user_configs=["configs/users.xml"],
    with_zookeeper=True,
    with_minio=True,
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
    macros={"shard": "shard1", "replica": "1"},
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.xml"],
    user_configs=["configs/users.xml"],
    with_zookeeper=True,
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
    macros={"shard": "shard1", "replica": "2"},
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def count_skip_ddls(node, db_name, since_ts):
    node.query("SYSTEM FLUSH LOGS")
    result = node.query(
        f"SELECT count() FROM system.text_log WHERE logger_name='DDLWorker({db_name})' AND position(message, 'Skip DDL query') > 0 AND event_time_microseconds > '{since_ts}'"
    ).strip()
    return int(result)

def get_random_string(string_length=8):
    alphabet = string.ascii_letters + string.digits
    return "".join((random.choice(alphabet) for _ in range(string_length)))

def get_last_ddl_worker_log_ts(node, db_name):
    node.query("SYSTEM FLUSH LOGS")
    result = node.query(
        f"SELECT max(event_time_microseconds) FROM system.text_log WHERE logger_name='DDLWorker({db_name})' "
    ).strip()

    if len(result) == 0:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return result


@pytest.mark.parametrize("append", [True, False])
@pytest.mark.parametrize("with_inner_table", [True, False])
@pytest.mark.parametrize("allow_skipping", [1, 0])
def test_refreshable_mv_skip_old_temp_tables_ddls(
    started_cluster,
    append: bool,
    with_inner_table: bool,
    allow_skipping: int,
):
    db_name = "test_" + get_random_string()
    node1.query(f"DROP DATABASE IF EXISTS {db_name} SYNC")
    node2.query(f"DROP DATABASE IF EXISTS {db_name}  SYNC")
    # Make sure that the MV is refreshed on node1
    node2.query("SYSTEM ENABLE FAILPOINT refresh_task_stop_racing_for_running_refresh")
    node2.query("SYSTEM ENABLE FAILPOINT database_replicated_delay_entry_execution")

    node1.query(
        f"CREATE DATABASE {db_name} ENGINE=Replicated('/test/{db_name}', "
        + r"'{shard}', '{replica}') "
        + f" SETTINGS allow_skipping_old_temporary_tables_ddls_of_refreshable_materialized_views={allow_skipping}"
    )

    append_clause = "APPEND" if append else ""

    if(with_inner_table):
        node1.query(
            f"CREATE TABLE {db_name}.target (x DateTime) ENGINE ReplicatedMergeTree ORDER BY x"
        )
        node1.query(
            f"CREATE MATERIALIZED VIEW {db_name}.mv REFRESH EVERY 1 HOUR {append_clause} TO {db_name}.target AS SELECT now() AS x"
        )
    else:
        node1.query(
            f"CREATE MATERIALIZED VIEW {db_name}.mv REFRESH EVERY 1 HOUR {append_clause} (x DateTime) ENGINE ReplicatedMergeTree ORDER BY x AS SELECT now() AS x"
        )

    node2.query(
        f"CREATE DATABASE {db_name} ENGINE=Replicated('/test/{db_name}', "
        + r"'{shard}', '{replica}')"
        + f" SETTINGS allow_skipping_old_temporary_tables_ddls_of_refreshable_materialized_views={allow_skipping}"
    )

    # Make sure that tables are replicated on node2
    assert (
        node2.query_with_retry(
            f"SELECT count() FROM system.tables WHERE database='{db_name}'",
            check_callback=lambda x: x.strip() == "2",
        ).strip()
        == "2"
    )

    last_log_ts = get_last_ddl_worker_log_ts(node2, db_name)

    last_refresh_time = node1.query(
        "SELECT last_refresh_time FROM system.view_refreshes WHERE view='mv'"
    )
    for i in range(2):
        node1.query(f"SYSTEM REFRESH VIEW {db_name}.mv")
    # Ensure that the mv is refresh
    node1.query_with_retry(
        "SELECT last_refresh_time FROM system.view_refreshes WHERE view='mv'",
        check_callback=lambda x: x != last_refresh_time,
    )

    # Make sure that the view is not refreshing, and it is scheduled to be refreshed in at least 10 minutes
    node1.query_with_retry(
        f"SELECT next_refresh_time FROM system.view_refreshes WHERE view='mv' AND database='{db_name}'",
        check_callback=lambda x: datetime.now()
        - datetime.strptime(x.strip(), "%Y-%m-%d %H:%M:%S")
        > datetime.timedelta(minutes=10),
    )

    node2.query("SYSTEM DISABLE FAILPOINT refresh_task_stop_racing_for_running_refresh")
    node2.query("SYSTEM DISABLE FAILPOINT database_replicated_delay_entry_execution")

    table_info1 = node1.query(f"SELECT uuid, name FROM system.tables WHERE database='{db_name}'")
    assert table_info1 == node2.query_with_retry(
        f"SELECT uuid, name FROM system.tables WHERE database='{db_name}'",
        check_callback=lambda x: x == table_info1,
    )
    data1 = TSV(node1.query(f"SELECT x FROM {db_name}.mv ORDER BY x"))
    data2 = TSV(node2.query(f"SELECT x FROM {db_name}.mv ORDER BY x"))
    assert data1 == data2

    if append or allow_skipping == 0:
        assert count_skip_ddls(node2, db_name, last_log_ts) == 0
    else:
        assert count_skip_ddls(node2, db_name, last_log_ts) > 0

    node1.query(f"DROP DATABASE IF EXISTS {db_name} SYNC")
    node2.query(f"DROP DATABASE IF EXISTS {db_name} SYNC")
