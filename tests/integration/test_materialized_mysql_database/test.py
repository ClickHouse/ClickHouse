import os
import logging
import pytest

from helpers.cluster import ClickHouseCluster, ClickHouseInstance
from . import materialized_with_ddl
from .utils import MySQLConnection, ReplicationHelper


cluster = ClickHouseCluster(__file__)
mysql_node = None
mysql8_node = None

node_db = cluster.add_instance(
    "node1",
    main_configs=["configs/config.xml", "configs/timezone_config.xml"],
    user_configs=["configs/users.xml"],
    with_mysql=True,
    with_mysql8=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        node_db.stop_clickhouse()  # ensures that coverage report is written to disk, even if cluster.shutdown() times out.
        cluster.shutdown()


@pytest.fixture(scope="module")
def started_mysql_5_7():
    mysql_node = MySQLConnection(
        cluster.mysql_port, "root", "clickhouse", cluster.mysql_ip
    )
    yield mysql_node


@pytest.fixture(scope="module")
def started_mysql_8_0():
    mysql8_node = MySQLConnection(
        cluster.mysql8_port, "root", "clickhouse", cluster.mysql8_ip
    )
    yield mysql8_node


@pytest.fixture(scope="module")
def clickhouse_node():
    yield node_db


@pytest.fixture(scope="function")
def replication(started_cluster, started_mysql_8_0, request):
    try:
        replication = ReplicationHelper(cluster, node_db, started_mysql_8_0)
        yield replication
    finally:
        if hasattr(request.session, "testsfailed") and request.session.testsfailed:
            logging.warning("tests failed - not dropping databases")
        else:
            # drop databases only if the test succeeds - so we can inspect the database after failed tests
            try:
                replication.drop_dbs()
            except Exception as e:
                logging.warning("replication.drop_dbs() failed: %s", str(e))


def test_materialized_database_dml_with_mysql_8_0(
    started_cluster, started_mysql_8_0, clickhouse_node
):
    materialized_with_ddl.dml_with_materialized_mysql_database(
        clickhouse_node, started_mysql_8_0, "mysql80"
    )
    materialized_with_ddl.materialized_mysql_database_with_views(
        clickhouse_node, started_mysql_8_0, "mysql80"
    )
    materialized_with_ddl.materialized_mysql_database_with_datetime_and_decimal(
        clickhouse_node, started_mysql_8_0, "mysql80"
    )
    materialized_with_ddl.move_to_prewhere_and_column_filtering(
        clickhouse_node, started_mysql_8_0, "mysql80"
    )


def test_materialized_database_ddl_with_mysql_5_7(
    started_cluster, started_mysql_5_7, clickhouse_node
):
    materialized_with_ddl.drop_table_with_materialized_mysql_database(
        clickhouse_node, started_mysql_5_7, "mysql57"
    )
    materialized_with_ddl.create_table_with_materialized_mysql_database(
        clickhouse_node, started_mysql_5_7, "mysql57"
    )
    materialized_with_ddl.rename_table_with_materialized_mysql_database(
        clickhouse_node, started_mysql_5_7, "mysql57"
    )
    materialized_with_ddl.alter_add_column_with_materialized_mysql_database(
        clickhouse_node, started_mysql_5_7, "mysql57"
    )
    materialized_with_ddl.alter_drop_column_with_materialized_mysql_database(
        clickhouse_node, started_mysql_5_7, "mysql57"
    )
    # mysql 5.7 cannot support alter rename column
    # materialized_with_ddl.alter_rename_column_with_materialized_mysql_database(clickhouse_node, started_mysql_5_7, "mysql57")
    materialized_with_ddl.alter_rename_table_with_materialized_mysql_database(
        clickhouse_node, started_mysql_5_7, "mysql57"
    )
    materialized_with_ddl.alter_modify_column_with_materialized_mysql_database(
        clickhouse_node, started_mysql_5_7, "mysql57"
    )
    materialized_with_ddl.create_table_like_with_materialized_mysql_database(
        clickhouse_node, started_mysql_5_7, "mysql57"
    )


def test_materialized_database_ddl_with_mysql_8_0(
    started_cluster, started_mysql_8_0, clickhouse_node
):
    materialized_with_ddl.drop_table_with_materialized_mysql_database(
        clickhouse_node, started_mysql_8_0, "mysql80"
    )
    materialized_with_ddl.create_table_with_materialized_mysql_database(
        clickhouse_node, started_mysql_8_0, "mysql80"
    )
    materialized_with_ddl.rename_table_with_materialized_mysql_database(
        clickhouse_node, started_mysql_8_0, "mysql80"
    )
    materialized_with_ddl.alter_add_column_with_materialized_mysql_database(
        clickhouse_node, started_mysql_8_0, "mysql80"
    )
    materialized_with_ddl.alter_drop_column_with_materialized_mysql_database(
        clickhouse_node, started_mysql_8_0, "mysql80"
    )
    materialized_with_ddl.alter_rename_table_with_materialized_mysql_database(
        clickhouse_node, started_mysql_8_0, "mysql80"
    )
    materialized_with_ddl.alter_rename_column_with_materialized_mysql_database(
        clickhouse_node, started_mysql_8_0, "mysql80"
    )
    materialized_with_ddl.alter_modify_column_with_materialized_mysql_database(
        clickhouse_node, started_mysql_8_0, "mysql80"
    )
    materialized_with_ddl.create_table_like_with_materialized_mysql_database(
        clickhouse_node, started_mysql_8_0, "mysql80"
    )


def test_materialized_database_ddl_with_empty_transaction_5_7(
    started_cluster, started_mysql_5_7, clickhouse_node
):
    materialized_with_ddl.query_event_with_empty_transaction(
        clickhouse_node, started_mysql_5_7, "mysql57"
    )


def test_materialized_database_ddl_with_empty_transaction_8_0(
    started_cluster, started_mysql_8_0, clickhouse_node
):
    materialized_with_ddl.query_event_with_empty_transaction(
        clickhouse_node, started_mysql_8_0, "mysql80"
    )


def test_text_blob_charset(started_cluster, started_mysql_8_0, clickhouse_node):
    materialized_with_ddl.text_blob_with_charset_test(
        clickhouse_node, started_mysql_8_0, "mysql80"
    )


def test_select_without_columns_5_7(
    started_cluster, started_mysql_5_7, clickhouse_node
):
    materialized_with_ddl.select_without_columns(
        clickhouse_node, started_mysql_5_7, "mysql57"
    )


def test_select_without_columns_8_0(
    started_cluster, started_mysql_8_0, clickhouse_node
):
    materialized_with_ddl.select_without_columns(
        clickhouse_node, started_mysql_8_0, "mysql80"
    )


def test_insert_with_modify_binlog_checksum_5_7(
    started_cluster, started_mysql_5_7, clickhouse_node
):
    materialized_with_ddl.insert_with_modify_binlog_checksum(
        clickhouse_node, started_mysql_5_7, "mysql57"
    )


def test_insert_with_modify_binlog_checksum_8_0(
    started_cluster, started_mysql_8_0, clickhouse_node
):
    materialized_with_ddl.insert_with_modify_binlog_checksum(
        clickhouse_node, started_mysql_8_0, "mysql80"
    )


def test_materialized_database_err_sync_user_privs_5_7(
    started_cluster, started_mysql_5_7, clickhouse_node
):
    materialized_with_ddl.err_sync_user_privs_with_materialized_mysql_database(
        clickhouse_node, started_mysql_5_7, "mysql57"
    )


def test_materialized_database_err_sync_user_privs_8_0(
    started_cluster, started_mysql_8_0, clickhouse_node
):
    materialized_with_ddl.err_sync_user_privs_with_materialized_mysql_database(
        clickhouse_node, started_mysql_8_0, "mysql80"
    )


def test_network_partition_5_7(started_cluster, started_mysql_5_7, clickhouse_node):
    materialized_with_ddl.network_partition_test(
        clickhouse_node, started_mysql_5_7, "mysql57"
    )


def test_network_partition_8_0(started_cluster, started_mysql_8_0, clickhouse_node):
    materialized_with_ddl.network_partition_test(
        clickhouse_node, started_mysql_8_0, "mysql80"
    )


def test_mysql_kill_sync_thread_restore_5_7(
    started_cluster, started_mysql_5_7, clickhouse_node
):
    materialized_with_ddl.mysql_kill_sync_thread_restore_test(
        clickhouse_node, started_mysql_5_7, "mysql57"
    )


def test_mysql_kill_sync_thread_restore_8_0(
    started_cluster, started_mysql_8_0, clickhouse_node
):
    materialized_with_ddl.mysql_kill_sync_thread_restore_test(
        clickhouse_node, started_mysql_8_0, "mysql80"
    )


def test_mysql_killed_while_insert_5_7(
    started_cluster, started_mysql_5_7, clickhouse_node
):
    materialized_with_ddl.mysql_killed_while_insert(
        clickhouse_node, started_mysql_5_7, "mysql57"
    )


def test_mysql_killed_while_insert_8_0(
    started_cluster, started_mysql_8_0, clickhouse_node
):
    materialized_with_ddl.mysql_killed_while_insert(
        clickhouse_node, started_mysql_8_0, "mysql80"
    )


def test_clickhouse_killed_while_insert_5_7(
    started_cluster, started_mysql_5_7, clickhouse_node
):
    materialized_with_ddl.clickhouse_killed_while_insert(
        clickhouse_node, started_mysql_5_7, "mysql57"
    )


def test_clickhouse_killed_while_insert_8_0(
    started_cluster, started_mysql_8_0, clickhouse_node
):
    materialized_with_ddl.clickhouse_killed_while_insert(
        clickhouse_node, started_mysql_8_0, "mysql80"
    )


def test_utf8mb4(
    started_cluster, started_mysql_8_0, started_mysql_5_7, clickhouse_node
):
    materialized_with_ddl.utf8mb4_test(clickhouse_node, started_mysql_5_7, "mysql57")
    materialized_with_ddl.utf8mb4_test(clickhouse_node, started_mysql_8_0, "mysql80")
    materialized_with_ddl.utf8mb4_column_test(
        clickhouse_node, started_mysql_8_0, "mysql80"
    )
    materialized_with_ddl.utf8mb4_name_test(
        clickhouse_node, started_mysql_8_0, "mysql80"
    )


def test_system_parts_table(started_cluster, started_mysql_8_0, clickhouse_node):
    materialized_with_ddl.system_parts_test(
        clickhouse_node, started_mysql_8_0, "mysql80"
    )


def test_multi_table_update(
    started_cluster, started_mysql_8_0, started_mysql_5_7, clickhouse_node
):
    materialized_with_ddl.multi_table_update_test(
        clickhouse_node, started_mysql_5_7, "mysql57"
    )
    materialized_with_ddl.multi_table_update_test(
        clickhouse_node, started_mysql_8_0, "mysql80"
    )


def test_system_tables_table(
    started_cluster, started_mysql_8_0, started_mysql_5_7, clickhouse_node
):
    materialized_with_ddl.system_tables_test(
        clickhouse_node, started_mysql_5_7, "mysql57"
    )
    materialized_with_ddl.system_tables_test(
        clickhouse_node, started_mysql_8_0, "mysql80"
    )


def test_materialized_with_column_comments(
    started_cluster, started_mysql_8_0, started_mysql_5_7, clickhouse_node
):
    materialized_with_ddl.materialized_with_column_comments_test(
        clickhouse_node, started_mysql_5_7, "mysql57"
    )
    materialized_with_ddl.materialized_with_column_comments_test(
        clickhouse_node, started_mysql_8_0, "mysql80"
    )


def test_double_quoted_comment(started_cluster, started_mysql_8_0, clickhouse_node):
    materialized_with_ddl.double_quoted_comment(
        clickhouse_node, started_mysql_8_0, "mysql80"
    )


def test_default_values(started_cluster, started_mysql_8_0, clickhouse_node):
    materialized_with_ddl.default_values(clickhouse_node, started_mysql_8_0, "mysql80")


def test_materialized_with_enum(
    started_cluster, started_mysql_8_0, started_mysql_5_7, clickhouse_node
):
    materialized_with_ddl.materialized_with_enum8_test(
        clickhouse_node, started_mysql_5_7, "mysql57"
    )
    materialized_with_ddl.materialized_with_enum16_test(
        clickhouse_node, started_mysql_5_7, "mysql57"
    )
    materialized_with_ddl.alter_enum8_to_enum16_test(
        clickhouse_node, started_mysql_5_7, "mysql57"
    )
    materialized_with_ddl.materialized_with_enum8_test(
        clickhouse_node, started_mysql_8_0, "mysql80"
    )
    materialized_with_ddl.materialized_with_enum16_test(
        clickhouse_node, started_mysql_8_0, "mysql80"
    )
    materialized_with_ddl.alter_enum8_to_enum16_test(
        clickhouse_node, started_mysql_8_0, "mysql80"
    )


@pytest.mark.parametrize(
    "user",
    [
        pytest.param("default", id="default"),
        pytest.param("disable_bytes_settings", id="disable_bytes_settings"),
        pytest.param("disable_rows_settings", id="disable_rows_settings"),
    ],
)
def test_mysql_settings(
    started_cluster, started_mysql_8_0, started_mysql_5_7, clickhouse_node, user
):
    materialized_with_ddl.mysql_settings_test(
        clickhouse_node, started_mysql_5_7, "mysql57", user
    )
    materialized_with_ddl.mysql_settings_test(
        clickhouse_node, started_mysql_8_0, "mysql80", user
    )


def test_large_transaction(
    started_cluster, started_mysql_8_0, started_mysql_5_7, clickhouse_node
):
    materialized_with_ddl.materialized_mysql_large_transaction(
        clickhouse_node, started_mysql_8_0, "mysql80"
    )
    materialized_with_ddl.materialized_mysql_large_transaction(
        clickhouse_node, started_mysql_5_7, "mysql57"
    )


def test_table_table(
    started_cluster, started_mysql_8_0, started_mysql_5_7, clickhouse_node
):
    materialized_with_ddl.table_table(clickhouse_node, started_mysql_8_0, "mysql80")
    materialized_with_ddl.table_table(clickhouse_node, started_mysql_5_7, "mysql57")


def test_table_overrides(
    started_cluster, started_mysql_8_0, started_mysql_5_7, clickhouse_node
):
    materialized_with_ddl.table_overrides(clickhouse_node, started_mysql_5_7, "mysql57")
    materialized_with_ddl.table_overrides(clickhouse_node, started_mysql_8_0, "mysql80")


def test_materialized_database_support_all_kinds_of_mysql_datatype(
    started_cluster, started_mysql_8_0, started_mysql_5_7, clickhouse_node
):
    materialized_with_ddl.materialized_database_support_all_kinds_of_mysql_datatype(
        clickhouse_node, started_mysql_8_0, "mysql80"
    )
    materialized_with_ddl.materialized_database_support_all_kinds_of_mysql_datatype(
        clickhouse_node, started_mysql_5_7, "mysql57"
    )


def test_materialized_database_settings_materialized_mysql_tables_list(
    started_cluster, started_mysql_8_0, started_mysql_5_7, clickhouse_node
):
    materialized_with_ddl.materialized_database_settings_materialized_mysql_tables_list(
        clickhouse_node, started_mysql_8_0, "mysql80"
    )
    materialized_with_ddl.materialized_database_settings_materialized_mysql_tables_list(
        clickhouse_node, started_mysql_5_7, "mysql57"
    )


def test_materialized_database_mysql_date_type_to_date32(
    started_cluster, started_mysql_8_0, started_mysql_5_7, clickhouse_node
):
    materialized_with_ddl.materialized_database_mysql_date_type_to_date32(
        clickhouse_node, started_mysql_8_0, "mysql80"
    )
    materialized_with_ddl.materialized_database_mysql_date_type_to_date32(
        clickhouse_node, started_mysql_5_7, "mysql57"
    )


def test_savepoint_query(
    started_cluster, started_mysql_8_0, started_mysql_5_7, clickhouse_node
):
    materialized_with_ddl.savepoint(clickhouse_node, started_mysql_8_0, "mysql80")
    materialized_with_ddl.savepoint(clickhouse_node, started_mysql_5_7, "mysql57")


def test_materialized_database_mysql_drop_ddl(
    started_cluster, started_mysql_8_0, started_mysql_5_7, clickhouse_node
):
    materialized_with_ddl.dropddl(clickhouse_node, started_mysql_8_0, "mysql80")
    materialized_with_ddl.dropddl(clickhouse_node, started_mysql_5_7, "mysql57")


def test_named_collections(started_cluster, started_mysql_8_0, clickhouse_node):
    materialized_with_ddl.named_collections(
        clickhouse_node, started_mysql_8_0, "mysql80"
    )


def test_create_table_as_select(started_cluster, started_mysql_8_0, clickhouse_node):
    materialized_with_ddl.create_table_as_select(
        clickhouse_node, started_mysql_8_0, "mysql80"
    )


def test_table_with_indexes(started_cluster, started_mysql_8_0, clickhouse_node):
    materialized_with_ddl.table_with_indexes(
        clickhouse_node, started_mysql_8_0, "mysql80"
    )


def test_alter_database_table_overrides(replication):
    materialized_with_ddl.alter_database_table_overrides(replication)


def test_alter_database_settings(replication):
    materialized_with_ddl.alter_database_settings_test(replication)
