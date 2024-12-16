import logging
import os
import time

import pymysql.cursors
import pytest

from helpers.cluster import (
    ClickHouseCluster,
    ClickHouseInstance,
    get_docker_compose_path,
    is_arm,
)

from . import materialized_with_ddl

DOCKER_COMPOSE_PATH = get_docker_compose_path()

# skip all test on arm due to no arm support in mysql57
if is_arm():
    pytestmark = pytest.mark.skip

cluster = ClickHouseCluster(__file__)
mysql_node = None
mysql8_node = None

node_db = cluster.add_instance(
    "node1",
    main_configs=["configs/timezone_config.xml", "configs/no_async_load.xml"],
    user_configs=["configs/users.xml"],
    with_mysql57=True,
    with_mysql8=True,
    stay_alive=True,
)
node_disable_bytes_settings = cluster.add_instance(
    "node2",
    main_configs=["configs/timezone_config.xml", "configs/no_async_load.xml"],
    user_configs=["configs/users_disable_bytes_settings.xml"],
    with_mysql57=False,
    with_mysql8=False,
    stay_alive=True,
)
node_disable_rows_settings = cluster.add_instance(
    "node3",
    main_configs=["configs/timezone_config.xml", "configs/no_async_load.xml"],
    user_configs=["configs/users_disable_rows_settings.xml"],
    with_mysql57=False,
    with_mysql8=False,
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


class MySQLConnection:
    def __init__(
        self,
        port,
        user="root",
        password="clickhouse",
        ip_address=None,
    ):
        self.user = user
        self.port = port
        self.ip_address = ip_address
        self.password = password
        self.mysql_connection = None  # lazy init

    def alloc_connection(self):
        errors = []
        for _ in range(5):
            try:
                if self.mysql_connection is None:
                    self.mysql_connection = pymysql.connect(
                        user=self.user,
                        password=self.password,
                        host=self.ip_address,
                        port=self.port,
                        autocommit=True,
                    )
                else:
                    self.mysql_connection.ping(reconnect=True)
                logging.debug(
                    "MySQL Connection established: {}:{}".format(
                        self.ip_address, self.port
                    )
                )
                return self.mysql_connection
            except Exception as e:
                errors += [str(e)]
                time.sleep(1)
        raise Exception("Connection not established, {}".format(errors))

    def query(self, execution_query):
        with self.alloc_connection().cursor() as cursor:
            cursor.execute(execution_query)

    def create_min_priv_user(self, user, password):
        self.query("CREATE USER '" + user + "'@'%' IDENTIFIED BY '" + password + "'")
        self.grant_min_priv_for_user(user)

    def grant_min_priv_for_user(self, user, db="priv_err_db"):
        self.query(
            "GRANT REPLICATION SLAVE, REPLICATION CLIENT, RELOAD ON *.* TO '"
            + user
            + "'@'%'"
        )
        self.query("GRANT SELECT ON " + db + ".* TO '" + user + "'@'%'")

    def result(self, execution_query):
        with self.alloc_connection().cursor() as cursor:
            result = cursor.execute(execution_query)
            if result is not None:
                print(cursor.fetchall())

    def query_and_get_data(self, execution_query):
        with self.alloc_connection().cursor() as cursor:
            cursor.execute(execution_query)
            return cursor.fetchall()

    def close(self):
        if self.mysql_connection is not None:
            self.mysql_connection.close()


@pytest.fixture(scope="module")
def started_mysql_5_7():
    mysql_node = MySQLConnection(
        cluster.mysql57_port, "root", "clickhouse", cluster.mysql57_ip
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


class ReplicationHelper:
    def __init__(self, clickhouse, mysql, mysql_host=None):
        self.clickhouse = clickhouse
        self.mysql = mysql
        self.created_mysql_dbs = []
        self.created_clickhouse_dbs = []
        self.base_mysql_settings = os.getenv("TEST_BASE_MYSQL_SETTINGS", "")
        self.base_ch_settings = os.getenv("TEST_BASE_CH_SETTINGS", "")
        self.mysql_host = mysql_host if mysql_host is not None else cluster.mysql8_host
        self.created_insert_procedures = {}
        self.inserted_rows_per_sp = {}
        self.inserted_rows = 0

    def create_dbs(self, db_name, ch_settings="", mysql_settings=""):
        self.create_db_mysql(db_name, settings=mysql_settings)
        self.create_db_ch(db_name, settings=ch_settings)

    def create_db_mysql(self, db_name, settings=""):
        self.mysql.query(f"DROP DATABASE IF EXISTS {db_name}")
        self.mysql.query(
            f"CREATE DATABASE {db_name} {self.base_mysql_settings} {settings}"
        )
        self.created_mysql_dbs.append(db_name)

    def create_db_ch(
        self, db_name, from_mysql_db=None, settings="", table_overrides=""
    ):
        if from_mysql_db is None:
            from_mysql_db = db_name
        self.clickhouse.query(f"DROP DATABASE IF EXISTS {db_name}")
        all_settings = ""
        create_query = f"CREATE DATABASE {db_name} ENGINE = MaterializedMySQL('{self.mysql_host}:3306', '{from_mysql_db}', 'root', 'clickhouse')"
        if self.base_ch_settings or settings:
            separator = ", " if self.base_ch_settings and settings else ""
            create_query += f" SETTINGS {self.base_ch_settings}{separator}{settings}"
        if table_overrides:
            create_query += f" {table_overrides}"
        self.clickhouse.query(create_query)
        self.created_clickhouse_dbs.append(db_name)

    def drop_dbs_mysql(self):
        for db_name in self.created_mysql_dbs:
            self.mysql.query(f"DROP DATABASE IF EXISTS {db_name}")
        self.created_mysql_dbs = []
        self.created_insert_procedures = {}
        self.inserted_rows_per_sp = {}
        self.inserted_rows = 0

    def drop_dbs_ch(self):
        for db_name in self.created_clickhouse_dbs:
            self.clickhouse.query(f"DROP DATABASE IF EXISTS {db_name}")
        self.created_clickhouse_dbs = []

    def drop_dbs(self):
        self.drop_dbs_mysql()
        self.drop_dbs_ch()

    def create_stored_procedure(self, db, table, column):
        sp_id = f"{db}_{table}_{column}"
        if sp_id in self.created_insert_procedures:
            return sp_id
        self.mysql.query(f"DROP PROCEDURE IF EXISTS {db}.insert_test_data_{sp_id}")
        self.mysql.query(
            f"""
CREATE PROCEDURE {db}.insert_test_data_{sp_id}(IN num_rows INT, IN existing_rows INT)
BEGIN
    DECLARE i INT;
    SET i = existing_rows;
    SET @insert = concat("INSERT INTO {table} ({column}) VALUES ");
    SET @exedata = "";
    WHILE i < (num_rows + existing_rows) DO
        SET @exedata=concat(@exedata, ",(", i , ")");
        SET i = i + 1;
        IF i % 1000 = 0
        THEN
            SET @exedata = SUBSTRING(@exedata, 2);
            SET @exesql = concat(@insert, @exedata);
            PREPARE stmt FROM @exesql;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;
            SET @exedata = "";
        END IF;
    END WHILE;
    IF length(@exedata) > 0
    THEN
        SET @exedata = SUBSTRING(@exedata, 2);
        SET @exesql = concat(@insert, @exedata);
        PREPARE stmt FROM @exesql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END IF;
END"""
        )
        self.created_insert_procedures[sp_id] = True
        self.inserted_rows_per_sp[sp_id] = 0
        return sp_id

    def insert_data(self, db, table, num_rows, column="id"):
        """Inserts num_rows into db.table, into the column `column` (which must be INT)"""
        sp_id = self.create_stored_procedure(db, table, column)
        self.mysql.query(
            f"CALL {db}.insert_test_data_{sp_id}({num_rows}, {self.inserted_rows_per_sp[sp_id]})"
        )
        self.inserted_rows_per_sp[sp_id] += num_rows
        self.inserted_rows += num_rows

    def wait_for_sync_to_catch_up(
        self, database: str = "", retry_count=30, interval_seconds=1
    ):
        if database == "":
            database = self.created_clickhouse_dbs[-1]
        mysql_gtid = self.mysql.query_and_get_data("SELECT @@GLOBAL.gtid_executed")[0][
            0
        ]
        materialized_with_ddl.check_query(
            self.clickhouse,
            f"SELECT executed_gtid_set /* expect: {mysql_gtid} */ FROM system.materialized_mysql_databases WHERE name = '{database}'",
            f"{mysql_gtid}\n",
            retry_count=retry_count,
            interval_seconds=interval_seconds,
        )


@pytest.fixture(scope="function")
def replication(started_mysql_8_0, request):
    try:
        replication = ReplicationHelper(node_db, started_mysql_8_0)
        yield replication
    finally:
        if hasattr(request.session, "testsfailed") and request.session.testsfailed:
            logging.warning(f"tests failed - not dropping databases")
        else:
            # drop databases only if the test succeeds - so we can inspect the database after failed tests
            try:
                replication.drop_dbs()
            except Exception as e:
                logging.warning(f"replication.drop_dbs() failed: {e}")


def test_materialized_database_dml_with_mysql_5_7(
    started_cluster, started_mysql_5_7, clickhouse_node: ClickHouseInstance
):
    materialized_with_ddl.dml_with_materialized_mysql_database(
        clickhouse_node, started_mysql_5_7, "mysql57"
    )
    materialized_with_ddl.materialized_mysql_database_with_views(
        clickhouse_node, started_mysql_5_7, "mysql57"
    )
    materialized_with_ddl.materialized_mysql_database_with_datetime_and_decimal(
        clickhouse_node, started_mysql_5_7, "mysql57"
    )
    materialized_with_ddl.move_to_prewhere_and_column_filtering(
        clickhouse_node, started_mysql_5_7, "mysql57"
    )


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
    ("clickhouse_node"), [node_disable_bytes_settings, node_disable_rows_settings]
)
def test_mysql_settings(
    started_cluster, started_mysql_8_0, started_mysql_5_7, clickhouse_node
):
    materialized_with_ddl.mysql_settings_test(
        clickhouse_node, started_mysql_5_7, "mysql57"
    )
    materialized_with_ddl.mysql_settings_test(
        clickhouse_node, started_mysql_8_0, "mysql80"
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


def test_binlog_client(started_cluster, started_mysql_8_0, replication):
    materialized_with_ddl.binlog_client_test(node_db, started_mysql_8_0, replication)
    replication.drop_dbs()
    materialized_with_ddl.binlog_client_timeout_test(
        node_db, started_mysql_8_0, replication
    )
    replication.drop_dbs()
    materialized_with_ddl.wrong_password_test(node_db, started_mysql_8_0, replication)
    replication.drop_dbs()
    materialized_with_ddl.dispatcher_buffer_test(
        node_db, started_mysql_8_0, replication
    )
    replication.drop_dbs()
    materialized_with_ddl.gtid_after_attach_test(
        node_db, started_mysql_8_0, replication
    )


def test_create_database_without_mysql_connection(
    started_cluster, started_mysql_8_0, clickhouse_node: ClickHouseInstance
):
    materialized_with_ddl.mysql_create_database_without_connection(
        clickhouse_node, started_mysql_8_0, "mysql80"
    )
