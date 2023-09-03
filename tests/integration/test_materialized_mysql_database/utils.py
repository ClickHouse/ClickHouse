import os
import logging
import time
import pymysql
from helpers.cluster import ClickHouseInstance


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


class ReplicationHelper:
    def __init__(self, cluster, clickhouse, mysql, mysql_host=None):
        self.clickhouse: ClickHouseInstance = clickhouse
        self.mysql: MySQLConnection = mysql
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
