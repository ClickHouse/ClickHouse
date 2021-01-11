import contextlib
import time
from string import Template

import pymysql.cursors
import pytest
from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
clickhouse_node = cluster.add_instance('node1', main_configs=['configs/remote_servers.xml'], with_mysql=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


class MySQLNodeInstance:
    def __init__(self, user='root', password='clickhouse', hostname='127.0.0.1', port=3308):
        self.user = user
        self.port = port
        self.hostname = hostname
        self.password = password
        self.mysql_connection = None  # lazy init

    def query(self, execution_query):
        if self.mysql_connection is None:
            self.mysql_connection = pymysql.connect(user=self.user, password=self.password, host=self.hostname,
                                                    port=self.port)
        with self.mysql_connection.cursor() as cursor:
            def execute(query):
                res = cursor.execute(query)
                if query.lstrip().lower().startswith(('select', 'show')):
                    # Mimic output of the ClickHouseInstance, which is:
                    # tab-sparated values and newline (\n)-separated rows.
                    rows = []
                    for row in cursor.fetchall():
                        rows.append("\t".join(str(item) for item in row))
                    res = "\n".join(rows)
                return res

            if isinstance(execution_query, (str, bytes)):
                return execute(execution_query)
            else:
                return [execute(q) for q in execution_query]

    def close(self):
        if self.mysql_connection is not None:
            self.mysql_connection.close()


def test_mysql_ddl_for_mysql_database(started_cluster):
    with contextlib.closing(MySQLNodeInstance('root', 'clickhouse', '127.0.0.1', port=3308)) as mysql_node:
        mysql_node.query("CREATE DATABASE test_database DEFAULT CHARACTER SET 'utf8'")

        clickhouse_node.query(
            "CREATE DATABASE test_database ENGINE = MySQL('mysql1:3306', 'test_database', 'root', 'clickhouse')")
        assert 'test_database' in clickhouse_node.query('SHOW DATABASES')

        mysql_node.query(
            'CREATE TABLE `test_database`.`test_table` ( `id` int(11) NOT NULL, PRIMARY KEY (`id`) ) ENGINE=InnoDB;')
        assert 'test_table' in clickhouse_node.query('SHOW TABLES FROM test_database')

        time.sleep(
            3)  # Because the unit of MySQL modification time is seconds, modifications made in the same second cannot be obtained
        mysql_node.query('ALTER TABLE `test_database`.`test_table` ADD COLUMN `add_column` int(11)')
        assert 'add_column' in clickhouse_node.query(
            "SELECT name FROM system.columns WHERE table = 'test_table' AND database = 'test_database'")

        time.sleep(
            3)  # Because the unit of MySQL modification time is seconds, modifications made in the same second cannot be obtained
        mysql_node.query('ALTER TABLE `test_database`.`test_table` DROP COLUMN `add_column`')
        assert 'add_column' not in clickhouse_node.query(
            "SELECT name FROM system.columns WHERE table = 'test_table' AND database = 'test_database'")

        mysql_node.query('DROP TABLE `test_database`.`test_table`;')
        assert 'test_table' not in clickhouse_node.query('SHOW TABLES FROM test_database')

        clickhouse_node.query("DROP DATABASE test_database")
        assert 'test_database' not in clickhouse_node.query('SHOW DATABASES')

        mysql_node.query("DROP DATABASE test_database")


def test_clickhouse_ddl_for_mysql_database(started_cluster):
    with contextlib.closing(MySQLNodeInstance('root', 'clickhouse', '127.0.0.1', port=3308)) as mysql_node:
        mysql_node.query("CREATE DATABASE test_database DEFAULT CHARACTER SET 'utf8'")
        mysql_node.query(
            'CREATE TABLE `test_database`.`test_table` ( `id` int(11) NOT NULL, PRIMARY KEY (`id`) ) ENGINE=InnoDB;')

        clickhouse_node.query(
            "CREATE DATABASE test_database ENGINE = MySQL('mysql1:3306', 'test_database', 'root', 'clickhouse')")

        assert 'test_table' in clickhouse_node.query('SHOW TABLES FROM test_database')
        clickhouse_node.query("DROP TABLE test_database.test_table")
        assert 'test_table' not in clickhouse_node.query('SHOW TABLES FROM test_database')
        clickhouse_node.query("ATTACH TABLE test_database.test_table")
        assert 'test_table' in clickhouse_node.query('SHOW TABLES FROM test_database')
        clickhouse_node.query("DETACH TABLE test_database.test_table")
        assert 'test_table' not in clickhouse_node.query('SHOW TABLES FROM test_database')
        clickhouse_node.query("ATTACH TABLE test_database.test_table")
        assert 'test_table' in clickhouse_node.query('SHOW TABLES FROM test_database')

        clickhouse_node.query("DROP DATABASE test_database")
        assert 'test_database' not in clickhouse_node.query('SHOW DATABASES')

        mysql_node.query("DROP DATABASE test_database")


def test_clickhouse_dml_for_mysql_database(started_cluster):
    with contextlib.closing(MySQLNodeInstance('root', 'clickhouse', '127.0.0.1', port=3308)) as mysql_node:
        mysql_node.query("CREATE DATABASE test_database DEFAULT CHARACTER SET 'utf8'")
        mysql_node.query(
            'CREATE TABLE `test_database`.`test_table` ( `i``d` int(11) NOT NULL, PRIMARY KEY (`i``d`)) ENGINE=InnoDB;')
        clickhouse_node.query(
            "CREATE DATABASE test_database ENGINE = MySQL('mysql1:3306', test_database, 'root', 'clickhouse')")

        assert clickhouse_node.query("SELECT count() FROM `test_database`.`test_table`").rstrip() == '0'
        clickhouse_node.query("INSERT INTO `test_database`.`test_table`(`i``d`) select number from numbers(10000)")
        assert clickhouse_node.query("SELECT count() FROM `test_database`.`test_table`").rstrip() == '10000'

        clickhouse_node.query("DROP DATABASE test_database")
        assert 'test_database' not in clickhouse_node.query('SHOW DATABASES')

        mysql_node.query("DROP DATABASE test_database")


def test_clickhouse_join_for_mysql_database(started_cluster):
    with contextlib.closing(MySQLNodeInstance('root', 'clickhouse', '127.0.0.1', port=3308)) as mysql_node:
        mysql_node.query("CREATE DATABASE IF NOT EXISTS test DEFAULT CHARACTER SET 'utf8'")
        mysql_node.query("CREATE TABLE test.t1_mysql_local ("
                         "pays    VARCHAR(55) DEFAULT 'FRA' NOT NULL,"
                         "service VARCHAR(5)  DEFAULT ''    NOT NULL,"
                         "opco    CHAR(3)     DEFAULT ''    NOT NULL"
                         ")")
        mysql_node.query("CREATE TABLE test.t2_mysql_local ("
                         "service VARCHAR(5) DEFAULT '' NOT NULL,"
                         "opco    VARCHAR(5) DEFAULT ''"
                         ")")
        clickhouse_node.query(
            "CREATE TABLE default.t1_remote_mysql AS mysql('mysql1:3306','test','t1_mysql_local','root','clickhouse')")
        clickhouse_node.query(
            "CREATE TABLE default.t2_remote_mysql AS mysql('mysql1:3306','test','t2_mysql_local','root','clickhouse')")
        assert clickhouse_node.query("SELECT s.pays "
                                     "FROM default.t1_remote_mysql AS s "
                                     "LEFT JOIN default.t1_remote_mysql AS s_ref "
                                     "ON (s_ref.opco = s.opco AND s_ref.service = s.service)") == ''
        mysql_node.query("DROP DATABASE test")


def test_bad_arguments_for_mysql_database_engine(started_cluster):
    with contextlib.closing(MySQLNodeInstance('root', 'clickhouse', '127.0.0.1', port=3308)) as mysql_node:
        with pytest.raises(QueryRuntimeException) as exception:
            mysql_node.query("CREATE DATABASE IF NOT EXISTS test_bad_arguments DEFAULT CHARACTER SET 'utf8'")
            clickhouse_node.query(
                "CREATE DATABASE test_database_bad_arguments ENGINE = MySQL('mysql1:3306', test_bad_arguments, root, 'clickhouse')")
        assert 'Database engine MySQL requested literal argument.' in str(exception.value)
        mysql_node.query("DROP DATABASE test_bad_arguments")


def test_data_types_support_level_for_mysql_database_engine(started_cluster):
    with contextlib.closing(MySQLNodeInstance('root', 'clickhouse', '127.0.0.1', port=3308)) as mysql_node:
        mysql_node.query("CREATE DATABASE IF NOT EXISTS test DEFAULT CHARACTER SET 'utf8'")
        clickhouse_node.query("CREATE DATABASE test_database ENGINE = MySQL('mysql1:3306', test, 'root', 'clickhouse')",
            settings={"mysql_datatypes_support_level": "decimal,datetime64"})

        assert "SETTINGS mysql_datatypes_support_level = \\'decimal,datetime64\\'" in clickhouse_node.query("SHOW CREATE DATABASE test_database FORMAT TSV")
        clickhouse_node.query("DETACH DATABASE test_database")

        # without context settings
        clickhouse_node.query("ATTACH DATABASE test_database")
        assert "SETTINGS mysql_datatypes_support_level = \\'decimal,datetime64\\'" in clickhouse_node.query("SHOW CREATE DATABASE test_database FORMAT TSV")

        clickhouse_node.query(
            "CREATE DATABASE test_database_1 ENGINE = MySQL('mysql1:3306', test, 'root', 'clickhouse') SETTINGS mysql_datatypes_support_level = 'decimal,datetime64'",
            settings={"mysql_datatypes_support_level": "decimal"})

        assert "SETTINGS mysql_datatypes_support_level = \\'decimal,datetime64\\'" in clickhouse_node.query("SHOW CREATE DATABASE test_database_1 FORMAT TSV")
        clickhouse_node.query("DETACH DATABASE test_database_1")

        # without context settings
        clickhouse_node.query("ATTACH DATABASE test_database_1")
        assert "SETTINGS mysql_datatypes_support_level = \\'decimal,datetime64\\'" in clickhouse_node.query("SHOW CREATE DATABASE test_database_1 FORMAT TSV")

        clickhouse_node.query("DROP DATABASE test_database")
        clickhouse_node.query("DROP DATABASE test_database_1")
        assert 'test_database' not in clickhouse_node.query('SHOW DATABASES')
        mysql_node.query("DROP DATABASE test")


decimal_values = [0.123, 0.4, 5.67, 8.91011, 123456789.123, -0.123, -0.4, -5.67, -8.91011, -123456789.123]
timestamp_values = ['2015-05-18 07:40:01.123', '2019-09-16 19:20:11.123']
timestamp_values_no_subsecond = ['2015-05-18 07:40:01', '2019-09-16 19:20:11']


@pytest.mark.parametrize("case_name, mysql_type, expected_ch_type, mysql_values, setting_mysql_datatypes_support_level",
                         [
                             ("decimal_default", "decimal NOT NULL", "Decimal(10, 0)", decimal_values,
                              "decimal,datetime64"),
                             ("decimal_default_nullable", "decimal", "Nullable(Decimal(10, 0))", decimal_values,
                              "decimal,datetime64"),
                             ("decimal_18_6", "decimal(18, 6) NOT NULL", "Decimal(18, 6)", decimal_values,
                              "decimal,datetime64"),
                             ("decimal_38_6", "decimal(38, 6) NOT NULL", "Decimal(38, 6)", decimal_values,
                              "decimal,datetime64"),

                             # Due to python DB driver roundtrip MySQL timestamp and datetime values
                             # are printed with 6 digits after decimal point, so to simplify tests a bit,
                             # we only validate precision of 0 and 6.
                             ("timestamp_default", "timestamp", "DateTime", timestamp_values, "decimal,datetime64"),
                             ("timestamp_6", "timestamp(6)", "DateTime64(6)", timestamp_values, "decimal,datetime64"),
                             ("datetime_default", "DATETIME NOT NULL", "DateTime64(0)", timestamp_values,
                              "decimal,datetime64"),
                             ("datetime_6", "DATETIME(6) NOT NULL", "DateTime64(6)", timestamp_values,
                              "decimal,datetime64"),

                             # right now precision bigger than 39 is not supported by ClickHouse's Decimal, hence fall back to String
                             (
                                     "decimal_40_6", "decimal(40, 6) NOT NULL", "String", decimal_values,
                                     "decimal,datetime64"),
                             ("decimal_18_6", "decimal(18, 6) NOT NULL", "String", decimal_values, "datetime64"),
                             ("decimal_18_6", "decimal(18, 6) NOT NULL", "String", decimal_values, ""),
                             ("datetime_6", "DATETIME(6) NOT NULL", "DateTime", timestamp_values_no_subsecond,
                              "decimal"),
                             ("datetime_6", "DATETIME(6) NOT NULL", "DateTime", timestamp_values_no_subsecond, ""),
                         ])
def test_mysql_types(started_cluster, case_name, mysql_type, expected_ch_type, mysql_values,
                     setting_mysql_datatypes_support_level):
    """ Verify that values written to MySQL can be read on ClickHouse side via DB engine MySQL,
    or Table engine MySQL, or mysql() table function.
    Make sure that type is converted properly and values match exactly.
    """

    substitutes = dict(
        mysql_db='decimal_support',
        table_name=case_name,
        mysql_type=mysql_type,
        mysql_values=', '.join('({})'.format(repr(x)) for x in mysql_values),
        ch_mysql_db='mysql_db',
        ch_mysql_table='mysql_table_engine_' + case_name,
        expected_ch_type=expected_ch_type,
    )

    clickhouse_query_settings = dict(
        mysql_datatypes_support_level=setting_mysql_datatypes_support_level
    )

    def execute_query(node, query, **kwargs):
        def do_execute(query):
            query = Template(query).safe_substitute(substitutes)
            res = node.query(query, **kwargs)
            return res if isinstance(res, int) else res.rstrip('\n\r')

        if isinstance(query, (str, bytes)):
            return do_execute(query)
        else:
            return [do_execute(q) for q in query]

    with contextlib.closing(MySQLNodeInstance('root', 'clickhouse', '127.0.0.1', port=3308)) as mysql_node:
        execute_query(mysql_node, [
            "DROP DATABASE IF EXISTS ${mysql_db}",
            "CREATE DATABASE ${mysql_db}  DEFAULT CHARACTER SET 'utf8'",
            "CREATE TABLE `${mysql_db}`.`${table_name}` (value ${mysql_type})",
            "INSERT INTO `${mysql_db}`.`${table_name}` (value) VALUES ${mysql_values}",
            "SELECT * FROM `${mysql_db}`.`${table_name}`",
            "FLUSH TABLES"
        ])

        assert execute_query(mysql_node, "SELECT COUNT(*) FROM ${mysql_db}.${table_name}") \
               == \
               "{}".format(len(mysql_values))

        # MySQL TABLE ENGINE
        execute_query(clickhouse_node, [
            "DROP TABLE IF EXISTS ${ch_mysql_table};",
            "CREATE TABLE ${ch_mysql_table} (value ${expected_ch_type}) ENGINE = MySQL('mysql1:3306', '${mysql_db}', '${table_name}', 'root', 'clickhouse')",
        ], settings=clickhouse_query_settings)

        # Validate type
        assert \
            execute_query(clickhouse_node, "SELECT toTypeName(value) FROM ${ch_mysql_table} LIMIT 1",
                          settings=clickhouse_query_settings) \
            == \
            expected_ch_type

        # Validate values
        assert \
            execute_query(clickhouse_node, "SELECT value FROM ${ch_mysql_table}",
                          settings=clickhouse_query_settings) \
            == \
            execute_query(mysql_node, "SELECT value FROM ${mysql_db}.${table_name}")

        # MySQL DATABASE ENGINE
        execute_query(clickhouse_node, [
            "DROP DATABASE IF EXISTS ${ch_mysql_db}",
            "CREATE DATABASE ${ch_mysql_db} ENGINE = MySQL('mysql1:3306', '${mysql_db}', 'root', 'clickhouse')"
        ], settings=clickhouse_query_settings)

        # Validate type
        assert \
            execute_query(clickhouse_node, "SELECT toTypeName(value) FROM ${ch_mysql_db}.${table_name} LIMIT 1",
                          settings=clickhouse_query_settings) \
            == \
            expected_ch_type

        # Validate values
        assert \
            execute_query(clickhouse_node, "SELECT value FROM ${ch_mysql_db}.${table_name}",
                          settings=clickhouse_query_settings) \
            == \
            execute_query(mysql_node, "SELECT value FROM ${mysql_db}.${table_name}")

        # MySQL TABLE FUNCTION
        # Validate type
        assert \
            execute_query(clickhouse_node,
                          "SELECT toTypeName(value) FROM mysql('mysql1:3306', '${mysql_db}', '${table_name}', 'root', 'clickhouse') LIMIT 1",
                          settings=clickhouse_query_settings) \
            == \
            expected_ch_type

        # Validate values
        assert \
            execute_query(mysql_node, "SELECT value FROM ${mysql_db}.${table_name}") \
            == \
            execute_query(clickhouse_node,
                          "SELECT value FROM mysql('mysql1:3306', '${mysql_db}', '${table_name}', 'root', 'clickhouse')",
                          settings=clickhouse_query_settings)
