import contextlib
import time
from string import Template
import uuid

import pymysql.cursors
import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import mysql_pass
from helpers.network import PartitionManager

cluster = ClickHouseCluster(__file__)
clickhouse_node = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml", "configs/named_collections.xml", "configs/backups.xml"],
    user_configs=["configs/users.xml"],
    with_mysql8=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


class MySQLNodeInstance:
    def __init__(self, cluster, name, user, password, hostname, port):
        self.cluster = cluster
        self.name = name
        self.docker_id = self.cluster.get_instance_docker_id(self.name)
        self.ipv6_address = None

        self.user = user
        self.port = port
        self.hostname = hostname
        self.password = password
        self.mysql_connection = None  # lazy init
        self.ip_address = hostname

    def exec_in_container(self, cmd, **kwargs):
        return self.cluster.exec_in_container(self.docker_id, cmd, **kwargs)

    def query(self, execution_query):
        if self.mysql_connection is None:
            self.mysql_connection = pymysql.connect(
                user=self.user,
                password=self.password,
                host=self.hostname,
                port=self.port,
                autocommit=True,
            )
        with self.mysql_connection.cursor() as cursor:

            def execute(query):
                res = cursor.execute(query)
                if query.lstrip().lower().startswith(("select", "show")):
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
    with contextlib.closing(
        MySQLNodeInstance(
            started_cluster,
            "mysql80",
            "root", mysql_pass, started_cluster.mysql8_ip, started_cluster.mysql8_port
        )
    ) as mysql_node:
        mysql_node.query("DROP DATABASE IF EXISTS test_database")
        mysql_node.query("CREATE DATABASE test_database DEFAULT CHARACTER SET 'utf8'")

        clickhouse_node.query("DROP DATABASE IF EXISTS test_database")
        clickhouse_node.query(
            f"CREATE DATABASE test_database ENGINE = MySQL('mysql80:3306', 'test_database', 'root', '{mysql_pass}')"
        )
        assert "test_database" in clickhouse_node.query("SHOW DATABASES")

        mysql_node.query(
            "CREATE TABLE `test_database`.`test_table` ( `id` int(11) NOT NULL, PRIMARY KEY (`id`) ) ENGINE=InnoDB;"
        )
        assert "test_table" in clickhouse_node.query("SHOW TABLES FROM test_database")

        time.sleep(
            3
        )  # Because the unit of MySQL modification time is seconds, modifications made in the same second cannot be obtained
        mysql_node.query(
            "ALTER TABLE `test_database`.`test_table` ADD COLUMN `add_column` int(11)"
        )
        assert "add_column" in clickhouse_node.query(
            "SELECT name FROM system.columns WHERE table = 'test_table' AND database = 'test_database'"
        )

        time.sleep(
            3
        )  # Because the unit of MySQL modification time is seconds, modifications made in the same second cannot be obtained
        mysql_node.query(
            "ALTER TABLE `test_database`.`test_table` DROP COLUMN `add_column`"
        )
        assert "add_column" not in clickhouse_node.query(
            "SELECT name FROM system.columns WHERE table = 'test_table' AND database = 'test_database'"
        )

        mysql_node.query("DROP TABLE `test_database`.`test_table`;")
        assert "test_table" not in clickhouse_node.query(
            "SHOW TABLES FROM test_database"
        )

        clickhouse_node.query("DROP DATABASE test_database")
        assert "test_database" not in clickhouse_node.query("SHOW DATABASES")

        mysql_node.query("DROP DATABASE test_database")


def test_clickhouse_ddl_for_mysql_database(started_cluster):
    with contextlib.closing(
        MySQLNodeInstance(
            started_cluster,
            "mysql80",
            "root", mysql_pass, started_cluster.mysql8_ip, started_cluster.mysql8_port
        )
    ) as mysql_node:
        mysql_node.query("DROP DATABASE IF EXISTS test_database")
        mysql_node.query("CREATE DATABASE test_database DEFAULT CHARACTER SET 'utf8'")
        mysql_node.query(
            "CREATE TABLE `test_database`.`test_table` ( `id` int(11) NOT NULL, PRIMARY KEY (`id`) ) ENGINE=InnoDB;"
        )

        clickhouse_node.query("DROP DATABASE IF EXISTS test_database")
        clickhouse_node.query(
            f"CREATE DATABASE test_database ENGINE = MySQL('mysql80:3306', 'test_database', 'root', '{mysql_pass}')"
        )

        assert "test_table" in clickhouse_node.query("SHOW TABLES FROM test_database")
        clickhouse_node.query("DROP TABLE test_database.test_table")
        assert "test_table" not in clickhouse_node.query(
            "SHOW TABLES FROM test_database"
        )
        clickhouse_node.query("ATTACH TABLE test_database.test_table")
        assert "test_table" in clickhouse_node.query("SHOW TABLES FROM test_database")
        clickhouse_node.query("DETACH TABLE test_database.test_table")
        assert "test_table" not in clickhouse_node.query(
            "SHOW TABLES FROM test_database"
        )
        clickhouse_node.query("ATTACH TABLE test_database.test_table")
        assert "test_table" in clickhouse_node.query("SHOW TABLES FROM test_database")
        # Test DETACH PERMANENTLY for system.detached_tables (PR #107943)
        clickhouse_node.query("DETACH TABLE test_database.test_table PERMANENTLY")
        is_perm = clickhouse_node.query("SELECT is_permanently FROM system.detached_tables WHERE database = 'test_database' AND table = 'test_table'").strip()
        assert is_perm == "1", f"Expected is_permanently=1, got {is_perm}"
        clickhouse_node.query("ATTACH TABLE test_database.test_table")

        # Test ordinary DETACH TABLE (not PERMANENTLY) for system.detached_tables (PR #107943)
        clickhouse_node.query("DETACH TABLE test_database.test_table")
        is_perm_ordinary = clickhouse_node.query("SELECT is_permanently FROM system.detached_tables WHERE database = 'test_database' AND table = 'test_table'").strip()
        assert is_perm_ordinary == "0", f"Expected is_permanently=0 for ordinary detach, got {is_perm_ordinary}"
        clickhouse_node.query("ATTACH TABLE test_database.test_table")

        clickhouse_node.query("DROP DATABASE test_database")
        assert "test_database" not in clickhouse_node.query("SHOW DATABASES")

        mysql_node.query("DROP DATABASE test_database")


def test_clickhouse_dml_for_mysql_database(started_cluster):
    with contextlib.closing(
        MySQLNodeInstance(
            started_cluster,
            "mysql80",
            "root", mysql_pass, started_cluster.mysql8_ip, started_cluster.mysql8_port
        )
    ) as mysql_node:
        mysql_node.query("DROP DATABASE IF EXISTS test_database")
        mysql_node.query("CREATE DATABASE test_database DEFAULT CHARACTER SET 'utf8'")
        mysql_node.query(
            "CREATE TABLE `test_database`.`test_table` ( `i``d` int(11) NOT NULL, PRIMARY KEY (`i``d`)) ENGINE=InnoDB;"
        )

        clickhouse_node.query("DROP DATABASE IF EXISTS test_database")
        clickhouse_node.query(
            f"CREATE DATABASE test_database ENGINE = MySQL('mysql80:3306', test_database, 'root', '{mysql_pass}')"
        )

        assert (
            clickhouse_node.query(
                "SELECT count() FROM `test_database`.`test_table`"
            ).rstrip()
            == "0"
        )
        clickhouse_node.query(
            "INSERT INTO `test_database`.`test_table`(`i``d`) select number from numbers(10000)"
        )
        assert (
            clickhouse_node.query(
                "SELECT count() FROM `test_database`.`test_table`"
            ).rstrip()
            == "10000"
        )

        clickhouse_node.query("DROP DATABASE test_database")
        assert "test_database" not in clickhouse_node.query("SHOW DATABASES")

        mysql_node.query("DROP DATABASE test_database")


def test_clickhouse_join_for_mysql_database(started_cluster):
    with contextlib.closing(
        MySQLNodeInstance(
            started_cluster,
            "mysql80",
            "root", mysql_pass, started_cluster.mysql8_ip, started_cluster.mysql8_port
        )
    ) as mysql_node:
        mysql_node.query("DROP DATABASE IF EXISTS test")
        mysql_node.query("CREATE DATABASE test DEFAULT CHARACTER SET 'utf8'")
        mysql_node.query(
            "CREATE TABLE test.t1_mysql_local ("
            "pays    VARCHAR(55) DEFAULT 'FRA' NOT NULL,"
            "service VARCHAR(5)  DEFAULT ''    NOT NULL,"
            "opco    CHAR(3)     DEFAULT ''    NOT NULL"
            ")"
        )
        mysql_node.query(
            "CREATE TABLE test.t2_mysql_local ("
            "service VARCHAR(5) DEFAULT '' NOT NULL,"
            "opco    VARCHAR(5) DEFAULT ''"
            ")"
        )
        clickhouse_node.query("DROP TABLE IF EXISTS default.t1_remote_mysql SYNC")
        clickhouse_node.query("DROP TABLE IF EXISTS default.t2_remote_mysql SYNC")
        clickhouse_node.query(
            f"CREATE TABLE default.t1_remote_mysql AS mysql('mysql80:3306','test','t1_mysql_local','root','{mysql_pass}')"
        )
        clickhouse_node.query(
            f"CREATE TABLE default.t2_remote_mysql AS mysql('mysql80:3306','test','t2_mysql_local','root','{mysql_pass}')"
        )
        clickhouse_node.query(
            "INSERT INTO `default`.`t1_remote_mysql` VALUES ('EN','A',''),('RU','B','AAA')"
        )
        clickhouse_node.query(
            "INSERT INTO `default`.`t2_remote_mysql` VALUES ('A','AAA'),('Z','')"
        )

        assert (
            clickhouse_node.query(
                "SELECT s.pays "
                "FROM default.t1_remote_mysql AS s "
                "LEFT JOIN default.t1_remote_mysql AS s_ref "
                "ON (s_ref.opco = s.opco AND s_ref.service = s.service) "
                "WHERE s_ref.opco != '' AND s.opco != '' "
            ).rstrip()
            == "RU"
        )
        mysql_node.query("DROP DATABASE test")


def test_bad_arguments_for_mysql_database_engine(started_cluster):
    with contextlib.closing(
        MySQLNodeInstance(
            started_cluster,
            "mysql80",
            "root",
            mysql_pass,
            started_cluster.mysql8_ip,
            port=started_cluster.mysql8_port,
        )
    ) as mysql_node:
        with pytest.raises(QueryRuntimeException) as exception:
            mysql_node.query("DROP DATABASE IF EXISTS test_bad_arguments")
            mysql_node.query(
                "CREATE DATABASE test_bad_arguments DEFAULT CHARACTER SET 'utf8'"
            )
            clickhouse_node.query(
                f"CREATE DATABASE test_database_bad_arguments ENGINE = MySQL('mysql80:3306', test_bad_arguments, root, '{mysql_pass}')"
            )
        assert "Database engine MySQL requested literal argument." in str(
            exception.value
        )
        mysql_node.query("DROP DATABASE test_bad_arguments")


def test_column_comments_for_mysql_database_engine(started_cluster):
    with contextlib.closing(
        MySQLNodeInstance(
            started_cluster,
            "mysql80",
            "root", mysql_pass, started_cluster.mysql8_ip, started_cluster.mysql8_port
        )
    ) as mysql_node:
        mysql_node.query("DROP DATABASE IF EXISTS test_database")
        mysql_node.query("CREATE DATABASE test_database DEFAULT CHARACTER SET 'utf8'")

        clickhouse_node.query("DROP DATABASE IF EXISTS test_database")
        clickhouse_node.query(
            f"CREATE DATABASE test_database ENGINE = MySQL('mysql80:3306', 'test_database', 'root', '{mysql_pass}')"
        )
        assert "test_database" in clickhouse_node.query("SHOW DATABASES")

        mysql_node.query(
            "CREATE TABLE `test_database`.`test_table` ( `id` int(11) NOT NULL, PRIMARY KEY (`id`), `test` int COMMENT 'test comment') ENGINE=InnoDB;"
        )
        assert "test comment" in clickhouse_node.query(
            "DESCRIBE TABLE `test_database`.`test_table`"
        )

        time.sleep(
            3
        )  # Because the unit of MySQL modification time is seconds, modifications made in the same second cannot be obtained
        mysql_node.query(
            "ALTER TABLE `test_database`.`test_table` ADD COLUMN `add_column` int(11) COMMENT 'add_column comment'"
        )
        assert "add_column comment" in clickhouse_node.query(
            "SELECT comment FROM system.columns WHERE table = 'test_table' AND database = 'test_database'"
        )

        clickhouse_node.query("DROP DATABASE test_database")
        mysql_node.query("DROP DATABASE test_database")


def test_data_types_support_level_for_mysql_database_engine(started_cluster):
    with contextlib.closing(
        MySQLNodeInstance(
            started_cluster,
            "mysql80",
            "root", mysql_pass, started_cluster.mysql8_ip, started_cluster.mysql8_port
        )
    ) as mysql_node:
        mysql_node.query("DROP DATABASE IF EXISTS test")
        mysql_node.query(
            "CREATE DATABASE test DEFAULT CHARACTER SET 'utf8'"
        )
        clickhouse_node.query("DROP DATABASE IF EXISTS test_database")
        clickhouse_node.query(
            f"CREATE DATABASE test_database ENGINE = MySQL('mysql80:3306', test, 'root', '{mysql_pass}')",
            settings={"mysql_datatypes_support_level": "decimal,datetime64"},
        )

        assert (
            "SETTINGS mysql_datatypes_support_level = \\'decimal,datetime64\\'"
            in clickhouse_node.query("SHOW CREATE DATABASE test_database FORMAT TSV")
        )
        clickhouse_node.query("DETACH DATABASE test_database")

        # without context settings
        clickhouse_node.query("ATTACH DATABASE test_database")
        assert (
            "SETTINGS mysql_datatypes_support_level = \\'decimal,datetime64\\'"
            in clickhouse_node.query("SHOW CREATE DATABASE test_database FORMAT TSV")
        )

        clickhouse_node.query(
            f"CREATE DATABASE test_database_1 ENGINE = MySQL('mysql80:3306', test, 'root', '{mysql_pass}') SETTINGS mysql_datatypes_support_level = 'decimal,datetime64'",
            settings={"mysql_datatypes_support_level": "decimal"},
        )

        assert (
            "SETTINGS mysql_datatypes_support_level = \\'decimal,datetime64\\'"
            in clickhouse_node.query("SHOW CREATE DATABASE test_database_1 FORMAT TSV")
        )
        clickhouse_node.query("DETACH DATABASE test_database_1")

        # without context settings
        clickhouse_node.query("ATTACH DATABASE test_database_1")
        assert (
            "SETTINGS mysql_datatypes_support_level = \\'decimal,datetime64\\'"
            in clickhouse_node.query("SHOW CREATE DATABASE test_database_1 FORMAT TSV")
        )

        clickhouse_node.query("DROP DATABASE test_database")
        clickhouse_node.query("DROP DATABASE test_database_1")
        assert "test_database" not in clickhouse_node.query("SHOW DATABASES")
        mysql_node.query("DROP DATABASE test")


float_values = [0, "NULL"]
clickhouse_float_values = [0, "\\N"]
int32_values = [0, 1, -1, 2147483647, -2147483648]
uint32_values = [
    0,
    1,
]  # [FIXME] seems client have issue with value 4294967295, it returns -1 for it
mint_values = [0, 1, -1, 8388607, -8388608]
umint_values = [0, 1, 16777215]
int16_values = [0, 1, -1, 32767, -32768]
uint16_values = [0, 1, 65535]
int8_values = [0, 1, -1, 127, -128]
uint8_values = [0, 1, 255]
string_values = ["'ClickHouse'", "NULL"]
clickhouse_string_values = ["ClickHouse", "\\N"]
date_values = ["'1970-01-01'"]
date2Date32_values = ["'1925-01-01'", "'2283-11-11'"]
date2String_values = ["'1000-01-01'", "'9999-12-31'"]


decimal_values = [
    0,
    0.123,
    0.4,
    5.67,
    8.91011,
    123456789.123,
    -0.123,
    -0.4,
    -5.67,
    -8.91011,
    -123456789.123,
]
timestamp_values = ["'2015-05-18 07:40:01.123'", "'2019-09-16 19:20:11.123'"]
timestamp_values_no_subsecond = ["'2015-05-18 07:40:01'", "'2019-09-16 19:20:11'"]


def arryToString(expected_clickhouse_values):
    return "\n".join(str(value) for value in expected_clickhouse_values)


#  if expected_clickhouse_values is "", compare MySQL and ClickHouse query results directly
@pytest.mark.parametrize(
    "case_name, mysql_type, expected_ch_type, mysql_values, expected_clickhouse_values, setting_mysql_datatypes_support_level",
    [
        pytest.param(
            "common_types",
            "FLOAT",
            "Nullable(Float32)",
            float_values,
            clickhouse_float_values,
            "",
            id="float_1",
        ),
        pytest.param(
            "common_types",
            "FLOAT UNSIGNED",
            "Nullable(Float32)",
            float_values,
            clickhouse_float_values,
            "",
            id="float_2",
        ),
        pytest.param(
            "common_types",
            "INT",
            "Nullable(Int32)",
            int32_values,
            int32_values,
            "",
            id="common_types_1",
        ),
        pytest.param(
            "common_types",
            "INT NOT NULL",
            "Int32",
            int32_values,
            int32_values,
            "",
            id="common_types_2",
        ),
        pytest.param(
            "common_types",
            "INT UNSIGNED NOT NULL",
            "UInt32",
            uint32_values,
            uint32_values,
            "",
            id="common_types_3",
        ),
        pytest.param(
            "common_types",
            "INT UNSIGNED",
            "Nullable(UInt32)",
            uint32_values,
            uint32_values,
            "",
            id="common_types_4",
        ),
        pytest.param(
            "common_types",
            "INT UNSIGNED DEFAULT NULL",
            "Nullable(UInt32)",
            uint32_values,
            uint32_values,
            "",
            id="common_types_5",
        ),
        pytest.param(
            "common_types",
            "INT UNSIGNED DEFAULT '1'",
            "Nullable(UInt32)",
            uint32_values,
            uint32_values,
            "",
            id="common_types_6",
        ),
        pytest.param(
            "common_types",
            "INT(10)",
            "Nullable(Int32)",
            int32_values,
            int32_values,
            "",
            id="common_types_7",
        ),
        pytest.param(
            "common_types",
            "INT(10) NOT NULL",
            "Int32",
            int32_values,
            int32_values,
            "",
            id="common_types_8",
        ),
        pytest.param(
            "common_types",
            "INT(10) UNSIGNED NOT NULL",
            "UInt32",
            uint32_values,
            uint32_values,
            "",
            id="common_types_8",
        ),
        pytest.param(
            "common_types",
            "INT(10) UNSIGNED",
            "Nullable(UInt32)",
            uint32_values,
            uint32_values,
            "",
            id="common_types_9",
        ),
        pytest.param(
            "common_types",
            "INT(10) UNSIGNED DEFAULT NULL",
            "Nullable(UInt32)",
            uint32_values,
            uint32_values,
            "",
            id="common_types_10",
        ),
        pytest.param(
            "common_types",
            "INT(10) UNSIGNED DEFAULT '1'",
            "Nullable(UInt32)",
            uint32_values,
            uint32_values,
            "",
            id="common_types_11",
        ),
        pytest.param(
            "common_types",
            "INTEGER",
            "Nullable(Int32)",
            int32_values,
            int32_values,
            "",
            id="common_types_12",
        ),
        pytest.param(
            "common_types",
            "INTEGER UNSIGNED",
            "Nullable(UInt32)",
            uint32_values,
            uint32_values,
            "",
            id="common_types_13",
        ),
        pytest.param(
            "common_types",
            "MEDIUMINT",
            "Nullable(Int32)",
            mint_values,
            mint_values,
            "",
            id="common_types_14",
        ),
        pytest.param(
            "common_types",
            "MEDIUMINT UNSIGNED",
            "Nullable(UInt32)",
            umint_values,
            umint_values,
            "",
            id="common_types_15",
        ),
        pytest.param(
            "common_types",
            "SMALLINT",
            "Nullable(Int16)",
            int16_values,
            int16_values,
            "",
            id="common_types_16",
        ),
        pytest.param(
            "common_types",
            "SMALLINT UNSIGNED",
            "Nullable(UInt16)",
            uint16_values,
            uint16_values,
            "",
            id="common_types_17",
        ),
        pytest.param(
            "common_types",
            "TINYINT",
            "Nullable(Int8)",
            int8_values,
            int8_values,
            "",
            id="common_types_18",
        ),
        pytest.param(
            "common_types",
            "TINYINT UNSIGNED",
            "Nullable(UInt8)",
            uint8_values,
            uint8_values,
            "",
            id="common_types_19",
        ),
        pytest.param(
            "common_types",
            "VARCHAR(10)",
            "Nullable(String)",
            string_values,
            clickhouse_string_values,
            "",
            id="common_types_20",
        ),
        pytest.param(
            "common_types",
            "DATE",
            "Nullable(Date)",
            date_values,
            "",
            "",
            id="common_types_21",
        ),
        pytest.param(
            "common_types",
            "DATE",
            "Nullable(Date32)",
            date2Date32_values,
            "",
            "date2Date32",
            id="common_types_22",
        ),
        pytest.param(
            "common_types",
            "DATE",
            "Nullable(String)",
            date2String_values,
            "",
            "date2String",
            id="common_types_23",
        ),
        pytest.param(
            "common_types",
            "binary(1)",
            "Nullable(FixedString(1))",
            [1],
            [1],
            "",
            id="common_types_24",
        ),
        pytest.param(
            "common_types",
            "binary(0)",
            "Nullable(FixedString(1))",
            ["NULL"],
            ["\\N"],
            "",
            id="common_types_25",
        ),
        pytest.param(
            "decimal_default",
            "decimal NOT NULL",
            "Decimal(10, 0)",
            decimal_values,
            "",
            "decimal,datetime64",
            id="decimal_1",
        ),
        pytest.param(
            "decimal_default_nullable",
            "decimal",
            "Nullable(Decimal(10, 0))",
            decimal_values,
            "",
            "decimal,datetime64",
            id="decimal_2",
        ),
        pytest.param(
            "decimal_18_6",
            "decimal(18, 6) NOT NULL",
            "Decimal(18, 6)",
            decimal_values,
            "",
            "decimal,datetime64",
            id="decimal_3",
        ),
        pytest.param(
            "decimal_38_6",
            "decimal(38, 6) NOT NULL",
            "Decimal(38, 6)",
            decimal_values,
            "",
            "decimal,datetime64",
            id="decimal_4",
        ),
        # Due to python DB driver roundtrip MySQL timestamp and datetime values
        # are printed with 6 digits after decimal point, so to simplify tests a bit,
        # we only validate precision of 0 and 6.
        pytest.param(
            "timestamp_default",
            "timestamp",
            "Nullable(DateTime)",
            timestamp_values,
            "",
            "decimal,datetime64",
            id="timestamp_default",
        ),
        pytest.param(
            "timestamp_6",
            "timestamp(6)",
            "Nullable(DateTime64(6))",
            timestamp_values,
            "",
            "decimal,datetime64",
            id="timestamp_6",
        ),
        pytest.param(
            "datetime_default",
            "DATETIME NOT NULL",
            "DateTime64(0)",
            timestamp_values,
            "",
            "decimal,datetime64",
            id="datetime_default",
        ),
        pytest.param(
            "datetime_6",
            "DATETIME(6) NOT NULL",
            "DateTime64(6)",
            timestamp_values,
            "",
            "decimal,datetime64",
            id="datetime_6_1",
        ),
        pytest.param(
            "decimal_40_6",
            "decimal(40, 6) NOT NULL",
            "Decimal(40, 6)",
            decimal_values,
            "",
            "decimal,datetime64",
            id="decimal_40_6",
        ),
        pytest.param(
            "decimal_18_6",
            "decimal(18, 6) NOT NULL",
            "String",
            decimal_values,
            "",
            "datetime64",
            id="decimal_18_6_1",
        ),
        pytest.param(
            "decimal_18_6",
            "decimal(18, 6) NOT NULL",
            "String",
            decimal_values,
            "",
            "",
            id="decimal_18_6_2",
        ),
        pytest.param(
            "datetime_6",
            "DATETIME(6) NOT NULL",
            "DateTime",
            timestamp_values_no_subsecond,
            "",
            "decimal",
            id="datetime_6_2",
        ),
        pytest.param(
            "datetime_6",
            "DATETIME(6) NOT NULL",
            "DateTime",
            timestamp_values_no_subsecond,
            "",
            "",
            id="datetime_6_3",
        ),
    ],
)
def test_mysql_types(
    started_cluster,
    case_name,
    mysql_type,
    expected_ch_type,
    mysql_values,
    expected_clickhouse_values,
    setting_mysql_datatypes_support_level,
):
    """Verify that values written to MySQL can be read on ClickHouse side via DB engine MySQL,
    or Table engine MySQL, or mysql() table function.
    Make sure that type is converted properly and values match exactly.
    """

    substitutes = dict(
        mysql_db="decimal_support",
        table_name=case_name,
        mysql_type=mysql_type,
        mysql_values=", ".join("({})".format(x) for x in mysql_values),
        ch_mysql_db="mysql_db",
        ch_mysql_table="mysql_table_engine_" + case_name,
        expected_ch_type=expected_ch_type,
    )

    clickhouse_query_settings = dict(
        mysql_datatypes_support_level=setting_mysql_datatypes_support_level,
        output_format_decimal_trailing_zeros=1,
    )

    def execute_query(node, query, **kwargs):
        def do_execute(query):
            query = Template(query).safe_substitute(substitutes)
            res = node.query(query, **kwargs)
            return res if isinstance(res, int) else res.rstrip("\n\r")

        if isinstance(query, (str, bytes)):
            return do_execute(query)
        else:
            return [do_execute(q) for q in query]

    with contextlib.closing(
        MySQLNodeInstance(
            started_cluster,
            "mysql80",
            "root",
            mysql_pass,
            started_cluster.mysql8_ip,
            port=started_cluster.mysql8_port,
        )
    ) as mysql_node:
        execute_query(
            mysql_node,
            [
                "DROP DATABASE IF EXISTS ${mysql_db}",
                "CREATE DATABASE ${mysql_db}  DEFAULT CHARACTER SET 'utf8'",
                "CREATE TABLE `${mysql_db}`.`${table_name}` (value ${mysql_type})",
                "INSERT INTO `${mysql_db}`.`${table_name}` (value) VALUES ${mysql_values}",
                "SELECT * FROM `${mysql_db}`.`${table_name}`",
                "FLUSH TABLES",
            ],
        )

        assert execute_query(
            mysql_node, "SELECT COUNT(*) FROM ${mysql_db}.${table_name}"
        ) == "{}".format(len(mysql_values))

        # MySQL TABLE ENGINE
        execute_query(
            clickhouse_node,
            [
                "DROP TABLE IF EXISTS ${ch_mysql_table};",
                "CREATE TABLE ${ch_mysql_table} (value ${expected_ch_type}) ENGINE = MySQL('mysql80:3306', '${mysql_db}', '${table_name}', 'root'," + f"'{mysql_pass}')",
            ],
            settings=clickhouse_query_settings,
        )

        # Validate type
        assert (
            execute_query(
                clickhouse_node,
                "SELECT toTypeName(value) FROM ${ch_mysql_table} LIMIT 1",
                settings=clickhouse_query_settings,
            )
            == expected_ch_type
        )

        expected_format_clickhouse_values = arryToString(expected_clickhouse_values)
        if expected_format_clickhouse_values == "":
            expected_format_clickhouse_values = execute_query(
                mysql_node, "SELECT value FROM ${mysql_db}.${table_name}"
            )

        # Validate values
        assert expected_format_clickhouse_values == execute_query(
            clickhouse_node,
            "SELECT value FROM ${ch_mysql_table}",
            settings=clickhouse_query_settings,
        )

        # MySQL DATABASE ENGINE
        execute_query(
            clickhouse_node,
            [
                "DROP DATABASE IF EXISTS ${ch_mysql_db}",
                "CREATE DATABASE ${ch_mysql_db} ENGINE = MySQL('mysql80:3306', '${mysql_db}', 'root'," + f"'{mysql_pass}')",
            ],
            settings=clickhouse_query_settings,
        )

        # Validate type
        assert (
            execute_query(
                clickhouse_node,
                "SELECT toTypeName(value) FROM ${ch_mysql_db}.${table_name} LIMIT 1",
                settings=clickhouse_query_settings,
            )
            == expected_ch_type
        )

        # Validate values
        assert expected_format_clickhouse_values == execute_query(
            clickhouse_node,
            "SELECT value FROM ${ch_mysql_db}.${table_name}",
            settings=clickhouse_query_settings,
        )

        # MySQL TABLE FUNCTION
        # Validate type
        assert (
            execute_query(
                clickhouse_node,
                "SELECT toTypeName(value) FROM mysql('mysql80:3306', '${mysql_db}', '${table_name}', 'root'," + f"'{mysql_pass}') LIMIT 1",
                settings=clickhouse_query_settings,
            )
            == expected_ch_type
        )

        # Validate values
        assert expected_format_clickhouse_values == execute_query(
            clickhouse_node,
            "SELECT value FROM mysql('mysql80:3306', '${mysql_db}', '${table_name}', 'root'," + f"'{mysql_pass}')",
            settings=clickhouse_query_settings,
        )


def test_predefined_connection_configuration(started_cluster):
    with contextlib.closing(
        MySQLNodeInstance(
            started_cluster,
            "mysql80",
            "root", mysql_pass, started_cluster.mysql8_ip, started_cluster.mysql8_port
        )
    ) as mysql_node:
        mysql_node.query("DROP DATABASE IF EXISTS test_database")
        mysql_node.query("CREATE DATABASE test_database DEFAULT CHARACTER SET 'utf8'")
        mysql_node.query(
            "CREATE TABLE `test_database`.`test_table` ( `id` int(11) NOT NULL, PRIMARY KEY (`id`) ) ENGINE=InnoDB;"
        )

        clickhouse_node.query("DROP DATABASE IF EXISTS test_database")
        clickhouse_node.query("CREATE DATABASE test_database ENGINE = MySQL(mysql1)")
        clickhouse_node.query(
            "INSERT INTO `test_database`.`test_table` select number from numbers(100)"
        )
        assert (
            clickhouse_node.query(
                "SELECT count() FROM `test_database`.`test_table`"
            ).rstrip()
            == "100"
        )

        result = clickhouse_node.query("show create table test_database.test_table")
        assert (
            result.strip()
            == "CREATE TABLE test_database.test_table\\n(\\n    `id` Int32\\n)\\nENGINE = MySQL(mysql1, `table` = \\'test_table\\')"
        )

        clickhouse_node.query("DROP DATABASE test_database")
        clickhouse_node.query_and_get_error(
            "CREATE DATABASE test_database ENGINE = MySQL(mysql2)"
        )
        clickhouse_node.query_and_get_error(
            "CREATE DATABASE test_database ENGINE = MySQL(unknown_collection)"
        )
        clickhouse_node.query_and_get_error(
            "CREATE DATABASE test_database ENGINE = MySQL(mysql1, 1)"
        )

        clickhouse_node.query(
            "CREATE DATABASE test_database ENGINE = MySQL(mysql1, port=3306)"
        )
        assert (
            clickhouse_node.query(
                "SELECT count() FROM `test_database`.`test_table`"
            ).rstrip()
            == "100"
        )


def test_restart_server(started_cluster):
    with contextlib.closing(
        MySQLNodeInstance(
            started_cluster,
            "mysql80",
            "root", mysql_pass, started_cluster.mysql8_ip, started_cluster.mysql8_port
        )
    ) as mysql_node:
        mysql_node.query("DROP DATABASE IF EXISTS test_restart")
        clickhouse_node.query("DROP DATABASE IF EXISTS test_restart")
        clickhouse_node.query_and_get_error(
            f"CREATE DATABASE test_restart ENGINE = MySQL('mysql80:3306', 'test_restart', 'root', '{mysql_pass}')"
        )
        assert "test_restart" not in clickhouse_node.query("SHOW DATABASES")

        mysql_node.query("CREATE DATABASE test_restart DEFAULT CHARACTER SET 'utf8'")
        mysql_node.query(
            "CREATE TABLE `test_restart`.`test_table` ( `id` int(11) NOT NULL, PRIMARY KEY (`id`) ) ENGINE=InnoDB;"
        )
        clickhouse_node.query(
            f"CREATE DATABASE test_restart ENGINE = MySQL('mysql80:3306', 'test_restart', 'root', '{mysql_pass}')"
        )

        assert "test_restart" in clickhouse_node.query("SHOW DATABASES")
        assert "test_table" in clickhouse_node.query("SHOW TABLES FROM test_restart")

        with PartitionManager() as pm:
            pm.partition_instances(
                clickhouse_node, mysql_node, action="REJECT --reject-with tcp-reset"
            )
            clickhouse_node.restart_clickhouse()
            clickhouse_node.query_and_get_error("SHOW TABLES FROM test_restart")
        assert "test_table" in clickhouse_node.query("SHOW TABLES FROM test_restart")


def test_memory_leak(started_cluster):
    with contextlib.closing(
        MySQLNodeInstance(
            started_cluster,
            "mysql80",
            "root", mysql_pass, started_cluster.mysql8_ip, started_cluster.mysql8_port
        )
    ) as mysql_node:
        mysql_node.query("DROP DATABASE IF EXISTS test_database")
        mysql_node.query("CREATE DATABASE test_database DEFAULT CHARACTER SET 'utf8'")
        mysql_node.query(
            "CREATE TABLE `test_database`.`test_table` ( `id` int(11) NOT NULL, PRIMARY KEY (`id`) ) ENGINE=InnoDB;"
        )

        clickhouse_node.query("DROP DATABASE IF EXISTS test_database")
        clickhouse_node.query(
            f"CREATE DATABASE test_database ENGINE = MySQL('mysql80:3306', 'test_database', 'root', '{mysql_pass}') SETTINGS connection_auto_close = 1"
        )
        clickhouse_node.query("SELECT count() FROM `test_database`.`test_table`")

        clickhouse_node.query("DROP DATABASE test_database")
        clickhouse_node.restart_clickhouse()


def test_password_leak(started_cluster):
    with contextlib.closing(
        MySQLNodeInstance(
            started_cluster,
            "mysql80",
            "root", mysql_pass, started_cluster.mysql8_ip, started_cluster.mysql8_port
        )
    ) as mysql_node:
        mysql_node.query("DROP DATABASE IF EXISTS test_database")
        mysql_node.query("CREATE DATABASE test_database DEFAULT CHARACTER SET 'utf8'")
        mysql_node.query(
            "CREATE TABLE `test_database`.`test_table` ( `id` int(11) NOT NULL, PRIMARY KEY (`id`) ) ENGINE=InnoDB;"
        )

        clickhouse_node.query("DROP DATABASE IF EXISTS test_database")
        clickhouse_node.query(
            f"CREATE DATABASE test_database ENGINE = MySQL('mysql80:3306', 'test_database', 'root', '{mysql_pass}') SETTINGS connection_auto_close = 1"
        )
        assert "clickhouse" not in clickhouse_node.query(
            "SHOW CREATE test_database.test_table"
        )


def test_mysql_database_engine_comment(started_cluster):
    with contextlib.closing(
        MySQLNodeInstance(
            started_cluster,
            "mysql80",
            "root", mysql_pass, started_cluster.mysql8_ip, started_cluster.mysql8_port
        )
    ) as mysql_node:
        mysql_node.query("DROP DATABASE IF EXISTS test_database")
        mysql_node.query("CREATE DATABASE test_database DEFAULT CHARACTER SET 'utf8'")

        clickhouse_node.query("DROP DATABASE IF EXISTS test_database")
        clickhouse_node.query(
            f"CREATE DATABASE test_database ENGINE = MySQL('mysql80:3306', 'test_database', 'root', '{mysql_pass}') \
            comment 'test mysql database engine comment'"
        )
        assert "test_database" in clickhouse_node.query("SHOW DATABASES")

        assert (
            clickhouse_node.query("SELECT comment FROM system.databases WHERE name='test_database'").rstrip()
            == "test mysql database engine comment"
        )

        clickhouse_node.query(
           "ALTER DATABASE test_database MODIFY COMMENT 'new comment on mysql database engine'"
        )

        assert (
            clickhouse_node.query("SELECT comment FROM system.databases WHERE name='test_database'").rstrip()
            == "new comment on mysql database engine"
        )

        clickhouse_node.query("DROP DATABASE test_database")
        assert "test_database" not in clickhouse_node.query("SHOW DATABASES")

        mysql_node.query("DROP DATABASE test_database")


def test_backup_database(started_cluster):
    with contextlib.closing(
        MySQLNodeInstance(
            started_cluster,
            "mysql80",
            "root", mysql_pass, started_cluster.mysql8_ip, started_cluster.mysql8_port
        )
    ) as mysql_node:
        mysql_node.query("DROP DATABASE IF EXISTS backup_database")
        mysql_node.query("CREATE DATABASE backup_database DEFAULT CHARACTER SET 'utf8'")

        clickhouse_node.query("DROP DATABASE IF EXISTS backup_database")
        clickhouse_node.query(
            f"CREATE DATABASE backup_database ENGINE = MySQL('mysql80:3306', 'backup_database', 'root', '{mysql_pass}')"
        )

        backup_id = uuid.uuid4().hex
        backup_name = f"File('/backups/test_backup_{backup_id}/')"

        clickhouse_node.query(f"BACKUP DATABASE backup_database TO {backup_name}")
        clickhouse_node.query("DROP DATABASE backup_database SYNC")
        assert "backup_database" not in clickhouse_node.query("SHOW DATABASES")

        clickhouse_node.query(f"RESTORE DATABASE backup_database FROM {backup_name}")
        assert (
            clickhouse_node.query("SHOW CREATE DATABASE backup_database")
            == "CREATE DATABASE backup_database\\nENGINE = MySQL(\\'mysql80:3306\\', \\'backup_database\\', \\'root\\', \\'[HIDDEN]\\')\n"
        )

        clickhouse_node.query("DROP DATABASE backup_database")
        mysql_node.query("DROP DATABASE backup_database")


def test_mysql_detached_table_reconciliation(started_cluster):
    """
    Test that detached tables are reconciled against remote MySQL schema.

    Scenario:
    1. Create MySQL database in ClickHouse pointing to remote MySQL
    2. Create a table on remote MySQL
    3. DETACH the table in ClickHouse (it appears in system.detached_tables)
    4. DROP the table on remote MySQL
    5. Trigger schema refresh in ClickHouse (any query that calls fetchTablesIntoLocalCache)
    6. Verify the table no longer appears in system.detached_tables

    This tests the reconciliation logic added in destroyLocalCacheExtraTables
    that mirrors DatabasePostgreSQL::removeOutdatedTables behavior.
    """
    with contextlib.closing(
        MySQLNodeInstance(
            started_cluster,
            "mysql80",
            "root", mysql_pass, started_cluster.mysql8_ip, started_cluster.mysql8_port
        )
    ) as mysql_node:
        mysql_node.query("DROP DATABASE IF EXISTS test_reconcile")
        mysql_node.query("CREATE DATABASE test_reconcile DEFAULT CHARACTER SET 'utf8'")
        mysql_node.query(
            "CREATE TABLE test_reconcile.test_table "
            "(id INT NOT NULL PRIMARY KEY, value VARCHAR(50)) ENGINE=InnoDB"
        )
        mysql_node.query("INSERT INTO test_reconcile.test_table VALUES (1, 'test')")

        clickhouse_node.query("DROP DATABASE IF EXISTS test_reconcile")
        clickhouse_node.query(
            f"CREATE DATABASE test_reconcile ENGINE = MySQL('mysql80:3306', 'test_reconcile', 'root', '{mysql_pass}')"
        )

        assert "test_table" in clickhouse_node.query("SHOW TABLES FROM test_reconcile")
        count = clickhouse_node.query("SELECT count() FROM test_reconcile.test_table").strip()
        assert count == "1", f"Expected 1 row, got {count}"

        clickhouse_node.query("DETACH TABLE test_reconcile.test_table")

        detached_count = clickhouse_node.query(
            "SELECT count() FROM system.detached_tables "
            "WHERE database = 'test_reconcile' AND table = 'test_table'"
        ).strip()
        assert detached_count == "1", f"Expected table in detached_tables, got count {detached_count}"

        is_perm = clickhouse_node.query(
            "SELECT is_permanently FROM system.detached_tables "
            "WHERE database = 'test_reconcile' AND table = 'test_table'"
        ).strip()
        assert is_perm == "0", f"Expected is_permanently=0, got {is_perm}"

        mysql_node.query("DROP TABLE test_reconcile.test_table")

        mysql_tables = mysql_node.query("SHOW TABLES FROM test_reconcile")
        assert "test_table" not in mysql_tables, "Table should be dropped from MySQL"

        clickhouse_node.query("SHOW TABLES FROM test_reconcile")

        try:
            clickhouse_node.query("SHOW CREATE TABLE test_reconcile.nonexistent")
        except QueryRuntimeException:
            pass

        detached_count_after = clickhouse_node.query(
            "SELECT count() FROM system.detached_tables "
            "WHERE database = 'test_reconcile' AND table = 'test_table'"
        ).strip()
        assert detached_count_after == "0", (
            f"Expected table to disappear from detached_tables after reconciliation, "
            f"but count is {detached_count_after}"
        )

        clickhouse_node.query("DROP DATABASE test_reconcile")
        mysql_node.query("DROP DATABASE test_reconcile")


def test_mysql_detached_table_reconciliation_permanent(started_cluster):
    """
    Test reconciliation for PERMANENTLY detached tables.

    Verifies that:
    1. The .remove_flag marker file is preserved even when the remote table is dropped
    2. Permanent detach shows is_permanently=1 before and after reconciliation
    """
    with contextlib.closing(
        MySQLNodeInstance(
            started_cluster,
            "mysql80",
            "root", mysql_pass, started_cluster.mysql8_ip, started_cluster.mysql8_port
        )
    ) as mysql_node:
        mysql_node.query("DROP DATABASE IF EXISTS test_reconcile_perm")
        mysql_node.query("CREATE DATABASE test_reconcile_perm DEFAULT CHARACTER SET 'utf8'")
        mysql_node.query(
            "CREATE TABLE test_reconcile_perm.perm_table "
            "(id INT NOT NULL PRIMARY KEY) ENGINE=InnoDB"
        )

        clickhouse_node.query("DROP DATABASE IF EXISTS test_reconcile_perm")
        clickhouse_node.query(
            f"CREATE DATABASE test_reconcile_perm ENGINE = MySQL('mysql80:3306', 'test_reconcile_perm', 'root', '{mysql_pass}')"
        )

        assert "perm_table" in clickhouse_node.query("SHOW TABLES FROM test_reconcile_perm")

        clickhouse_node.query("DETACH TABLE test_reconcile_perm.perm_table PERMANENTLY")

        is_perm = clickhouse_node.query(
            "SELECT is_permanently FROM system.detached_tables "
            "WHERE database = 'test_reconcile_perm' AND table = 'perm_table'"
        ).strip()
        assert is_perm == "1", f"Expected is_permanently=1 for PERMANENTLY detached, got {is_perm}"

        mysql_node.query("DROP TABLE test_reconcile_perm.perm_table")

        clickhouse_node.query("SHOW TABLES FROM test_reconcile_perm")

        detached_result = clickhouse_node.query(
            "SELECT table, is_permanently FROM system.detached_tables "
            "WHERE database = 'test_reconcile_perm' AND table = 'perm_table' "
            "FORMAT TabSeparated"
        ).strip()
        assert detached_result == "perm_table\t1", (
            f"Expected permanent detach marker to be preserved after remote drop. "
            f"Expected 'perm_table\\t1', got '{detached_result}'"
        )

        clickhouse_node.query("DROP DATABASE test_reconcile_perm")
        mysql_node.query("DROP DATABASE test_reconcile_perm")


def test_permanent_detach_marker_preserved_after_remote_drop(started_cluster):
    """
    Regression test for permanent detach marker preservation and ordinary detach pruning.

    When a table is permanently detached (via DETACH TABLE PERMANENTLY or DROP TABLE),
    and the remote MySQL table is subsequently dropped, the .remove_flag marker should
    be PRESERVED (not deleted by reconciliation). If a same-name table is later recreated
    remotely, it should stay hidden in ClickHouse until explicit ATTACH TABLE.

    Conversely, ordinary DETACH TABLE entries (without PERMANENTLY) should be PRUNED when
    the remote table disappears, as there is nothing left to ATTACH.

    This test verifies that both on-demand reconciliation (via SHOW TABLES or similar)
    and background reconciliation (cleanOutdatedTables thread) correctly:
    1. Preserve permanent detach markers (.remove_flag files)
    2. Prune ordinary detach entries (no marker)

    The test creates BOTH a permanent and an ordinary detach to ensure has_non_permanent_detach
    evaluates to true, which gates the background reconciliation block in DatabaseMySQL.cpp.
    """
    with contextlib.closing(
        MySQLNodeInstance(
            started_cluster,
            "mysql80",
            "root", mysql_pass, started_cluster.mysql8_ip, started_cluster.mysql8_port
        )
    ) as mysql_node:
        mysql_node.query("DROP DATABASE IF EXISTS test_perm_marker")
        mysql_node.query("CREATE DATABASE test_perm_marker DEFAULT CHARACTER SET 'utf8'")
        mysql_node.query(
            "CREATE TABLE test_perm_marker.permanent_table "
            "(id INT NOT NULL PRIMARY KEY, value VARCHAR(100)) ENGINE=InnoDB"
        )
        mysql_node.query(
            "CREATE TABLE test_perm_marker.ordinary_table "
            "(id INT NOT NULL PRIMARY KEY, value VARCHAR(100)) ENGINE=InnoDB"
        )

        clickhouse_node.query("DROP DATABASE IF EXISTS test_perm_marker")
        clickhouse_node.query(
            f"CREATE DATABASE test_perm_marker ENGINE = MySQL('mysql80:3306', 'test_perm_marker', 'root', '{mysql_pass}')"
        )

        # Verify both tables are visible initially
        tables = clickhouse_node.query("SHOW TABLES FROM test_perm_marker")
        assert "permanent_table" in tables
        assert "ordinary_table" in tables

        # Permanently detach one table (creates .remove_flag marker)
        clickhouse_node.query("DETACH TABLE test_perm_marker.permanent_table PERMANENTLY")

        # Ordinary detach the other table (no marker)
        clickhouse_node.query("DETACH TABLE test_perm_marker.ordinary_table")

        # Verify both show in system.detached_tables with correct is_permanently values
        detached_permanent = clickhouse_node.query(
            "SELECT table, is_permanently FROM system.detached_tables "
            "WHERE database = 'test_perm_marker' AND table = 'permanent_table' "
            "FORMAT TabSeparated"
        ).strip()
        assert detached_permanent == "permanent_table\t1", (
            f"Expected 'permanent_table\\t1', got '{detached_permanent}'"
        )

        detached_ordinary = clickhouse_node.query(
            "SELECT table, is_permanently FROM system.detached_tables "
            "WHERE database = 'test_perm_marker' AND table = 'ordinary_table' "
            "FORMAT TabSeparated"
        ).strip()
        assert detached_ordinary == "ordinary_table\t0", (
            f"Expected 'ordinary_table\\t0', got '{detached_ordinary}'"
        )

        # Drop BOTH remote MySQL tables
        mysql_node.query("DROP TABLE test_perm_marker.permanent_table")
        mysql_node.query("DROP TABLE test_perm_marker.ordinary_table")

        # Wait for background reconciliation thread to run (cleanOutdatedTables sleeps 30s between runs)
        # DO NOT call SHOW TABLES or any other on-demand trigger here - that would bypass the background
        # reconciliation path we're testing (destroyLocalCacheExtraTables would prune ordinary_table
        # immediately, causing has_non_permanent_detach to become false, which skips the background fetch).
        # Use polling pattern to ensure background cleaner genuinely executes its fetch+reconcile path
        max_wait = 65
        start_time = time.time()
        bg_reconciliation_verified = False

        while time.time() - start_time < max_wait:
            detached_tables = clickhouse_node.query(
                "SELECT table, is_permanently FROM system.detached_tables "
                "WHERE database = 'test_perm_marker' "
                "ORDER BY table FORMAT TabSeparated"
            ).strip()

            # Expected state after background reconciliation:
            # - permanent_table still present with is_permanently=1 (marker preserved)
            # - ordinary_table removed (entry pruned)
            if detached_tables == "permanent_table\t1":
                bg_reconciliation_verified = True
                break

            time.sleep(5)  # Check every 5 seconds

        assert bg_reconciliation_verified, (
            f"REGRESSION: Background reconciliation did not complete correctly within {max_wait}s. "
            f"Expected only 'permanent_table\\t1', final state: '{detached_tables}'"
        )

        # Now recreate the remote permanent_table with the same name to verify marker preservation
        # prevents it from reappearing
        mysql_node.query(
            "CREATE TABLE test_perm_marker.permanent_table "
            "(id INT NOT NULL PRIMARY KEY, value VARCHAR(100)) ENGINE=InnoDB"
        )
        mysql_node.query("INSERT INTO test_perm_marker.permanent_table VALUES (1, 'should not appear')")

        # Trigger schema refresh
        clickhouse_node.query("SHOW TABLES FROM test_perm_marker")

        # The table should still NOT appear in ClickHouse (preserved permanent detach)
        visible_tables = clickhouse_node.query("SHOW TABLES FROM test_perm_marker").strip()
        assert "permanent_table" not in visible_tables, (
            f"REGRESSION: Permanently detached table reappeared after remote table was recreated. "
            f"Tables: {visible_tables}"
        )

        # Verify it's still in detached_tables
        still_detached = clickhouse_node.query(
            "SELECT table, is_permanently FROM system.detached_tables "
            "WHERE database = 'test_perm_marker' AND table = 'permanent_table' "
            "FORMAT TabSeparated"
        ).strip()
        assert still_detached == "permanent_table\t1", (
            f"Expected 'permanent_table\\t1' after remote recreation, got '{still_detached}'"
        )

        # The table should only become visible again after explicit ATTACH TABLE
        clickhouse_node.query("ATTACH TABLE test_perm_marker.permanent_table")

        # Now it should be visible
        assert "permanent_table" in clickhouse_node.query("SHOW TABLES FROM test_perm_marker")

        # And no longer in detached_tables
        final_detached_count = clickhouse_node.query(
            "SELECT count() FROM system.detached_tables "
            "WHERE database = 'test_perm_marker' AND table = 'permanent_table'"
        ).strip()
        assert final_detached_count == "0", (
            f"Expected count=0 after ATTACH, got {final_detached_count}"
        )

        # Verify we can query the data
        result = clickhouse_node.query("SELECT value FROM test_perm_marker.permanent_table WHERE id = 1").strip()
        assert result == "should not appear"

        # Cleanup
        clickhouse_node.query("DROP DATABASE test_perm_marker")
        mysql_node.query("DROP DATABASE test_perm_marker")
