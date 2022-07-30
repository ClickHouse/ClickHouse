import logging
import os

import time
import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "h0_0_0",
            main_configs=["configs/config.xml"],
            extra_configs=["configs/hdfs-site.xml", "data/prepare_hive_data.sh"],
            with_hive=True,
        )

        logging.info("Starting cluster ...")
        cluster.start()
        cluster.copy_file_to_container(
            "roottesthivequery_hdfs1_1",
            "/ClickHouse/tests/integration/test_hive_query/data/prepare_hive_data.sh",
            "/prepare_hive_data.sh",
        )
        cluster.exec_in_container(
            "roottesthivequery_hdfs1_1", ["bash", "-c", "bash /prepare_hive_data.sh"]
        )
        yield cluster
    finally:
        cluster.shutdown()


def test_create_parquet_table(started_cluster):
    logging.info("Start testing creating hive table ...")
    node = started_cluster.instances["h0_0_0"]
    test_passed = False
    for i in range(10):
        node.query("set input_format_parquet_allow_missing_columns = true")
        result = node.query(
            """
DROP TABLE IF EXISTS default.demo_parquet;
CREATE TABLE default.demo_parquet (`id` Nullable(String), `score` Nullable(Int32), `day` Nullable(String)) ENGINE = Hive('thrift://hivetest:9083', 'test', 'demo') PARTITION BY(day)
            """
        )
        logging.info("create result {}".format(result))
        if result.strip() == "":
            test_passed = True
            break
        time.sleep(60)
    assert test_passed


def test_create_parquet_table_1(started_cluster):
    logging.info("Start testing creating hive table ...")
    node = started_cluster.instances["h0_0_0"]
    for i in range(10):
        node.query("set input_format_parquet_allow_missing_columns = true")
        result = node.query(
            """
DROP TABLE IF EXISTS default.demo_parquet_parts;
CREATE TABLE default.demo_parquet_parts (`id` Nullable(String), `score` Nullable(Int32), `day` Nullable(String), `hour` String) ENGINE = Hive('thrift://hivetest:9083', 'test', 'parquet_demo') PARTITION BY(day, hour);
            """
        )
        logging.info("create result {}".format(result))
        if result.strip() == "":
            test_passed = True
            break
        time.sleep(60)
    assert test_passed


def test_create_orc_table(started_cluster):
    logging.info("Start testing creating hive table ...")
    node = started_cluster.instances["h0_0_0"]
    test_passed = False
    for i in range(10):
        result = node.query(
            """
    DROP TABLE IF EXISTS default.demo_orc;
    CREATE TABLE default.demo_orc (`id` Nullable(String), `score` Nullable(Int32), `day` Nullable(String)) ENGINE = Hive('thrift://hivetest:9083', 'test', 'demo_orc') PARTITION BY(day)
            """
        )
        logging.info("create result {}".format(result))
        if result.strip() == "":
            test_passed = True
            break
        time.sleep(60)

    assert test_passed


def test_create_text_table(started_cluster):
    logging.info("Start testing creating hive table ...")
    node = started_cluster.instances["h0_0_0"]
    result = node.query(
        """
    DROP TABLE IF EXISTS default.demo_text;
    CREATE TABLE default.demo_text (`id` Nullable(String), `score` Nullable(Int32), `day` Nullable(String)) ENGINE = Hive('thrift://hivetest:9083', 'test', 'demo_text') PARTITION BY (tuple())
            """
    )
    logging.info("create result {}".format(result))

    assert result.strip() == ""


def test_parquet_groupby(started_cluster):
    logging.info("Start testing groupby ...")
    node = started_cluster.instances["h0_0_0"]
    result = node.query(
        """
    SELECT day, count(*) FROM default.demo_parquet group by day order by day
            """
    )
    expected_result = """2021-11-01	1
2021-11-05	2
2021-11-11	1
2021-11-16	2
"""
    assert result == expected_result


def test_parquet_in_filter(started_cluster):
    logging.info("Start testing groupby ...")
    node = started_cluster.instances["h0_0_0"]
    result = node.query(
        """
    SELECT count(*) FROM default.demo_parquet_parts where day = '2021-11-05' and hour in ('00')
            """
    )
    expected_result = """2
"""
    logging.info("query result:{}".format(result))
    assert result == expected_result


def test_orc_groupby(started_cluster):
    logging.info("Start testing groupby ...")
    node = started_cluster.instances["h0_0_0"]
    result = node.query(
        """
    SELECT day, count(*) FROM default.demo_orc group by day order by day
            """
    )
    expected_result = """2021-11-01	1
2021-11-05	2
2021-11-11	1
2021-11-16	2
"""
    assert result == expected_result


@pytest.mark.parametrize(
    "table,use_local_cache_for_remote_storage,enable_orc_file_minmax_index,enable_orc_stripe_minmax_index",
    [
        pytest.param(
            "demo_orc_no_cache_no_index",
            "false",
            "false",
            "false",
            id="demo_orc_no_cache_no_index",
        ),
        pytest.param(
            "demo_orc_with_cache_no_index",
            "true",
            "false",
            "false",
            id="demo_orc_with_cache_no_index",
        ),
        pytest.param(
            "demo_orc_no_cache_file_index",
            "false",
            "true",
            "false",
            id="demo_orc_no_cache_file_index",
        ),
        pytest.param(
            "demo_orc_with_cache_file_index",
            "true",
            "true",
            "false",
            id="demo_orc_with_cache_file_index",
        ),
        pytest.param(
            "demo_orc_no_cache_stripe_index",
            "false",
            "true",
            "true",
            id="demo_orc_no_cache_stripe_index",
        ),
        pytest.param(
            "demo_orc_with_cache_stripe_index",
            "true",
            "true",
            "true",
            id="demo_orc_with_cache_stripe_index",
        ),
    ],
)
def test_orc_minmax_index(
    started_cluster,
    table,
    use_local_cache_for_remote_storage,
    enable_orc_file_minmax_index,
    enable_orc_stripe_minmax_index,
):
    node = started_cluster.instances["h0_0_0"]
    result = node.query(
        """
        DROP TABLE IF EXISTS default.{table};
        CREATE TABLE default.{table} (`id` Nullable(String), `score` Nullable(Int32), `day` Nullable(String)) ENGINE = Hive('thrift://hivetest:9083', 'test', 'demo_orc') PARTITION BY(day)
        SETTINGS enable_orc_file_minmax_index = {enable_orc_file_minmax_index}, enable_orc_stripe_minmax_index = {enable_orc_stripe_minmax_index};
    """.format(
            table=table,
            enable_orc_file_minmax_index=enable_orc_file_minmax_index,
            enable_orc_stripe_minmax_index=enable_orc_stripe_minmax_index,
        )
    )
    assert result.strip() == ""

    for i in range(2):
        result = node.query(
            """
            SELECT day, id, score FROM default.{table} where day >= '2021-11-05' and day <= '2021-11-16' and score >= 15 and score <= 30 order by day, id
            SETTINGS use_local_cache_for_remote_storage = {use_local_cache_for_remote_storage}
            """.format(
                table=table,
                use_local_cache_for_remote_storage=use_local_cache_for_remote_storage,
            )
        )

        assert (
            result
            == """2021-11-05	abd	15
2021-11-16	aaa	22
"""
        )


@pytest.mark.parametrize(
    "table,use_local_cache_for_remote_storage,enable_parquet_rowgroup_minmax_index",
    [
        pytest.param(
            "demo_parquet_no_cache_no_index",
            "false",
            "false",
            id="demo_parquet_no_cache_no_index",
        ),
        pytest.param(
            "demo_parquet_with_cache_no_index",
            "true",
            "false",
            id="demo_parquet_with_cache_no_index",
        ),
        pytest.param(
            "demo_parquet_no_cache_rowgroup_index",
            "false",
            "true",
            id="demo_parquet_no_cache_rowgroup_index",
        ),
        pytest.param(
            "demo_parquet_with_cache_rowgroup_index",
            "true",
            "true",
            id="demo_parquet_with_cache_rowgroup_index",
        ),
    ],
)
def test_parquet_minmax_index(
    started_cluster,
    table,
    use_local_cache_for_remote_storage,
    enable_parquet_rowgroup_minmax_index,
):
    node = started_cluster.instances["h0_0_0"]
    result = node.query(
        """
        DROP TABLE IF EXISTS default.{table};
        CREATE TABLE default.{table} (`id` Nullable(String), `score` Nullable(Int32), `day` Nullable(String)) ENGINE = Hive('thrift://hivetest:9083', 'test', 'demo') PARTITION BY(day)
        SETTINGS enable_parquet_rowgroup_minmax_index = {enable_parquet_rowgroup_minmax_index}
    """.format(
            table=table,
            enable_parquet_rowgroup_minmax_index=enable_parquet_rowgroup_minmax_index,
        )
    )
    assert result.strip() == ""

    for i in range(2):
        result = node.query(
            """
            SELECT day, id, score FROM default.{table} where day >= '2021-11-05' and day <= '2021-11-16' and score >= 15 and score <= 30 order by day, id
            SETTINGS use_local_cache_for_remote_storage = {use_local_cache_for_remote_storage}
            """.format(
                table=table,
                use_local_cache_for_remote_storage=use_local_cache_for_remote_storage,
            )
        )

        assert (
            result
            == """2021-11-05	abd	15
2021-11-16	aaa	22
"""
        )


def test_hive_columns_prunning(started_cluster):
    logging.info("Start testing groupby ...")
    node = started_cluster.instances["h0_0_0"]
    result = node.query(
        """
    SELECT count(*) FROM default.demo_parquet_parts where day = '2021-11-05'
            """
    )
    expected_result = """4
"""
    logging.info("query result:{}".format(result))
    assert result == expected_result


def test_text_count(started_cluster):
    node = started_cluster.instances["h0_0_0"]
    result = node.query(
        """
    SELECT day, count(*) FROM default.demo_orc group by day order by day SETTINGS format_csv_delimiter = '\x01'
            """
    )
    expected_result = """2021-11-01	1
2021-11-05	2
2021-11-11	1
2021-11-16	2
"""
    assert result == expected_result


def test_parquet_groupby_with_cache(started_cluster):
    logging.info("Start testing groupby ...")
    node = started_cluster.instances["h0_0_0"]
    result = node.query(
        """
    SELECT day, count(*) FROM default.demo_parquet group by day order by day
            """
    )
    expected_result = """2021-11-01	1
2021-11-05	2
2021-11-11	1
2021-11-16	2
"""
    assert result == expected_result


def test_parquet_groupby_by_hive_function(started_cluster):
    logging.info("Start testing groupby ...")
    node = started_cluster.instances["h0_0_0"]
    result = node.query(
        """
    SELECT day, count(*) FROM hive('thrift://hivetest:9083', 'test', 'demo', '`id` Nullable(String), `score` Nullable(Int32), `day` Nullable(String)', 'day') group by day order by day
            """
    )
    expected_result = """2021-11-01	1
2021-11-05	2
2021-11-11	1
2021-11-16	2
"""
    assert result == expected_result


def test_cache_read_bytes(started_cluster):
    node = started_cluster.instances["h0_0_0"]
    result = node.query(
        """
    CREATE TABLE IF NOT EXISTS default.demo_parquet_1 (`id` Nullable(String), `score` Nullable(Int32), `day` Nullable(String)) ENGINE = Hive('thrift://hivetest:9083', 'test', 'demo') PARTITION BY(day)
            """
    )
    test_passed = False
    for i in range(10):
        result = node.query(
            """
    SELECT * FROM default.demo_parquet_1 settings input_format_parquet_allow_missing_columns = true
            """
        )
        node.query("system flush logs")
        result = node.query(
            "select sum(ProfileEvent_ExternalDataSourceLocalCacheReadBytes)  from system.metric_log where ProfileEvent_ExternalDataSourceLocalCacheReadBytes > 0"
        )
        if result.strip() == "0":
            logging.info("ProfileEvent_ExternalDataSourceLocalCacheReadBytes == 0")
            time.sleep(10)
            continue
        test_passed = True
        break
    assert test_passed


def test_cache_dir_use(started_cluster):
    node = started_cluster.instances["h0_0_0"]
    result0 = node.exec_in_container(
        ["bash", "-c", "ls /tmp/clickhouse_local_cache | wc -l"]
    )
    result1 = node.exec_in_container(
        ["bash", "-c", "ls /tmp/clickhouse_local_cache1 | wc -l"]
    )
    assert result0 != "0" and result1 != "0"


def test_hive_struct_type(started_cluster):
    node = started_cluster.instances["h0_0_0"]
    result = node.query(
        """
        CREATE TABLE IF NOT EXISTS default.test_hive_types (`f_tinyint` Int8, `f_smallint` Int16, `f_int` Int32, `f_integer` Int32, `f_bigint` Int64, `f_float` Float32, `f_double` Float64, `f_decimal` Float64, `f_timestamp` DateTime, `f_date` Date, `f_string` String, `f_varchar` String, `f_char` String, `f_bool` Boolean, `f_array_int` Array(Int32), `f_array_string` Array(String), `f_array_float` Array(Float32), `f_map_int` Map(String, Int32), `f_map_string` Map(String, String), `f_map_float` Map(String, Float32), `f_struct` Tuple(a String, b Int32, c Float32, d Tuple(x Int32, y String)), `day` String) ENGINE = Hive('thrift://hivetest:9083', 'test', 'test_hive_types') PARTITION BY (day)
        """
    )
    result = node.query(
        """
    SELECT * FROM default.test_hive_types WHERE day = '2022-02-20' SETTINGS input_format_parquet_import_nested=1
        """
    )
    expected_result = """1	2	3	4	5	6.11	7.22	8	2022-02-20 14:47:04	2022-02-20	hello world	hello world	hello world	true	[1,2,3]	['hello world','hello world']	[1.1,1.2]	{'a':100,'b':200,'c':300}	{'a':'aa','b':'bb','c':'cc'}	{'a':111.1,'b':222.2,'c':333.3}	('aaa',200,333.3,(10,'xyz'))	2022-02-20"""
    assert result.strip() == expected_result

    result = node.query(
        """
    SELECT day, f_struct.a, f_struct.d.x FROM default.test_hive_types WHERE day = '2022-02-20' SETTINGS input_format_parquet_import_nested=1
        """
    )
    expected_result = """2022-02-20	aaa	10"""


def test_table_alter_add(started_cluster):
    node = started_cluster.instances["h0_0_0"]
    result = node.query("DROP TABLE IF EXISTS default.demo_parquet_1")
    result = node.query(
        """
CREATE TABLE IF NOT EXISTS default.demo_parquet_1 (`score` Nullable(Int32), `day` Nullable(String)) ENGINE = Hive('thrift://hivetest:9083', 'test', 'demo') PARTITION BY(day)
        """
    )
    result = node.query(
        """
ALTER TABLE default.demo_parquet_1 ADD COLUMN id Nullable(String) FIRST
        """
    )
    result = node.query("""DESC default.demo_parquet_1 FORMAT TSV""")

    expected_result = "id\tNullable(String)\t\t\t\t\t\nscore\tNullable(Int32)\t\t\t\t\t\nday\tNullable(String)"
    assert result.strip() == expected_result


def test_table_alter_drop(started_cluster):
    node = started_cluster.instances["h0_0_0"]
    result = node.query("DROP TABLE IF EXISTS default.demo_parquet_1")
    result = node.query(
        """
CREATE TABLE IF NOT EXISTS default.demo_parquet_1 (`id` Nullable(String), `score` Nullable(Int32), `day` Nullable(String)) ENGINE = Hive('thrift://hivetest:9083', 'test', 'demo') PARTITION BY(day)
        """
    )
    result = node.query(
        """
ALTER TABLE default.demo_parquet_1 DROP COLUMN id
        """
    )

    result = node.query("""DESC default.demo_parquet_1 FORMAT TSV""")
    expected_result = """score\tNullable(Int32)\t\t\t\t\t\nday\tNullable(String)"""
    assert result.strip() == expected_result


def test_table_alter_comment(started_cluster):
    node = started_cluster.instances["h0_0_0"]
    result = node.query("DROP TABLE IF EXISTS default.demo_parquet_1")
    result = node.query(
        """
CREATE TABLE IF NOT EXISTS default.demo_parquet_1 (`id` Nullable(String), `score` Nullable(Int32), `day` Nullable(String)) ENGINE = Hive('thrift://hivetest:9083', 'test', 'demo') PARTITION BY(day)
        """
    )

    result = node.query(
        """ALTER TABLE default.demo_parquet_1 COMMENT COLUMN id 'Text comment'"""
    )
    result = node.query("""DESC default.demo_parquet_1 FORMAT TSV""")
    expected_result = """id\tNullable(String)\t\t\tText comment\t\t\nscore\tNullable(Int32)\t\t\t\t\t\nday\tNullable(String)"""
    assert result.strip() == expected_result
