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
