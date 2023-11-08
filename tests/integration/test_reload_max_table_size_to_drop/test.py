import os
import shutil
import time
import contextlib

import pytest
from helpers.cluster import ClickHouseCluster
from helpers.client import QueryRuntimeException
from functools import partial

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/max_table_size_to_drop.xml"])

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        node.query(
            "CREATE TABLE test(date Date, id UInt32) ENGINE = MergeTree() PARTITION BY date ORDER BY id"
        )
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture
def reset_config():
    """
    Reset configuration with max_table/partition_size_to_drop
    """
    config_file = "max_table_size_to_drop.xml"
    shutil.copy(
        os.path.join(os.path.dirname(__file__), f"configs/{config_file}"),
        os.path.join(node.config_d_dir, config_file),
    )

    node.query("SYSTEM RELOAD CONFIG")


def test_reload_max_table_size_to_drop(start_cluster, reset_config):
    node.query("INSERT INTO test VALUES (now(), 0)")
    config_path = os.path.join(
        SCRIPT_DIR,
        "./{}/node/configs/config.d/max_table_size_to_drop.xml".format(
            start_cluster.instances_dir_name
        ),
    )

    time.sleep(5)  # wait for data part commit

    drop = node.get_query_request("DROP TABLE test SYNC")
    out, err = drop.get_answer_and_error()
    assert out == ""
    assert "Table or Partition in default.test was not dropped." in err

    config = open(config_path, "r")
    config_lines = config.readlines()
    config.close()
    config_lines = [
        line.replace("<max_table_size_to_drop>1", "<max_table_size_to_drop>1000000")
        for line in config_lines
    ]
    config = open(config_path, "w")
    config.writelines(config_lines)
    config.close()

    node.query("SYSTEM RELOAD CONFIG")

    drop = node.get_query_request("DROP TABLE test")
    out, err = drop.get_answer_and_error()
    assert out == ""
    assert err == ""


def test_max_table_size_to_drop_for_engine_settings(start_cluster, reset_config):
    node.query(
        "CREATE TABLE data(date Date, id UInt32) ENGINE = MergeTree() ORDER BY tuple()"
    )
    node.query("INSERT INTO data VALUES (now(), 0)")

    node.query(
        "CREATE TABLE data2(date Date, id UInt32) ENGINE = MergeTree() ORDER BY tuple() SETTINGS max_table_size_to_drop = 10000"
    )
    node.query("INSERT INTO data2 VALUES (now(), 0)")

    # wait_table_size_changed("data2", lambda x: x > 1)
    # Table can be dropped since engine max_table_size_to_drop setting overwrite server setting
    node.query("DROP TABLE data2 SYNC")

    assert "data2" not in node.query("show tables")

    # But can't still delete `data` table since it uses server setting
    with pytest.raises(QueryRuntimeException) as exc:
        node.query("DROP TABLE data SYNC")

    assert "TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT" in str(exc)


def test_max_table_size_to_drop_for_engine_settings_with_reset(
    start_cluster, reset_config
):
    node.query(
        "CREATE TABLE data2(date Date, id UInt32) ENGINE = MergeTree() ORDER BY tuple() SETTINGS max_table_size_to_drop = 10000"
    )
    node.query("INSERT INTO data2 VALUES (now(), 0)")

    # reset settings to default, so server-wise setting will be used to check the max table size to drop
    node.query("ALTER TABLE data2 RESET SETTING max_table_size_to_drop")

    with pytest.raises(QueryRuntimeException) as exc:
        node.query("DROP TABLE data2 SYNC")

    assert "TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT" in str(exc)


def test_max_partition_size_to_drop_for_table(start_cluster, reset_config):
    node.query(
        "CREATE TABLE data3(date Date, id UInt32) ENGINE = MergeTree() PARTITION BY date ORDER BY tuple()"
    )
    node.query("INSERT INTO data3 VALUES ('2022-01-01', 0)")

    with pytest.raises(QueryRuntimeException) as exc:
        node.query("ALTER TABLE data3 DROP PARTITION '2022-01-01'")

    assert "TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT" in str(exc)

    node.query("ALTER TABLE data3 MODIFY SETTING max_partition_size_to_drop=1000")

    node.query("ALTER TABLE data3 DROP PARTITION '2022-01-01'")

    assert int(node.query("SELECT count(id) FROM data3")) == 0
