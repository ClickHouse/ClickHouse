import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

instance = cluster.add_instance(
    "instance",
    stay_alive=True,
    main_configs=["config/models_config.xml", "config/logger_library_bridge.xml"],
)


@pytest.fixture(scope="module")
def ch_cluster():
    try:
        cluster.start()

        instance.exec_in_container(["mkdir", f"/etc/clickhouse-server/model/"])

        machine = instance.get_machine_name()
        for source_name in os.listdir(os.path.join(SCRIPT_DIR, "model/.")):
            dest_name = source_name
            if machine in source_name:
                machine_suffix = "_" + machine
                dest_name = source_name[: -len(machine_suffix)]

            os.system(
                "docker cp {local} {cont_id}:{dist}".format(
                    local=os.path.join(SCRIPT_DIR, f"model/{source_name}"),
                    cont_id=instance.docker_id,
                    dist=f"/etc/clickhouse-server/model/{dest_name}",
                )
            )

        instance.restart_clickhouse()

        yield cluster

    finally:
        cluster.shutdown()


# ---------------------------------------------------------------------------
# simple_model.bin has 2 float features and 9 categorical features


def testConstantFeatures(ch_cluster):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")

    result = instance.query("system reload models")

    result = instance.query(
        "select catboostEvaluate('/etc/clickhouse-server/model/simple_model.bin', 1.0, 2.0, 3, 4, 5, 6, 7, 8, 9, 10, 11);"
    )
    expected = "-1.930268705869267\n"
    assert result == expected


def testNonConstantFeatures(ch_cluster):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")

    result = instance.query("system reload models")

    instance.query("DROP TABLE IF EXISTS T;")
    instance.query(
        "CREATE TABLE T(ID UInt32, F1 Float32, F2 Float32, F3 UInt32, F4 UInt32, F5 UInt32, F6 UInt32, F7 UInt32, F8 UInt32, F9 Float32, F10 Float32, F11 Float32) ENGINE MergeTree ORDER BY ID;"
    )
    instance.query("INSERT INTO T VALUES(0, 1.0, 2.0, 3, 4, 5, 6, 7, 8, 9, 10, 11);")

    result = instance.query(
        "select catboostEvaluate('/etc/clickhouse-server/model/simple_model.bin', F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11) from T;"
    )
    expected = "-1.930268705869267\n"
    assert result == expected

    instance.query("DROP TABLE IF EXISTS T;")


def testModelPathIsNotAConstString(ch_cluster):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")

    result = instance.query("system reload models")

    err = instance.query_and_get_error(
        "select catboostEvaluate(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);"
    )
    assert (
        "Illegal type UInt8 of first argument of function catboostEvaluate, expected a string"
        in err
    )

    instance.query("DROP TABLE IF EXISTS T;")
    instance.query("CREATE TABLE T(ID UInt32, A String) ENGINE MergeTree ORDER BY ID")
    instance.query("INSERT INTO T VALUES(0, 'test');")
    err = instance.query_and_get_error(
        "select catboostEvaluate(A, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11) FROM T;"
    )
    assert (
        "First argument of function catboostEvaluate must be a constant string" in err
    )
    instance.query("DROP TABLE IF EXISTS T;")


def testWrongNumberOfFeatureArguments(ch_cluster):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")

    result = instance.query("system reload models")

    err = instance.query_and_get_error(
        "select catboostEvaluate('/etc/clickhouse-server/model/simple_model.bin');"
    )
    assert "Function catboostEvaluate expects at least 2 arguments" in err

    err = instance.query_and_get_error(
        "select catboostEvaluate('/etc/clickhouse-server/model/simple_model.bin', 1, 2);"
    )
    assert (
        "Number of columns is different with number of features: columns size 2 float features size 2 + cat features size 9"
        in err
    )


def testFloatFeatureMustBeNumeric(ch_cluster):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")

    result = instance.query("system reload models")

    err = instance.query_and_get_error(
        "select catboostEvaluate('/etc/clickhouse-server/model/simple_model.bin', 1.0, 'a', 3, 4, 5, 6, 7, 8, 9, 10, 11);"
    )
    assert "Column 1 should be numeric to make float feature" in err


def testCategoricalFeatureMustBeNumericOrString(ch_cluster):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")

    result = instance.query("system reload models")

    err = instance.query_and_get_error(
        "select catboostEvaluate('/etc/clickhouse-server/model/simple_model.bin', 1.0, 2.0, 3, 4, 5, 6, 7, tuple(8), 9, 10, 11);"
    )
    assert "Column 7 should be numeric or string" in err


def testOnLowCardinalityFeatures(ch_cluster):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")

    result = instance.query("system reload models")

    # same but on domain-compressed data
    result = instance.query(
        "select catboostEvaluate('/etc/clickhouse-server/model/simple_model.bin', toLowCardinality(1.0), toLowCardinality(2.0), toLowCardinality(3), toLowCardinality(4), toLowCardinality(5), toLowCardinality(6), toLowCardinality(7), toLowCardinality(8), toLowCardinality(9), toLowCardinality(10), toLowCardinality(11));"
    )
    expected = "-1.930268705869267\n"
    assert result == expected


def testOnNullableFeatures(ch_cluster):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")

    result = instance.query("system reload models")

    result = instance.query(
        "select catboostEvaluate('/etc/clickhouse-server/model/simple_model.bin', toNullable(1.0), toNullable(2.0), toNullable(3), toNullable(4), toNullable(5), toNullable(6), toNullable(7), toNullable(8), toNullable(9), toNullable(10), toNullable(11));"
    )
    expected = "-1.930268705869267\n"
    assert result == expected

    # Actual NULLs are disallowed
    err = instance.query_and_get_error(
        "select catboostEvaluate('/etc/clickhouse-server/model/simple_model.bin', toNullable(NULL), toNullable(NULL), toNullable(NULL), toNullable(NULL), toNullable(NULL), toNullable(NULL), toNullable(NULL), toNullable(NULL), toNullable(NULL), toNullable(NULL), toNullable(NULL));"
    )
    assert "Column 0 should be numeric to make float feature" in err


def testInvalidLibraryPath(ch_cluster):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")

    result = instance.query("system reload models")

    # temporarily move library elsewhere
    instance.exec_in_container(
        [
            "bash",
            "-c",
            "mv /etc/clickhouse-server/model/libcatboostmodel.so /etc/clickhouse-server/model/nonexistant.so",
        ]
    )

    err = instance.query_and_get_error(
        "select catboostEvaluate('/etc/clickhouse-server/model/simple_model.bin', 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);"
    )
    assert (
        "Can't load library /etc/clickhouse-server/model/libcatboostmodel.so: file doesn't exist"
        in err
    )

    # restore
    instance.exec_in_container(
        [
            "bash",
            "-c",
            "mv /etc/clickhouse-server/model/nonexistant.so /etc/clickhouse-server/model/libcatboostmodel.so",
        ]
    )


def testInvalidModelPath(ch_cluster):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")

    result = instance.query("system reload models")

    err = instance.query_and_get_error(
        "select catboostEvaluate('', 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);"
    )
    assert "Can't load model : file doesn't exist" in err

    err = instance.query_and_get_error(
        "select catboostEvaluate('model_non_existant.bin', 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);"
    )
    assert "Can't load model model_non_existant.bin: file doesn't exist" in err


def testRecoveryAfterCrash(ch_cluster):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")

    result = instance.query("system reload models")

    result = instance.query(
        "select catboostEvaluate('/etc/clickhouse-server/model/simple_model.bin', 1.0, 2.0, 3, 4, 5, 6, 7, 8, 9, 10, 11);"
    )
    expected = "-1.930268705869267\n"
    assert result == expected

    instance.exec_in_container(
        ["bash", "-c", "kill -9 `pidof clickhouse-library-bridge`"], user="root"
    )

    result = instance.query(
        "select catboostEvaluate('/etc/clickhouse-server/model/simple_model.bin', 1.0, 2.0, 3, 4, 5, 6, 7, 8, 9, 10, 11);"
    )
    assert result == expected


# ---------------------------------------------------------------------------
# amazon_model.bin has 0 float features and 9 categorical features


def testAmazonModelSingleRow(ch_cluster):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")

    result = instance.query("system reload models")

    result = instance.query(
        "select catboostEvaluate('/etc/clickhouse-server/model/amazon_model.bin', 1, 2, 3, 4, 5, 6, 7, 8, 9);"
    )
    expected = "0.7774665009089274\n"
    assert result == expected


def testAmazonModelManyRows(ch_cluster):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")

    result = instance.query("system reload models")

    result = instance.query("drop table if exists amazon")

    result = instance.query(
        "create table amazon ( DATE Date materialized today(), ACTION UInt8, RESOURCE UInt32, MGR_ID UInt32, ROLE_ROLLUP_1 UInt32, ROLE_ROLLUP_2 UInt32, ROLE_DEPTNAME UInt32, ROLE_TITLE UInt32, ROLE_FAMILY_DESC UInt32, ROLE_FAMILY UInt32, ROLE_CODE UInt32) engine = MergeTree order by DATE"
    )

    result = instance.query(
        "insert into amazon select number % 256, number, number, number, number, number, number, number, number, number from numbers(750000)"
    )

    # First compute prediction, then as a very crude way to fingerprint and compare the result: sum and floor
    # (the focus is to test that the exchange of large result sets between the server and the bridge works)
    result = instance.query(
        "SELECT floor(sum(catboostEvaluate('/etc/clickhouse-server/model/amazon_model.bin', RESOURCE, MGR_ID, ROLE_ROLLUP_1, ROLE_ROLLUP_2, ROLE_DEPTNAME, ROLE_TITLE, ROLE_FAMILY_DESC, ROLE_FAMILY, ROLE_CODE))) FROM amazon"
    )

    expected = "583092\n"
    assert result == expected

    result = instance.query("drop table if exists amazon")


def testModelUpdate(ch_cluster):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")

    result = instance.query("system reload models")

    query = "select catboostEvaluate('/etc/clickhouse-server/model/simple_model.bin', 1.0, 2.0, 3, 4, 5, 6, 7, 8, 9, 10, 11);"

    result = instance.query(query)
    expected = "-1.930268705869267\n"
    assert result == expected

    # simulate an update of the model: temporarily move the amazon model in place of the simple model
    instance.exec_in_container(
        [
            "bash",
            "-c",
            "mv /etc/clickhouse-server/model/simple_model.bin /etc/clickhouse-server/model/simple_model.bin.bak",
        ]
    )
    instance.exec_in_container(
        [
            "bash",
            "-c",
            "mv /etc/clickhouse-server/model/amazon_model.bin /etc/clickhouse-server/model/simple_model.bin",
        ]
    )

    # unload simple model
    result = instance.query(
        "system reload model '/etc/clickhouse-server/model/simple_model.bin'"
    )

    # load the simple-model-camouflaged amazon model
    result = instance.query(
        "select catboostEvaluate('/etc/clickhouse-server/model/simple_model.bin', 1, 2, 3, 4, 5, 6, 7, 8, 9);"
    )
    expected = "0.7774665009089274\n"
    assert result == expected

    # restore
    instance.exec_in_container(
        [
            "bash",
            "-c",
            "mv /etc/clickhouse-server/model/simple_model.bin /etc/clickhouse-server/model/amazon_model.bin",
        ]
    )
    instance.exec_in_container(
        [
            "bash",
            "-c",
            "mv /etc/clickhouse-server/model/simple_model.bin.bak /etc/clickhouse-server/model/simple_model.bin",
        ]
    )


def testSystemModelsAndModelRefresh(ch_cluster):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")

    result = instance.query("system reload models")

    # check model system view
    result = instance.query("select * from system.models")
    expected = ""
    assert result == expected

    # load simple model
    result = instance.query(
        "select catboostEvaluate('/etc/clickhouse-server/model/simple_model.bin', 1.0, 2.0, 3, 4, 5, 6, 7, 8, 9, 10, 11);"
    )
    expected = "-1.930268705869267\n"
    assert result == expected

    # check model system view with one model loaded
    result = instance.query("select * from system.models")
    assert result.count("\n") == 1
    expected = "/etc/clickhouse-server/model/simple_model.bin"
    assert expected in result

    # load amazon model
    result = instance.query(
        "select catboostEvaluate('/etc/clickhouse-server/model/amazon_model.bin', 1, 2, 3, 4, 5, 6, 7, 8, 9);"
    )
    expected = "0.7774665009089274\n"
    assert result == expected

    # check model system view with one model loaded
    result = instance.query("select * from system.models")
    assert result.count("\n") == 2
    expected = "/etc/clickhouse-server/model/simple_model.bin"
    assert expected in result
    expected = "/etc/clickhouse-server/model/amazon_model.bin"
    assert expected in result

    # unload simple model
    result = instance.query(
        "system reload model '/etc/clickhouse-server/model/simple_model.bin'"
    )

    # check model system view, it should not display the removed model
    result = instance.query("select * from system.models")
    assert result.count("\n") == 1
    expected = "/etc/clickhouse-server/model/amazon_model.bin"
    assert expected in result
