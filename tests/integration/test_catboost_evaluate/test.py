import os
import sys

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

# The catboost library (.so) is configured server-side (catboost_lib_path) and is not
# user-controlled, so it stays in /etc/clickhouse-server/model.
LIB_DIR = "/etc/clickhouse-server/model"
# The model path is the user-controlled first argument of catboostEvaluate() and is
# restricted to user_files (like file() and dictionaries), so models live there.
USER_FILES = "/var/lib/clickhouse/user_files"
SIMPLE_MODEL = f"{USER_FILES}/simple_model.bin"
AMAZON_MODEL = f"{USER_FILES}/amazon_model.bin"


@pytest.fixture(scope="module")
def ch_cluster():
    try:
        cluster.start()

        instance.exec_in_container(["mkdir", f"{LIB_DIR}/"])

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
                    dist=f"{LIB_DIR}/{dest_name}",
                )
            )

        # Place the model files inside user_files: catboostEvaluate() restricts
        # the model path to user_files, so the evaluated models must live there.
        for model_name in ("simple_model.bin", "amazon_model.bin"):
            os.system(
                "docker cp {local} {cont_id}:{dist}".format(
                    local=os.path.join(SCRIPT_DIR, f"model/{model_name}"),
                    cont_id=instance.docker_id,
                    dist=f"{USER_FILES}/{model_name}",
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
        f"select catboostEvaluate('{SIMPLE_MODEL}', 1.0, 2.0, 3, 4, 5, 6, 7, 8, 9, 10, 11);"
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
        f"select catboostEvaluate('{SIMPLE_MODEL}', F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11) from T;"
    )
    expected = "-1.930268705869267\n"
    assert result == expected

    instance.query("DROP TABLE IF EXISTS T;")


def testModelPathIsNotAConstString(ch_cluster):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")

    instance.query("system reload models")

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

    instance.query("system reload models")

    err = instance.query_and_get_error(f"select catboostEvaluate('{SIMPLE_MODEL}');")
    assert "Function catboostEvaluate expects at least 2 arguments" in err

    err = instance.query_and_get_error(
        f"select catboostEvaluate('{SIMPLE_MODEL}', 1, 2);"
    )
    assert (
        "Number of columns is different with number of features: columns size 2 float features size 2 + cat features size 9"
        in err
    )


def testFloatFeatureMustBeNumeric(ch_cluster):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")

    instance.query("system reload models")

    err = instance.query_and_get_error(
        f"select catboostEvaluate('{SIMPLE_MODEL}', 1.0, 'a', 3, 4, 5, 6, 7, 8, 9, 10, 11);"
    )
    assert "Column 1 should be numeric to make float feature" in err


def testCategoricalFeatureMustBeNumericOrString(ch_cluster):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")

    instance.query("system reload models")

    err = instance.query_and_get_error(
        f"select catboostEvaluate('{SIMPLE_MODEL}', 1.0, 2.0, 3, 4, 5, 6, 7, tuple(8), 9, 10, 11);"
    )
    assert "Column 7 should be numeric or string" in err


def testOnLowCardinalityFeatures(ch_cluster):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")

    result = instance.query("system reload models")

    # same but on domain-compressed data
    result = instance.query(
        f"select catboostEvaluate('{SIMPLE_MODEL}', toLowCardinality(1.0), toLowCardinality(2.0), toLowCardinality(3), toLowCardinality(4), toLowCardinality(5), toLowCardinality(6), toLowCardinality(7), toLowCardinality(8), toLowCardinality(9), toLowCardinality(10), toLowCardinality(11));"
    )
    expected = "-1.930268705869267\n"
    assert result == expected


def testOnNullableFeatures(ch_cluster):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")

    result = instance.query("system reload models")

    result = instance.query(
        f"select catboostEvaluate('{SIMPLE_MODEL}', toNullable(1.0), toNullable(2.0), toNullable(3), toNullable(4), toNullable(5), toNullable(6), toNullable(7), toNullable(8), toNullable(9), toNullable(10), toNullable(11));"
    )
    expected = "-1.930268705869267\n"
    assert result == expected

    # Actual NULLs are disallowed
    err = instance.query_and_get_error(
        f"select catboostEvaluate('{SIMPLE_MODEL}', toNullable(NULL), toNullable(NULL), toNullable(NULL), toNullable(NULL), toNullable(NULL), toNullable(NULL), toNullable(NULL), toNullable(NULL), toNullable(NULL), toNullable(NULL), toNullable(NULL));"
    )
    assert "Column 0 should be numeric to make float feature" in err


def testInvalidLibraryPath(ch_cluster):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")

    instance.query("system reload models")

    # temporarily move library elsewhere
    instance.exec_in_container(
        [
            "bash",
            "-c",
            f"mv {LIB_DIR}/libcatboostmodel.so {LIB_DIR}/nonexistant.so",
        ]
    )

    err = instance.query_and_get_error(
        f"select catboostEvaluate('{SIMPLE_MODEL}', 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);"
    )
    assert (
        f"Can't load library {LIB_DIR}/libcatboostmodel.so: file doesn't exist" in err
    )

    # restore
    instance.exec_in_container(
        [
            "bash",
            "-c",
            f"mv {LIB_DIR}/nonexistant.so {LIB_DIR}/libcatboostmodel.so",
        ]
    )


def testInvalidModelPath(ch_cluster):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")

    instance.query("system reload models")

    # A path inside user_files that does not exist still reports FILE_DOESNT_EXIST
    # (probing existence inside one's own sandbox is not an information leak).
    err = instance.query_and_get_error(
        f"select catboostEvaluate('{USER_FILES}/model_non_existant.bin', 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);"
    )
    assert (
        f"Can't load model {USER_FILES}/model_non_existant.bin: file doesn't exist"
        in err
    )


def testModelPathOutsideUserFilesIsDenied(ch_cluster):
    # Security: the model path is restricted to user_files. Any path outside it must be
    # rejected with the same PATH_ACCESS_DENIED error regardless of whether the target
    # file exists, is permission-denied, or does not exist - otherwise catboostEvaluate
    # would be a filesystem existence oracle and an arbitrary file read trigger.
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")

    instance.query("system reload models")

    # An existing file outside user_files, a permission-denied file, a missing
    # file, a path-traversal attempt, a relative path and an empty path must all
    # be indistinguishable.
    paths = [
        "/etc/passwd",  # exists, readable
        "/root/.ssh/id_rsa",  # permission denied / does not exist
        "/nonexistent_file_xyz",  # does not exist
        f"{USER_FILES}/../../../etc/passwd",  # path traversal out of the sandbox
        "model_non_existant.bin",  # relative path (resolved outside user_files)
        "",  # empty path
    ]

    errors = []
    for path in paths:
        err = instance.query_and_get_error(
            f"select catboostEvaluate('{path}', 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);"
        )
        assert "must be inside the user_files directory" in err, (path, err)
        # No FILE_DOESNT_EXIST leak: the error must not reveal whether the file exists.
        assert "file doesn't exist" not in err, (path, err)
        # Keep only the stable part of the message (the probed path only appears in the
        # echoed query after "In scope") so the responses can be compared for
        # indistinguishability.
        errors.append(err.split("In scope")[0])

    # Every probed path produces the same response - no oracle.
    assert len(set(errors)) == 1, errors


def testRecoveryAfterCrash(ch_cluster):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")

    result = instance.query("system reload models")

    result = instance.query(
        f"select catboostEvaluate('{SIMPLE_MODEL}', 1.0, 2.0, 3, 4, 5, 6, 7, 8, 9, 10, 11);"
    )
    expected = "-1.930268705869267\n"
    assert result == expected

    instance.exec_in_container(
        ["bash", "-c", "kill -9 `pidof clickhouse-library-bridge`"], user="root"
    )

    result = instance.query(
        f"select catboostEvaluate('{SIMPLE_MODEL}', 1.0, 2.0, 3, 4, 5, 6, 7, 8, 9, 10, 11);"
    )
    assert result == expected


# ---------------------------------------------------------------------------
# amazon_model.bin has 0 float features and 9 categorical features


def testAmazonModelSingleRow(ch_cluster):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")

    result = instance.query("system reload models")

    result = instance.query(
        f"select catboostEvaluate('{AMAZON_MODEL}', 1, 2, 3, 4, 5, 6, 7, 8, 9);"
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
        f"SELECT floor(sum(catboostEvaluate('{AMAZON_MODEL}', RESOURCE, MGR_ID, ROLE_ROLLUP_1, ROLE_ROLLUP_2, ROLE_DEPTNAME, ROLE_TITLE, ROLE_FAMILY_DESC, ROLE_FAMILY, ROLE_CODE))) FROM amazon"
    )

    expected = "583092\n"
    assert result == expected

    result = instance.query("drop table if exists amazon")


def testModelUpdate(ch_cluster):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")

    result = instance.query("system reload models")

    query = f"select catboostEvaluate('{SIMPLE_MODEL}', 1.0, 2.0, 3, 4, 5, 6, 7, 8, 9, 10, 11);"

    result = instance.query(query)
    expected = "-1.930268705869267\n"
    assert result == expected

    # simulate an update of the model: temporarily move the amazon model in place of the simple model
    instance.exec_in_container(
        [
            "bash",
            "-c",
            f"mv {SIMPLE_MODEL} {SIMPLE_MODEL}.bak",
        ]
    )
    instance.exec_in_container(
        [
            "bash",
            "-c",
            f"mv {AMAZON_MODEL} {SIMPLE_MODEL}",
        ]
    )

    # unload simple model
    result = instance.query(f"system reload model '{SIMPLE_MODEL}'")

    # load the simple-model-camouflaged amazon model
    result = instance.query(
        f"select catboostEvaluate('{SIMPLE_MODEL}', 1, 2, 3, 4, 5, 6, 7, 8, 9);"
    )
    expected = "0.7774665009089274\n"
    assert result == expected

    # restore
    instance.exec_in_container(
        [
            "bash",
            "-c",
            f"mv {SIMPLE_MODEL} {AMAZON_MODEL}",
        ]
    )
    instance.exec_in_container(
        [
            "bash",
            "-c",
            f"mv {SIMPLE_MODEL}.bak {SIMPLE_MODEL}",
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
        f"select catboostEvaluate('{SIMPLE_MODEL}', 1.0, 2.0, 3, 4, 5, 6, 7, 8, 9, 10, 11);"
    )
    expected = "-1.930268705869267\n"
    assert result == expected

    # check model system view with one model loaded
    result = instance.query("select * from system.models")
    assert result.count("\n") == 1
    expected = SIMPLE_MODEL
    assert expected in result

    # load amazon model
    result = instance.query(
        f"select catboostEvaluate('{AMAZON_MODEL}', 1, 2, 3, 4, 5, 6, 7, 8, 9);"
    )
    expected = "0.7774665009089274\n"
    assert result == expected

    # check model system view with one model loaded
    result = instance.query("select * from system.models")
    assert result.count("\n") == 2
    expected = SIMPLE_MODEL
    assert expected in result
    expected = AMAZON_MODEL
    assert expected in result

    # unload simple model
    result = instance.query(f"system reload model '{SIMPLE_MODEL}'")

    # check model system view, it should not display the removed model
    result = instance.query("select * from system.models")
    assert result.count("\n") == 1
    expected = AMAZON_MODEL
    assert expected in result
