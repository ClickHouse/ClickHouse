import pytest

from helpers.cluster import ClickHouseCluster

from .yt_helpers import YT_DEFAULT_TOKEN, YT_HOST, YT_PORT, YT_URI, YTsaurusCLI

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/display_secrets.xml"],
    user_configs=["configs/allow_experimental_ytsaurus.xml"],
    with_ytsaurus=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_yt_simple_table_engine(started_cluster):
    yt = YTsaurusCLI(started_cluster, instance, YT_HOST, YT_PORT)
    yt.create_table("//tmp/table", '{"a":"10","b":"20"}\n{"a":"20","b":"40"}')

    instance.query(
        f"CREATE TABLE yt_test(a Int32, b Int32) ENGINE=YTsaurus('{YT_URI}', '//tmp/table', '{YT_DEFAULT_TOKEN}')"
    )

    assert instance.query("SELECT * FROM yt_test") == "10\t20\n20\t40\n"
    assert instance.query("SELECT a,b FROM yt_test") == "10\t20\n20\t40\n"
    assert instance.query("SELECT a FROM yt_test") == "10\n20\n"

    assert instance.query("SELECT * FROM yt_test WHERE a > 15") == "20\t40\n"

    instance.query("DROP TABLE yt_test SYNC")

    yt.remove_table("//tmp/table")


def test_yt_simple_table_function(started_cluster):
    yt = YTsaurusCLI(started_cluster, instance, YT_HOST, YT_PORT)
    yt.create_table("//tmp/table", '{"a":"10","b":"20"}\n{"a":"20","b":"40"}')

    assert (
        instance.query(
            f"SELECT * FROM ytsaurus('{YT_URI}','//tmp/table', '{YT_DEFAULT_TOKEN}','a Int32, b Int32')"
        )
        == "10\t20\n20\t40\n"
    )
    assert (
        instance.query(
            f"SELECT a,b FROM ytsaurus('{YT_URI}','//tmp/table', '{YT_DEFAULT_TOKEN}', 'a Int32, b Int32')"
        )
        == "10\t20\n20\t40\n"
    )
    assert (
        instance.query(
            f"SELECT a FROM ytsaurus('{YT_URI}','//tmp/table', '{YT_DEFAULT_TOKEN}','a Int32, b Int32')"
        )
        == "10\n20\n"
    )
    assert (
        instance.query(
            f"SELECT * FROM ytsaurus('{YT_URI}','//tmp/table', '{YT_DEFAULT_TOKEN}','a Int32, b Int32') WHERE a > 15"
        )
        == "20\t40\n"
    )
    yt.remove_table("//tmp/table")


@pytest.mark.parametrize(
    "yt_data_type, yt_data, ch_column_type, ch_data_expected",
    [
        pytest.param(
            "string",
            '"test string"',
            "String",
            "test string",
            id="String",
        ),
        pytest.param(
            "int32",
            "-1",
            "Int32",
            "-1",
            id="Int32",
        ),
        pytest.param(
            "uint32",
            "1",
            "UInt32",
            "1",
            id="UInt32",
        ),
        pytest.param(
            "double",
            "0.1",
            "Float64",
            "0.1",
            id="Float64",
        ),
        pytest.param(
            "boolean",
            "true",
            "Bool",
            "true",
            id="Bool",
        ),
        pytest.param(
            "any",
            "[1, 2]",
            "Array(Int32)",
            "[1,2]",
            id="Array_simple",
        ),
        pytest.param(
            "any",
            "[[1,1],[1,1]]",
            "Array(Array(Int32))",
            "[[1,1],[1,1]]",
            id="Array_complex",
        ),
        pytest.param(
            "any",
            '{"a":"hello", "38 parrots":[38]}',
            "String",
            '{"a":"hello","38 parrots":[38]}',
            id="Dict",
        ),
    ],
)
def test_ytsaurus_types(
    started_cluster, yt_data_type, yt_data, ch_column_type, ch_data_expected
):
    yt = YTsaurusCLI(started_cluster, instance, "ytsaurus_backend1", 80)
    table_path = "//tmp/table"
    column_name = "a"
    yt_data_json = f'{{"{column_name}":{yt_data}}}\n'

    yt.create_table(table_path, yt_data_json, schema={column_name: yt_data_type})

    instance.query(
        f"CREATE TABLE yt_test(a {ch_column_type}) ENGINE=YTsaurus('{YT_URI}', '{table_path}', '{YT_DEFAULT_TOKEN}')"
    )
    assert instance.query("SELECT a FROM yt_test") == f"{ch_data_expected}\n"
    instance.query("DROP TABLE yt_test")
    yt.remove_table(table_path)


def test_ytsaurus_multiple_tables(started_cluster):
    table_path = "//tmp/table"
    yt = YTsaurusCLI(started_cluster, instance, YT_HOST, YT_PORT)
    yt.create_table(table_path, '{"a":"10","b":"20"}\n{"a":"20","b":"40"}')

    instance.query("CREATE DATABASE db")
    instance.query(
        f"CREATE TABLE db.good(a Int32, b Int32) ENGINE=YTsaurus('{YT_URI}', '//tmp/table', '{YT_DEFAULT_TOKEN}')"
    )
    instance.query(
        f"CREATE TABLE db.bad(a Int32, b Int32) ENGINE=YTsaurus('{YT_URI}', '//tmp/table', 'IncorrectToken')"
    )

    instance.query("SELECT * FROM db.good")
    instance.query_and_get_error("SELECT * FROM db.bad")

    instance.query(
        f"CREATE TABLE db.good2(a Int32, b Int32) ENGINE=YTsaurus('{YT_URI}', '//tmp/table', '{YT_DEFAULT_TOKEN}')"
    )
    instance.query("Select * from db.good2")

    instance.query(
        f"CREATE TABLE db.bad2(a Int32, b Int32) ENGINE=YTsaurus('{YT_URI}', '//tmp/table', 'IncorrectToken')"
    )
    instance.query_and_get_error("select * from db.bad2")
    instance.query("select * from db.good2")
    instance.query("select * from db.good")
    instance.query_and_get_error("select * from db.bad")

    instance.query("DROP DATABASE db")
    yt.remove_table(table_path)


def test_ytsaurus_dynamic_table(started_cluster):
    table_path = "//tmp/dynamic_table"
    yt = YTsaurusCLI(started_cluster, instance, YT_HOST, YT_PORT)
    yt.create_table(
        table_path,
        '{"a":10,"b":"20"}{"a":20,"b":"40"}',
        dynamic=True,
        schema={"a": "int32", "b": "string"},
        retry_count=5,
    )

    instance.query(
        f"CREATE TABLE yt_test(a Int32, b Int32) ENGINE=YTsaurus('{YT_URI}', '//tmp/dynamic_table', '{YT_DEFAULT_TOKEN}')"
    )
    assert instance.query("SELECT * FROM yt_test") == "10\t20\n20\t40\n"
    instance.query("DROP TABLE yt_test SYNC")
    yt.remove_table("//tmp/dynamic_table")


def test_hiding_credentials(started_cluster):
    table_name = "yt_hide_cred"
    instance.query(
        f"CREATE TABLE {table_name}(a Int32, b Int32) ENGINE=YTsaurus('{YT_URI}', '//tmp/{table_name}', '{YT_DEFAULT_TOKEN}')"
    )

    instance.query("SYSTEM FLUSH LOGS")
    message = instance.query(
        f"SELECT message FROM system.text_log WHERE message ILIKE '%CREATE TABLE {table_name}%'"
    )
    assert (
        f"YTsaurus(\\'http://ytsaurus_backend1:80\\', \\'//tmp/{table_name}\\', \\'[HIDDEN]\\'"
        in message
    )

    engine_full_hidden = instance.query(
        f"SELECT engine_full FROM system.tables WHERE name='{table_name}';"
    )
    assert "[HIDDEN]" in engine_full_hidden

    engine_full = instance.query(
        f"SELECT engine_full FROM system.tables WHERE name='{table_name}' SETTINGS format_display_secrets_in_show_and_select=1;"
    )
    assert f"{YT_DEFAULT_TOKEN}" in engine_full

    instance.query(f"DROP TABLE {table_name};")


def test_yt_simple_table_engine(started_cluster):
    yt = YTsaurusCLI(started_cluster, instance, YT_HOST, YT_PORT)
    yt.create_table("//tmp/table", '{"a":"10","b":"20"}\n{"a":"20","b":"40"}')

    instance.query(
        f"CREATE TABLE yt_test(a Int32, b Int32) ENGINE=YTsaurus('http://incorrect_enpoint|{YT_URI}', '//tmp/table', '{YT_DEFAULT_TOKEN}') SETTINGS http_max_tries = 10, http_retry_max_backoff_ms=2000"
    )

    assert (
        instance.query(
            "SELECT * FROM yt_test SETTINGS http_max_tries = 10, http_retry_max_backoff_ms=2000"
        )
        == "10\t20\n20\t40\n"
    )
    assert (
        instance.query(
            "SELECT a,b FROM yt_test SETTINGS http_max_tries = 10, http_retry_max_backoff_ms=2000"
        )
        == "10\t20\n20\t40\n"
    )
    assert (
        instance.query(
            "SELECT a FROM yt_test SETTINGS http_max_tries = 10, http_retry_max_backoff_ms=2000"
        )
        == "10\n20\n"
    )

    assert (
        instance.query(
            "SELECT * FROM yt_test WHERE a > 15 SETTINGS http_max_tries = 10, http_retry_max_backoff_ms=2000"
        )
        == "20\t40\n"
    )

    instance.query("DROP TABLE yt_test SYNC")

    yt.remove_table("//tmp/table")
