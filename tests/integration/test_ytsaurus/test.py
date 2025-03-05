import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
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


class YTsaurusCLI:
    def __init__(self, instance, proxy, port):
        self.instance = instance
        self.proxy = proxy
        self.port = port

    def create_table(self, table_path, data, schema=None):

        schema_arribute = ""
        if schema:
            schema_arribute = (
                '--attributes "{schema= ['
                + ";".join(
                    f"{{name = {name}; type = {type}}}" for name, type in schema.items()
                )
                + ']}"'
            )

        print(schema_arribute)

        cluster.exec_in_container(
            cluster.get_container_id(self.proxy),
            [
                "bash",
                "-c",
                "yt --proxy {}:{} create {} table {}".format(
                    self.proxy, self.port, schema_arribute, table_path
                ),
            ],
        )

        cluster.exec_in_container(
            cluster.get_container_id(self.proxy),
            [
                "bash",
                "-c",
                "echo '{}' | yt --proxy {}:{} write-table {} --format json".format(
                    data, self.proxy, self.port, table_path
                ),
            ],
        )

    def remove_table(self, table_path):
        cluster.exec_in_container(
            cluster.get_container_id(self.proxy),
            [
                "bash",
                "-c",
                "yt --proxy {}:{} remove {}".format(self.proxy, self.port, table_path),
            ],
        )


YT_HOST = "ytsaurus_backend1"
YT_PORT = 80
YT_URI = f"http://{YT_HOST}:{YT_PORT}"
YT_DEFAULT_TOKEN = "password"


def test_yt_simple_table_engine(started_cluster):
    yt = YTsaurusCLI(instance, YT_HOST, YT_PORT)
    yt.create_table("//tmp/table", '{"a":"10","b":"20"}\n{"a":"20","b":"40"}')

    instance.query(
        f"CREATE TABLE yt_test(a Int32, b Int32) ENGINE=Ytsaurus('{YT_URI}', '//tmp/table', '{YT_DEFAULT_TOKEN}')"
    )

    assert instance.query("SELECT * FROM yt_test") == "10\t20\n20\t40\n"
    assert instance.query("SELECT a,b FROM yt_test") == "10\t20\n20\t40\n"
    assert instance.query("SELECT a FROM yt_test") == "10\n20\n"

    assert instance.query("SELECT * FROM yt_test WHERE a > 15") == "20\t40\n"

    instance.query("DROP TABLE yt_test SYNC")

    yt.remove_table("//tmp/table")


def test_yt_simple_table_function(started_cluster):
    yt = YTsaurusCLI(instance, YT_HOST, YT_PORT)
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
    yt = YTsaurusCLI(instance, "ytsaurus_backend1", 80)
    table_path = "//tmp/table"
    column_name = "a"
    yt_data_json = f'{{"{column_name}":{yt_data}}}\n'

    yt.create_table(table_path, yt_data_json, schema={column_name: yt_data_type})

    instance.query(
        f"CREATE TABLE yt_test(a {ch_column_type}) ENGINE=Ytsaurus('{YT_URI}', '{table_path}', '{YT_DEFAULT_TOKEN}')"
    )
    assert instance.query("SELECT a FROM yt_test") == f"{ch_data_expected}\n"
    instance.query("DROP TABLE yt_test")
    yt.remove_table(table_path)


def test_ytsaurus_multiple_tables(started_cluster):
    table_path = "//tmp/table"
    yt = YTsaurusCLI(instance, YT_HOST, YT_PORT)
    yt.create_table(table_path, '{"a":"10","b":"20"}\n{"a":"20","b":"40"}')

    instance.query("CREATE DATABASE db")
    instance.query(
        f"CREATE TABLE db.good(a Int32, b Int32) ENGINE=Ytsaurus('{YT_URI}', '//tmp/table', '{YT_DEFAULT_TOKEN}')"
    )
    instance.query(
        f"CREATE TABLE db.bad(a Int32, b Int32) ENGINE=Ytsaurus('{YT_URI}', '//tmp/table', 'IncorrectToken')"
    )

    instance.query("SELECT * FROM db.good")
    instance.query_and_get_error("SELECT * FROM db.bad")

    instance.query(
        f"CREATE TABLE db.good2(a Int32, b Int32) ENGINE=Ytsaurus('{YT_URI}', '//tmp/table', '{YT_DEFAULT_TOKEN}')"
    )
    instance.query("Select * from db.good2")

    instance.query(
        f"CREATE TABLE db.bad2(a Int32, b Int32) ENGINE=Ytsaurus('{YT_URI}', '//tmp/table', 'IncorrectToken')"
    )
    instance.query_and_get_error("select * from db.bad2")
    instance.query("select * from db.good2")
    instance.query("select * from db.good")
    instance.query_and_get_error("select * from db.bad")

    instance.query("DROP DATABASE db")
    yt.remove_table(table_path)
