import json

import pytest

from helpers.cluster import ClickHouseCluster

# Reading a Log or TinyLog table whose array streams disagree on disk must raise an error naming
# the column, not hand a column whose offsets claim more elements than it holds to the query
# pipeline, where a sort indexes the elements out of bounds.
#
# The damage is produced by editing the files directly, so this cannot be a functional test:
# only the integration harness owns the server and its disk.


@pytest.fixture(scope="module")
def started_cluster():
    cluster = ClickHouseCluster(__file__)
    cluster.add_instance("node", stay_alive=True)
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize("engine", ["Log", "TinyLog"])
def test_read_array_with_missing_elements(started_cluster, engine):
    node = started_cluster.instances["node"]
    table = f"damaged_{engine.lower()}"

    node.query(f"DROP TABLE IF EXISTS {table}")
    node.query(
        f"CREATE TABLE {table} (a UInt64, arr Array(Array(Int64))) ENGINE = {engine}"
    )
    node.query(
        f"INSERT INTO {table} SELECT number, [[number, number + 1], [number + 2]] FROM numbers(500)"
    )
    assert node.query(f"SELECT count() FROM {table}").strip() == "500"

    table_path = node.query(
        f"SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND name = '{table}'"
    ).strip()

    node.query(f"DETACH TABLE {table}")

    # Empty the stream holding the sizes of the inner arrays while leaving the outer sizes and the
    # elements in place, and record the new size so that the size check accepts the table again.
    # This is the shape a Log table is left in when a write commit is interrupted between streams.
    sizes_path = table_path + "sizes.json"
    node.exec_in_container(
        ["bash", "-c", f"truncate -s 0 {table_path}arr.size1.bin"], user="root"
    )
    sizes = json.loads(node.exec_in_container(["cat", sizes_path], user="root"))
    root = sizes.get("clickhouse", sizes.get("yandex"))
    root["arr%2Esize1%2Ebin"]["size"] = "0"
    node.exec_in_container(
        ["bash", "-c", f"cat > {sizes_path} << 'EOF'\n{json.dumps(sizes)}\nEOF"],
        user="root",
    )

    node.query(f"ATTACH TABLE {table}")

    for query in (
        f"SELECT a, arr FROM {table} ORDER BY a DESC LIMIT 10 FORMAT Null",
        f"SELECT arr FROM {table} LIMIT 10 FORMAT Null",
    ):
        error = node.query_and_get_error(query)
        assert "INCORRECT_DATA" in error, error
        assert "arr" in error, error

    # The server must still be running: the damaged column was rejected, not consumed.
    assert node.query("SELECT 1").strip() == "1"

    node.query(f"DROP TABLE {table}")
