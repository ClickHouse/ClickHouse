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


def create_table(node, table, engine, inserts=1):
    """Fills a table with 500 rows of Array(Array(Int64)) per insert and detaches it.

    Returns the data path and the size of the elements stream after each insert. One insert makes
    the outer offsets describe 1000 inner arrays and the inner offsets 1500 leaf elements.
    """
    node.query(f"DROP TABLE IF EXISTS {table}")
    node.query(
        f"CREATE TABLE {table} (a UInt64, arr Array(Array(Int64))) ENGINE = {engine}"
    )

    table_path = node.query(
        f"SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND name = '{table}'"
    ).strip()

    sizes = []
    for i in range(inserts):
        node.query(
            f"INSERT INTO {table} SELECT number, [[number, number + 1], [number + 2]] "
            f"FROM numbers({i * 500}, 500)"
        )
        sizes.append(stream_size(node, table_path, "arr.bin"))
    assert node.query(f"SELECT count() FROM {table}").strip() == str(500 * inserts)

    node.query(f"DETACH TABLE {table}")
    return table_path, sizes


def stream_size(node, table_path, stream):
    return int(
        node.exec_in_container(
            ["bash", "-c", f"stat -c%s {table_path}{stream}"], user="root"
        ).strip()
    )


def record_size(node, table_path, stream, size):
    """Records `size` as the size of `stream` so that the size check accepts the table again."""
    sizes_path = table_path + "sizes.json"
    sizes = json.loads(node.exec_in_container(["cat", sizes_path], user="root"))
    root = sizes.get("clickhouse", sizes.get("yandex"))
    root[stream.replace(".", "%2E")]["size"] = str(size)
    node.exec_in_container(
        ["bash", "-c", f"cat > {sizes_path} << 'EOF'\n{json.dumps(sizes)}\nEOF"],
        user="root",
    )


# Emptying one stream and leaving the others in place is the shape a Log table is left in when a
# write commit is interrupted between streams. Which nesting level ends up lying depends on which
# stream is gone: without the inner sizes the outer array holds 0 of the 1000 inner arrays its
# offsets promise, while without the leaf elements the outer level is consistent (1000 == 1000) and
# it is the inner level that claims 1500 elements it does not have.
@pytest.mark.parametrize("damaged_stream", ["arr.size1.bin", "arr.bin"])
@pytest.mark.parametrize("engine", ["Log", "TinyLog"])
def test_read_array_with_missing_elements(started_cluster, engine, damaged_stream):
    node = started_cluster.instances["node"]
    table = f"damaged_{engine.lower()}_{damaged_stream.replace('.', '_')}"
    table_path, _ = create_table(node, table, engine)

    node.exec_in_container(
        ["bash", "-c", f"truncate -s 0 {table_path}{damaged_stream}"], user="root"
    )
    record_size(node, table_path, damaged_stream, 0)

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


# An elements stream that holds some but not all of the elements the offsets promise is rejected
# while it is being read, before the offsets can be compared with it, so it fails with a different
# error. Pinned here so that the contract holds for both shapes: no inconsistent array column reads
# successfully.
#
# The truncation has to land on a compression frame boundary, which is what the second insert is
# for: cutting inside a frame destroys it and the read dies in the decompressor instead, without a
# short elements column ever being built. Each insert is well under max_compress_block_size, so the
# size of the elements stream after the first insert is exactly that boundary.
@pytest.mark.parametrize("engine", ["Log", "TinyLog"])
def test_read_array_with_partial_elements(started_cluster, engine):
    node = started_cluster.instances["node"]
    table = f"partial_{engine.lower()}"
    table_path, sizes = create_table(node, table, engine, inserts=2)

    boundary = sizes[0]
    assert 0 < boundary < sizes[1], sizes
    node.exec_in_container(
        ["bash", "-c", f"truncate -s {boundary} {table_path}arr.bin"], user="root"
    )
    record_size(node, table_path, "arr.bin", boundary)

    node.query(f"ATTACH TABLE {table}")

    # One reading thread keeps the whole prefix in a single deserialization, so the offsets of both
    # inserts are compared against the elements of one.
    error = node.query_and_get_error(
        f"SELECT a, arr FROM {table} ORDER BY a DESC LIMIT 10 SETTINGS max_threads = 1 FORMAT Null"
    )
    assert "CANNOT_READ_ALL_DATA" in error, error
    assert "Cannot read all array values" in error, error

    assert node.query("SELECT 1").strip() == "1"

    node.query(f"DROP TABLE {table}")
