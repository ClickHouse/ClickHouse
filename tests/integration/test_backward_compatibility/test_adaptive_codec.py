import pytest

from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node_old = cluster.add_instance(
    "node1",
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
    with_installed_binary=True,
    with_zookeeper=True,
)
node_new = cluster.add_instance("node2", with_zookeeper=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def fill(table, adaptive, x_expr):
    node_old.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node_new.query(f"DROP TABLE IF EXISTS {table} SYNC")

    # The old replica fetches the new replica's merged part instead of merging locally, so it reads exactly the bytes the new server wrote.
    node_old.query(f"""
        CREATE TABLE {table} (id UInt64, x UInt64)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/0/{table}', 'old')
        ORDER BY id SETTINGS always_fetch_merged_part = 1
        """)
    # serialization_info_version=basic: otherwise the new server writes serialization.json in a format
    # the old version cannot parse. Generic old-version write compat, unrelated to adaptive codecs.
    node_new.query(f"""
        CREATE TABLE {table} (id UInt64, x UInt64)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/0/{table}', 'new')
        ORDER BY id
        SETTINGS min_bytes_for_wide_part = 0, max_compress_block_size = 65536,
            serialization_info_version = 'basic',
            allow_experimental_adaptive_codec_selection = {adaptive}
        """)
    node_new.query(f"INSERT INTO {table} SELECT number, {x_expr} FROM numbers(1000000)")
    node_new.query(
        f"INSERT INTO {table} SELECT number, number FROM numbers(1000000, 1000000)"
    )
    # Adaptive selection runs only on merge, so force one.
    node_new.query(f"OPTIMIZE TABLE {table} FINAL")
    node_old.query(f"SYSTEM SYNC REPLICA {table}", timeout=60)


def codecs(table, column):
    # On the new node: whether the column has any T64 block, and how many distinct codecs it uses.
    return node_new.query(f"""
        SELECT mapContains(codec_block_counts, 'T64'), length(codec_block_counts)
        FROM system.parts_columns
        WHERE database = currentDatabase() AND table = '{table}' AND active AND column = '{column}'
        """)


def test_setting_off_no_change(start_cluster):
    # Setting off: x keeps the default codec (one codec, not T64) and the old version reads it.
    fill("t_adaptive_compat_off", adaptive=0, x_expr="number")
    assert codecs("t_adaptive_compat_off", "x") == "0\t1\n"
    assert (
        node_old.query("SELECT count(), sum(x) FROM t_adaptive_compat_off")
        == "2000000\t1999999000000\n"
    )


def test_old_version_reads_uniform_adaptive_part(start_cluster):
    # Monotonic data: T64 wins in every block, so x is a uniform single-codec stream. The old strict
    # reader only rejects a codec change inside one stream, so it reads a uniform adaptive part fine.
    fill("t_adaptive_compat_uniform", adaptive=1, x_expr="number")
    assert codecs("t_adaptive_compat_uniform", "x") == "1\t1\n"
    assert (
        node_old.query("SELECT count(), sum(x) FROM t_adaptive_compat_uniform")
        == "2000000\t1999999000000\n"
    )


def test_old_version_cannot_read_mixed_codec_part(start_cluster):
    # Low ids random (stored raw), high ids monotonic (T64), so x mixes codecs after the merge.
    # id stays monotonic and uniform.
    fill(
        "t_adaptive_compat_mixed",
        adaptive=1,
        x_expr="if(number < 500000, cityHash64(number), number)",
    )
    assert codecs("t_adaptive_compat_mixed", "id") == "1\t1\n"
    assert node_new.query("""
        SELECT length(codec_block_counts) > 1 FROM system.parts_columns
        WHERE database = currentDatabase() AND table = 't_adaptive_compat_mixed'
            AND active AND column = 'x'
        """) == "1\n"

    # The fetch is file-level, so the old replica gets the part and reads its uniform column.
    assert (
        node_old.query("SELECT count(), sum(id) FROM t_adaptive_compat_mixed")
        == "2000000\t1999999000000\n"
    )
    # The mixed column hits a codec change mid-stream; the old version rejects it. This is the
    # boundary the setting docstring warns about for rolling upgrades.
    error = node_old.query_and_get_error("SELECT sum(x) FROM t_adaptive_compat_mixed")
    assert "CANNOT_DECOMPRESS" in error
