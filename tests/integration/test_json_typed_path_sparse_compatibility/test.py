import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key


cluster = ClickHouseCluster(__file__)
latest = cluster.add_instance("latest", with_minio=True)
old = cluster.add_instance(
    "old",
    image="clickhouse/clickhouse-server",
    tag="26.6",
    with_installed_binary=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        latest_version = latest.query("SELECT version()").strip()
        old_version = old.query("SELECT version()").strip()
        assert old_version.startswith("26.6.")
        assert latest_version != old_version
        yield cluster
    finally:
        cluster.shutdown()


def create_table(node, table, extra_settings=""):
    node.query(
        f"""
        CREATE TABLE {table}
        (
            id UInt64,
            j JSON(x Nullable(String), y String, max_dynamic_paths = 1)
        )
        ENGINE = MergeTree
        ORDER BY id
        SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5
        {extra_settings}
        """
    )


def insert_data(node, table):
    node.query(
        f"""
        INSERT INTO {table}
        SELECT
            number,
            CAST(
                if(number = 0,
                    '{{"x":"rare","y":"dense","dynamic":1,"shared":"value"}}',
                    '{{"x":null,"y":"dense","dynamic":1,"shared":"value"}}'),
                'JSON(x Nullable(String), y String, max_dynamic_paths = 1)')
        FROM numbers(1000)
        """
    )


def check_data(node, source):
    return node.query(
        f"SELECT count(), countIf(j.x = 'rare'), countIf(j.y = 'dense') FROM {source}"
    ).strip()


def check_recursive_data(node, source, path):
    return node.query(
        f"SELECT count(), countIf({path} = 'rare') FROM {source}"
    ).strip()


def test_native_protocol_downgrade(started_cluster):
    create_table(
        latest,
        "sparse_new",
        ", serialization_info_version = 'with_subcolumns', nullable_serialization_version = 'allow_sparse'",
    )
    create_table(old, "dense_old")
    insert_data(latest, "sparse_new")
    insert_data(old, "dense_old")

    assert check_data(latest, "remote('old', default, dense_old)") == "1000\t1\t1000"
    assert check_data(old, "remote('latest', default, sparse_new)") == "1000\t1\t1000"

    create_table(latest, "from_old")
    latest.query("INSERT INTO from_old SELECT * FROM remote('old', default, dense_old)")
    assert check_data(latest, "from_old") == "1000\t1\t1000"

    create_table(old, "from_new")
    old.query("INSERT INTO from_new SELECT * FROM remote('latest', default, sparse_new)")
    assert check_data(old, "from_new") == "1000\t1\t1000"


@pytest.mark.parametrize(
    ("suffix", "column_type", "value", "path"),
    [
        (
            "typed_tuple",
            "JSON(t Tuple(a Nullable(String), b UInt64), max_dynamic_paths = 0)",
            "CAST(if(number = 0, '{\\\"t\\\":{\\\"a\\\":\\\"rare\\\",\\\"b\\\":1}}', "
            "'{\\\"t\\\":{\\\"a\\\":null,\\\"b\\\":1}}'), "
            "'JSON(t Tuple(a Nullable(String), b UInt64), max_dynamic_paths = 0)')",
            "j.t.a",
        ),
        (
            "tuple_json",
            "Tuple(o JSON(x Nullable(String), max_dynamic_paths = 0))",
            "tuple(CAST(if(number = 0, '{\\\"x\\\":\\\"rare\\\"}', '{\\\"x\\\":null}'), "
            "'JSON(x Nullable(String), max_dynamic_paths = 0)'))",
            "j.o.x",
        ),
        (
            "nullable_json",
            "Nullable(JSON(x Nullable(String), max_dynamic_paths = 0))",
            "CAST(multiIf(number = 0, '{\\\"x\\\":\\\"rare\\\"}', number = 1, NULL, '{}'), "
            "'Nullable(JSON(x Nullable(String), max_dynamic_paths = 0))')",
            "j.x",
        ),
    ],
)
def test_recursive_native_protocol_downgrade(
    started_cluster, suffix, column_type, value, path
):
    source = f"recursive_{suffix}_new"
    destination = f"recursive_{suffix}_old"
    latest.query(
        f"""
        CREATE TABLE {source} (id UInt64, j {column_type})
        ENGINE = MergeTree
        ORDER BY id
        SETTINGS
            ratio_of_defaults_for_sparse_serialization = 0.5,
            serialization_info_version = 'with_subcolumns',
            nullable_serialization_version = 'allow_sparse'
        """
    )
    old.query(
        f"""
        CREATE TABLE {destination} (id UInt64, j {column_type})
        ENGINE = MergeTree
        ORDER BY id
        """
    )
    latest.query(
        f"INSERT INTO {source} SELECT number, {value} FROM numbers(1000)"
    )

    remote_source = f"remote('latest', default, {source})"
    assert check_recursive_data(old, remote_source, path) == "1000\t1"
    old.query(f"INSERT INTO {destination} SELECT * FROM {remote_source}")
    assert check_recursive_data(old, destination, path) == "1000\t1"
    assert check_recursive_data(
        latest, f"remote('old', default, {destination})", path
    ) == "1000\t1"


def test_object_storage_merge_and_mutation(started_cluster):
    disk = (
        "disk(type = s3, endpoint = 'http://minio1:9001/root/data/json_sparse/', "
        f"access_key_id = 'minio', secret_access_key = '{minio_secret_key}')"
    )
    create_table(
        latest,
        "sparse_s3",
        f", serialization_info_version = 'with_subcolumns', nullable_serialization_version = 'allow_sparse', disk = {disk}",
    )
    insert_data(latest, "sparse_s3")
    latest.query("INSERT INTO sparse_s3 SELECT id + 1000, j FROM sparse_s3")
    latest.query(
        "ALTER TABLE sparse_s3 MODIFY COLUMN j JSON(x Nullable(String), z Nullable(String), max_dynamic_paths = 1)",
        settings={"mutations_sync": 2},
    )
    latest.query("OPTIMIZE TABLE sparse_s3 FINAL")

    assert latest.query(
        "SELECT count(), countIf(j.x = 'rare'), countIf(j.z IS NULL), countIf(j.y = 'dense') FROM sparse_s3"
    ).strip() == "2000\t2\t2000\t2000"
    assert latest.query(
        """
        SELECT groupArray((path, serialization))
        FROM
        (
            SELECT
                tupleElement(item, 1) AS path,
                tupleElement(item, 2) AS serialization
            FROM
            (
                SELECT arrayJoin(arrayZip(subcolumns.names, subcolumns.serializations)) AS item
                FROM system.parts_columns
                WHERE database = currentDatabase() AND table = 'sparse_s3' AND column = 'j' AND active
            )
            WHERE path IN ('x', 'z')
            ORDER BY path
        )
        """
    ).strip() == "[('x','Sparse'),('z','Sparse')]"
