"""End-to-end tests for the Iceberg Table Engine (IcebergS3 / IcebergAzure).

The Iceberg Table Engine provides a read-only integration with existing
Apache Iceberg tables.  Unlike the table functions, the engine requires a
persistent `CREATE TABLE` statement and exposes engine-level SETTINGS for
metadata resolution, schema evolution, and data caching.

NOTE: ClickHouse was not originally designed to support tables with externally
changing schemas, so some features that work with regular tables may not work
with the Iceberg Table Engine, especially with the old analyzer.  We recommend
using the Iceberg Table Function for most use cases.
"""
import datetime
import decimal
import uuid

import pyarrow as pa
import pytest

from helpers.catalog_manager_iceberg_azure import IcebergAzureCatalogManager
from helpers.catalog_manager_iceberg_s3 import IcebergS3CatalogManager
from helpers.cluster import ClickHouseCluster
from pyiceberg.types import LongType

pytestmark = pytest.mark.e2e

only_azure = pytest.mark.only_backend("azure")
only_s3 = pytest.mark.only_backend("s3")

_BACKENDS = {
    "azure": IcebergAzureCatalogManager,
    "s3": IcebergS3CatalogManager,
}

# ---------------------------------------------------------------------------
# Test data
# ---------------------------------------------------------------------------

SALES_DATA = pa.table(
    {
        "id": pa.array(range(1, 21), type=pa.int64()),
        "product": pa.array(
            [
                "Widget A", "Widget B", "Gadget X", "Gadget Y", "Widget A",
                "Widget B", "Gadget X", "Gadget Y", "Widget A", "Widget B",
                "Gadget X", "Gadget Y", "Widget A", "Widget B", "Gadget X",
                "Gadget Y", "Widget A", "Widget B", "Gadget X", "Gadget Y",
            ],
            type=pa.string(),
        ),
        "category": pa.array(
            [
                "Widgets", "Widgets", "Gadgets", "Gadgets", "Widgets",
                "Widgets", "Gadgets", "Gadgets", "Widgets", "Widgets",
                "Gadgets", "Gadgets", "Widgets", "Widgets", "Gadgets",
                "Gadgets", "Widgets", "Widgets", "Gadgets", "Gadgets",
            ],
            type=pa.string(),
        ),
        "amount": pa.array(
            [
                10.50, 20.00, 30.75, 40.00, 15.25,
                25.00, 35.50, 45.00, 12.75, 22.00,
                32.25, 42.00, 18.00, 28.50, 38.75,
                48.00, 11.25, 21.50, 31.00, 41.75,
            ],
            type=pa.float64(),
        ),
        "quantity": pa.array([1, 2, 3, 4, 5] * 4, type=pa.int32()),
        "region": pa.array(
            [
                "East", "East", "East", "East", "East",
                "West", "West", "West", "West", "West",
                "East", "East", "East", "East", "East",
                "West", "West", "West", "West", "West",
            ],
            type=pa.string(),
        ),
    }
)

_TS = datetime.datetime(2025, 6, 15, 10, 30, 0)
_TS_UTC = datetime.datetime(2025, 6, 15, 10, 30, 0, tzinfo=datetime.timezone.utc)

TYPES_DATA = pa.table(
    {
        "id": pa.array([1, 2], type=pa.int64()),
        "col_bool": pa.array([True, False], type=pa.bool_()),
        "col_int32": pa.array([100000, -100000], type=pa.int32()),
        "col_int64": pa.array([4294967296, -4294967296], type=pa.int64()),
        "col_float32": pa.array([1.5, -1.5], type=pa.float32()),
        "col_float64": pa.array(
            [3.141592653589793, -3.141592653589793], type=pa.float64()
        ),
        "col_decimal": pa.array(
            [decimal.Decimal("12345.67"), decimal.Decimal("-12345.67")],
            type=pa.decimal128(10, 2),
        ),
        "col_date": pa.array(
            [datetime.date(2025, 1, 1), datetime.date(2000, 6, 15)],
            type=pa.date32(),
        ),
        "col_timestamp": pa.array([_TS, _TS], type=pa.timestamp("us")),
        "col_timestamptz": pa.array(
            [_TS_UTC, _TS_UTC], type=pa.timestamp("us", tz="UTC")
        ),
        "col_string": pa.array(["hello", "world"], type=pa.string()),
        "col_binary": pa.array(
            [b"\x00\x01\x02", b"\xff\xfe"], type=pa.binary()
        ),
    }
)

PARTITIONED_DATA = pa.table(
    {
        "id": pa.array(range(1, 13), type=pa.int64()),
        "region": pa.array(
            ["US", "US", "US", "EU", "EU", "EU", "US", "US", "US", "EU", "EU", "EU"],
            type=pa.string(),
        ),
        "value": pa.array(
            [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120],
            type=pa.int64(),
        ),
    }
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module", params=sorted(_BACKENDS))
def manager(request):
    mgr = _BACKENDS[request.param].from_env()
    yield mgr
    mgr.cleanup_all()


@pytest.fixture(autouse=True)
def _skip_by_backend(request):
    marker = request.node.get_closest_marker("only_backend")
    if marker is not None:
        required = marker.args[0]
        current = request.node.callspec.params.get("manager")
        if current != required:
            pytest.skip(f"requires {required} backend")


_COMMON_MAIN_CONFIGS = ["configs/merge_tree.xml"]
_COMMON_USER_CONFIGS = ["configs/allow_experimental.xml"]


@pytest.fixture(scope="module")
def started_cluster(manager):
    cluster = ClickHouseCluster(__file__, name=f"test_e2e_iceberg_engine_{manager}")
    cluster.add_instance(
        "node",
        main_configs=_COMMON_MAIN_CONFIGS,
        user_configs=_COMMON_USER_CONFIGS,
        stay_alive=True,
    )
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(scope="module")
def node(started_cluster):
    return started_cluster.instances["node"]


@pytest.fixture(scope="module")
def sales_table(manager):
    name = manager.create_table(SALES_DATA)
    yield name
    manager.cleanup_table(name)


@pytest.fixture(scope="module")
def types_table(manager):
    name = manager.create_table(TYPES_DATA)
    yield name
    manager.cleanup_table(name)


@pytest.fixture(scope="module")
def partitioned_table(manager):
    name = manager.create_partitioned_table(PARTITIONED_DATA, partition_column="region")
    yield name
    manager.cleanup_table(name)


@pytest.fixture(scope="module")
def multi_snapshot_table(manager):
    batch1 = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "value": pa.array(["a", "b", "c"], type=pa.string()),
        }
    )
    batch2 = pa.table(
        {
            "id": pa.array([4, 5, 6], type=pa.int64()),
            "value": pa.array(["d", "e", "f"], type=pa.string()),
        }
    )
    result = manager.create_table_multi_snapshot([batch1, batch2])
    yield result
    manager.cleanup_table(result[0])


def _create_engine_table(node, manager, iceberg_table_name, *, settings: str = "") -> str:
    """Create a ClickHouse engine table pointing at an Iceberg table.

    Returns the CH table name.
    """
    ch_tbl = f"eng_{uuid.uuid4().hex[:8]}"
    sql = manager.create_engine_sql(ch_tbl, iceberg_table_name)
    if settings:
        sql += f" SETTINGS {settings}"
    node.query(sql)
    return ch_tbl


# ===================================================================
# Basic connectivity
# ===================================================================


def test_engine_basic_read(node, manager, sales_table):
    """CREATE TABLE ... ENGINE reads correct row count."""
    tbl = _create_engine_table(node, manager, sales_table)
    result = node.query(f"SELECT count() FROM {tbl} FORMAT TSV").strip()
    assert int(result) == 20


def test_engine_select_all(node, manager, sales_table):
    """SELECT * via engine table returns all 20 rows."""
    tbl = _create_engine_table(node, manager, sales_table)
    result = node.query(f"SELECT * FROM {tbl} FORMAT TSV").strip()
    assert len(result.split("\n")) == 20


def test_engine_describe(node, manager, sales_table):
    """DESCRIBE on Iceberg engine table lists all expected columns."""
    tbl = _create_engine_table(node, manager, sales_table)
    result = node.query(f"DESCRIBE {tbl} FORMAT TSV").strip()
    columns = {line.split("\t")[0] for line in result.splitlines()}
    for col in ("id", "product", "category", "amount", "quantity", "region"):
        assert col in columns, f"Column '{col}' missing from DESCRIBE"


def test_engine_show_create(node, manager, sales_table):
    """`SHOW CREATE TABLE` output contains the Iceberg engine name."""
    tbl = _create_engine_table(node, manager, sales_table)
    ddl = node.query(f"SHOW CREATE TABLE {tbl}").strip()
    assert "Iceberg" in ddl


def test_engine_system_tables(node, manager, sales_table):
    """`system.tables` reports the correct Iceberg engine name."""
    tbl = _create_engine_table(node, manager, sales_table)
    row = node.query(
        f"SELECT engine FROM system.tables "
        f"WHERE database = 'default' AND name = '{tbl}' FORMAT TSV"
    ).strip()
    assert "Iceberg" in row


def test_engine_system_columns(node, manager, sales_table):
    """`system.columns` lists all columns with correct types for an engine table."""
    tbl = _create_engine_table(node, manager, sales_table)
    result = node.query(
        f"SELECT name, type FROM system.columns "
        f"WHERE database = 'default' AND table = '{tbl}' "
        f"ORDER BY position FORMAT TSV"
    ).strip()
    columns = {}
    for line in result.splitlines():
        name, col_type = line.split("\t")
        columns[name] = col_type
    assert "id" in columns and "Int" in columns["id"]
    assert "product" in columns and "String" in columns["product"]
    assert "amount" in columns and "Float" in columns["amount"]
    assert len(columns) == 6


# ===================================================================
# Query patterns
# ===================================================================


def test_engine_where_filter(node, manager, sales_table):
    """WHERE clause on engine table returns correct filtered count."""
    tbl = _create_engine_table(node, manager, sales_table)
    east = node.query(
        f"SELECT count() FROM {tbl} WHERE region = 'East' FORMAT TSV"
    ).strip()
    west = node.query(
        f"SELECT count() FROM {tbl} WHERE region = 'West' FORMAT TSV"
    ).strip()
    assert int(east) == 10
    assert int(west) == 10


def test_engine_aggregations(node, manager, sales_table):
    """COUNT, SUM, AVG, MAX, MIN via engine table match reference values."""
    tbl = _create_engine_table(node, manager, sales_table)
    result = node.query(
        f"SELECT count(*), sum(amount), avg(amount), max(amount), min(amount) "
        f"FROM {tbl} FORMAT TSV"
    ).strip()
    parts = result.split("\t")
    assert int(parts[0]) == 20
    assert float(parts[1]) == pytest.approx(569.75)
    assert float(parts[2]) == pytest.approx(28.4875)
    assert float(parts[3]) == pytest.approx(48.0)
    assert float(parts[4]) == pytest.approx(10.5)


def test_engine_group_by(node, manager, sales_table):
    """GROUP BY on engine table returns correct groupings."""
    tbl = _create_engine_table(node, manager, sales_table)
    result = node.query(
        f"SELECT category, region, count(*) "
        f"FROM {tbl} GROUP BY category, region "
        f"ORDER BY category, region FORMAT TSV"
    ).strip()
    lines = result.split("\n")
    assert len(lines) == 4


def test_engine_order_by(node, manager, sales_table):
    """ORDER BY result is stable across repeated queries via engine."""
    tbl = _create_engine_table(node, manager, sales_table)
    sql = f"SELECT id FROM {tbl} ORDER BY amount DESC LIMIT 5 FORMAT TSV"
    r1 = node.query(sql).strip()
    r2 = node.query(sql).strip()
    assert r1 == r2
    ids = [int(x) for x in r1.split("\n")]
    assert ids == [16, 8, 12, 20, 4]


def test_engine_projection_pushdown(node, manager, sales_table):
    """SELECT specific columns returns the correct subset."""
    tbl = _create_engine_table(node, manager, sales_table)
    result = node.query(
        f"SELECT id, product FROM {tbl} ORDER BY id LIMIT 3 FORMAT TSV"
    ).strip()
    lines = result.split("\n")
    assert len(lines) == 3
    assert lines[0].split("\t") == ["1", "Widget A"]


# ===================================================================
# Type mapping
# ===================================================================


def test_engine_primitive_types(node, manager, types_table):
    """Iceberg-to-ClickHouse type mapping is correct for engine-attached tables."""
    tbl = _create_engine_table(node, manager, types_table)
    prim_cols = [
        "col_bool", "col_int32", "col_int64", "col_float32", "col_float64",
        "col_decimal", "col_date", "col_timestamp", "col_timestamptz",
        "col_string", "col_binary",
    ]
    type_expr = ", ".join(f"toTypeName({c})" for c in prim_cols)
    types = node.query(
        f"SELECT {type_expr} FROM {tbl} LIMIT 1 FORMAT TSV"
    ).strip().split("\t")
    expected = {
        0: "Bool", 1: "Int32", 2: "Int64", 3: "Float32", 4: "Float64",
        5: "Decimal", 6: "Date", 7: "DateTime64", 8: "DateTime64",
        9: "String", 10: "String",
    }
    for idx, substr in expected.items():
        assert substr in types[idx], (
            f"{prim_cols[idx]}: expected {substr} in {types[idx]}"
        )


# ===================================================================
# Read-only nature of the engine
# ===================================================================


def test_engine_insert_works(node, manager):
    """INSERT into an Iceberg engine table succeeds when `allow_insert_into_iceberg=1`.

    Creates a fresh table (not reusing any shared fixture) and verifies:
    1. INSERT into an empty table populates it correctly.
    2. A second INSERT into the existing table appends rows correctly.
    """
    schema_sql = "id Int64, name String, value Float64"
    ch_tbl = f"eng_write_{uuid.uuid4().hex[:8]}"
    create_sql, _ = manager.create_writable_table_sql(ch_tbl, schema_sql)
    node.query(create_sql)

    # Insert into the empty table
    node.query(
        f"INSERT INTO {ch_tbl} VALUES (1, 'Alice', 1.5), (2, 'Bob', 2.5)"
    )
    count = node.query(f"SELECT count() FROM {ch_tbl} FORMAT TSV").strip()
    assert int(count) == 2

    # Insert into the existing table (append)
    node.query(f"INSERT INTO {ch_tbl} VALUES (3, 'Carol', 3.5)")
    count = node.query(f"SELECT count() FROM {ch_tbl} FORMAT TSV").strip()
    assert int(count) == 3

    result = node.query(
        f"SELECT id, name, value FROM {ch_tbl} ORDER BY id FORMAT TSV"
    ).strip()
    lines = result.split("\n")
    assert lines[0] == "1\tAlice\t1.5"
    assert lines[1] == "2\tBob\t2.5"
    assert lines[2] == "3\tCarol\t3.5"


# ===================================================================
# CTAS from engine table
# ===================================================================


def test_engine_ctas(node, manager, sales_table):
    """CREATE TABLE AS SELECT from Iceberg engine table copies all rows."""
    src = _create_engine_table(node, manager, sales_table)
    dst = f"local_ctas_{uuid.uuid4().hex[:8]}"
    node.query(
        f"CREATE TABLE {dst} ENGINE = MergeTree ORDER BY id "
        f"AS SELECT * FROM {src}"
    )
    count = node.query(f"SELECT count() FROM {dst} FORMAT TSV").strip()
    assert int(count) == 20


def test_engine_ctas_filtered(node, manager, sales_table):
    """CTAS with WHERE copies only matching rows."""
    src = _create_engine_table(node, manager, sales_table)
    dst = f"local_east_{uuid.uuid4().hex[:8]}"
    node.query(
        f"CREATE TABLE {dst} ENGINE = MergeTree ORDER BY id "
        f"AS SELECT * FROM {src} WHERE region = 'East'"
    )
    count = node.query(f"SELECT count() FROM {dst} FORMAT TSV").strip()
    assert int(count) == 10


# ===================================================================
# Schema evolution
# ===================================================================


def test_engine_schema_evolution_read(node, manager):
    """Engine reads an evolved schema correctly with allow_dynamic_metadata_for_data_lakes.

    Old rows show NULL for the new column; new rows show the actual value.
    """
    initial = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "val": pa.array(["a", "b"], type=pa.string()),
        }
    )
    evolved = pa.table(
        {
            "id": pa.array([3, 4], type=pa.int64()),
            "val": pa.array(["c", "d"], type=pa.string()),
            "extra": pa.array([100, 200], type=pa.int64()),
        }
    )
    table_name = manager.create_table_with_schema_evolution(
        initial_data=initial,
        evolved_data=evolved,
        new_column_name="extra",
        new_column_type=LongType(),
    )
    tbl = _create_engine_table(
        node,
        manager,
        table_name,
        settings="allow_dynamic_metadata_for_data_lakes=1",
    )

    result = node.query(
        f"SELECT id, val, extra FROM {tbl} ORDER BY id FORMAT TSV"
    ).strip()
    lines = result.split("\n")
    assert len(lines) == 4
    # Old rows: extra is NULL
    assert lines[0].split("\t")[2] == "\\N"
    assert lines[1].split("\t")[2] == "\\N"
    # New rows: extra has values
    assert int(lines[2].split("\t")[2]) == 100
    assert int(lines[3].split("\t")[2]) == 200


def test_engine_schema_evolution_describe(node, manager):
    """DESCRIBE on an evolved-schema engine table shows the new column."""
    initial = pa.table(
        {
            "id": pa.array([10], type=pa.int64()),
            "val": pa.array(["x"], type=pa.string()),
        }
    )
    evolved = pa.table(
        {
            "id": pa.array([20], type=pa.int64()),
            "val": pa.array(["y"], type=pa.string()),
            "score": pa.array([99], type=pa.int64()),
        }
    )
    table_name = manager.create_table_with_schema_evolution(
        initial_data=initial,
        evolved_data=evolved,
        new_column_name="score",
        new_column_type=LongType(),
    )
    tbl = _create_engine_table(
        node,
        manager,
        table_name,
        settings="allow_dynamic_metadata_for_data_lakes=1",
    )
    result = node.query(f"DESCRIBE {tbl} FORMAT TSV").strip()
    col_names = {line.split("\t")[0] for line in result.splitlines()}
    assert "id" in col_names
    assert "val" in col_names
    assert "score" in col_names


# ===================================================================
# Time travel
# ===================================================================


def test_engine_time_travel_snapshot_id(node, manager, multi_snapshot_table):
    """Time travel via `iceberg_snapshot_id` setting on engine table."""
    table_name, snapshot_ids = multi_snapshot_table
    tbl = _create_engine_table(node, manager, table_name)

    result = node.query(
        f"SELECT count() FROM {tbl} FORMAT TSV",
        settings={"iceberg_snapshot_id": snapshot_ids[0]},
    ).strip()
    assert int(result) == 3

    result = node.query(
        f"SELECT count() FROM {tbl} FORMAT TSV",
        settings={"iceberg_snapshot_id": snapshot_ids[1]},
    ).strip()
    assert int(result) == 6


def test_engine_time_travel_values(node, manager, multi_snapshot_table):
    """Time travel via snapshot ID returns correct row values."""
    table_name, snapshot_ids = multi_snapshot_table
    tbl = _create_engine_table(node, manager, table_name)

    result = node.query(
        f"SELECT value FROM {tbl} ORDER BY id FORMAT TSV",
        settings={"iceberg_snapshot_id": snapshot_ids[0]},
    ).strip()
    assert result.split("\n") == ["a", "b", "c"]


def test_engine_time_travel_timestamp_ms(node, manager, multi_snapshot_table):
    """Time travel via `iceberg_timestamp_ms` with the first snapshot timestamp."""
    table_name, snapshot_ids = multi_snapshot_table
    tbl = _create_engine_table(node, manager, table_name)

    ts_map = manager.get_snapshot_timestamps(table_name)
    ts_first = ts_map[snapshot_ids[0]]

    result = node.query(
        f"SELECT count() FROM {tbl} FORMAT TSV",
        settings={"iceberg_timestamp_ms": ts_first},
    ).strip()
    assert int(result) == 3


def test_engine_time_travel_both_params_error(node, manager, multi_snapshot_table):
    """Specifying both `iceberg_snapshot_id` and `iceberg_timestamp_ms` raises an exception."""
    import time

    table_name, snapshot_ids = multi_snapshot_table
    tbl = _create_engine_table(node, manager, table_name)

    try:
        node.query(
            f"SELECT count() FROM {tbl} FORMAT TSV",
            settings={
                "iceberg_snapshot_id": snapshot_ids[0],
                "iceberg_timestamp_ms": int(time.time() * 1000),
            },
        )
        assert False, "Expected an exception but query succeeded"
    except Exception as e:
        assert (
            "iceberg_snapshot_id" in str(e)
            or "iceberg_timestamp_ms" in str(e)
            or "Cannot" in str(e)
            or "cannot" in str(e)
        )


# ===================================================================
# Partition pruning
# ===================================================================


def test_engine_partition_pruning(node, manager, partitioned_table):
    """`use_iceberg_partition_pruning=1` filters correctly on engine tables."""
    tbl = _create_engine_table(node, manager, partitioned_table)

    us = node.query(
        f"SELECT count() FROM {tbl} WHERE region = 'US' FORMAT TSV",
        settings={"use_iceberg_partition_pruning": 1},
    ).strip()
    eu = node.query(
        f"SELECT count() FROM {tbl} WHERE region = 'EU' FORMAT TSV",
        settings={"use_iceberg_partition_pruning": 1},
    ).strip()
    none = node.query(
        f"SELECT count() FROM {tbl} WHERE region = 'NONEXISTENT' FORMAT TSV",
        settings={"use_iceberg_partition_pruning": 1},
    ).strip()

    assert int(us) == 6
    assert int(eu) == 6
    assert int(none) == 0


def test_engine_partitioned_aggregation(node, manager, partitioned_table):
    """GROUP BY partition column via engine table returns correct sums."""
    tbl = _create_engine_table(node, manager, partitioned_table)
    result = node.query(
        f"SELECT region, sum(value) FROM {tbl} "
        f"GROUP BY region ORDER BY region FORMAT TSV"
    ).strip()
    lines = result.split("\n")
    assert len(lines) == 2
    eu_parts = lines[0].split("\t")
    us_parts = lines[1].split("\t")
    assert eu_parts[0] == "EU" and int(eu_parts[1]) == 480
    assert us_parts[0] == "US" and int(us_parts[1]) == 300


# ===================================================================
# Metadata cache
# ===================================================================


def test_engine_metadata_cache(node, manager, sales_table):
    """Repeated queries with `use_iceberg_metadata_files_cache=1` are consistent."""
    tbl = _create_engine_table(node, manager, sales_table)
    r1 = node.query(
        f"SELECT count() FROM {tbl} FORMAT TSV",
        settings={"use_iceberg_metadata_files_cache": 1},
    ).strip()
    r2 = node.query(
        f"SELECT count() FROM {tbl} FORMAT TSV",
        settings={"use_iceberg_metadata_files_cache": 1},
    ).strip()
    assert r1 == r2 == "20"


def test_engine_metadata_cache_toggle(node, manager, sales_table):
    """Cache on vs off produces identical results."""
    tbl = _create_engine_table(node, manager, sales_table)
    no_cache = node.query(
        f"SELECT sum(amount) FROM {tbl} FORMAT TSV",
        settings={"use_iceberg_metadata_files_cache": 0},
    ).strip()
    with_cache = node.query(
        f"SELECT sum(amount) FROM {tbl} FORMAT TSV",
        settings={"use_iceberg_metadata_files_cache": 1},
    ).strip()
    assert float(no_cache) == pytest.approx(float(with_cache))


# ===================================================================
# Named collections
# ===================================================================


@only_s3
def test_engine_named_collection_s3(node, manager, sales_table):
    """Engine table created via named collection reads data correctly."""
    cfg = manager.config
    url = manager.s3_url(sales_table)
    nc_name = f"nc_{uuid.uuid4().hex[:8]}"
    node.query(
        f"CREATE NAMED COLLECTION {nc_name} AS "
        f"url = '{url}', "
        f"access_key_id = '{cfg.access_key_id}', "
        f"secret_access_key = '{cfg.secret_access_key}'"
    )
    tbl = f"eng_nc_{uuid.uuid4().hex[:8]}"
    node.query(f"CREATE TABLE {tbl} ENGINE = IcebergS3({nc_name})")
    result = node.query(f"SELECT count() FROM {tbl} FORMAT TSV").strip()
    assert int(result) == 20


@only_azure
def test_engine_named_collection_azure(node, manager, sales_table):
    """Engine table created via Azure named collection reads data correctly."""
    cfg = manager.config
    bp = manager.blob_path(sales_table)
    nc_name = f"nc_{uuid.uuid4().hex[:8]}"
    node.query(
        f"CREATE NAMED COLLECTION {nc_name} AS "
        f"storage_account_url = '{cfg.storage_account_url}', "
        f"container = '{cfg.container_name}', "
        f"account_name = '{cfg.account_name}', "
        f"account_key = '{cfg.account_key}'"
    )
    tbl = f"eng_nc_{uuid.uuid4().hex[:8]}"
    node.query(
        f"CREATE TABLE {tbl} ENGINE = IcebergAzure({nc_name}, blob_path = '{bp}')"
    )
    result = node.query(f"SELECT count() FROM {tbl} FORMAT TSV").strip()
    assert int(result) == 20


# ===================================================================
# Iceberg alias (IcebergS3 only)
# ===================================================================


@only_s3
def test_engine_iceberg_alias(node, manager, sales_table):
    """`ENGINE = Iceberg` is an alias for `ENGINE = IcebergS3`."""
    cfg = manager.config
    url = manager.s3_url(sales_table)
    tbl = f"eng_alias_{uuid.uuid4().hex[:8]}"
    node.query(
        f"CREATE TABLE {tbl} ENGINE = Iceberg("
        f"'{url}', '{cfg.access_key_id}', '{cfg.secret_access_key}')"
    )
    ddl = node.query(f"SHOW CREATE TABLE {tbl}").strip()
    assert "Iceberg" in ddl

    result = node.query(f"SELECT count() FROM {tbl} FORMAT TSV").strip()
    assert int(result) == 20


# ===================================================================
# Multiple engine tables on the same Iceberg table
# ===================================================================


def test_engine_multiple_tables_same_iceberg(node, manager, sales_table):
    """Two separate engine tables pointing to the same Iceberg table give consistent reads."""
    tbl1 = _create_engine_table(node, manager, sales_table)
    tbl2 = _create_engine_table(node, manager, sales_table)

    c1 = node.query(f"SELECT count() FROM {tbl1} FORMAT TSV").strip()
    c2 = node.query(f"SELECT count() FROM {tbl2} FORMAT TSV").strip()
    assert c1 == c2 == "20"

    s1 = node.query(f"SELECT sum(amount) FROM {tbl1} FORMAT TSV").strip()
    s2 = node.query(f"SELECT sum(amount) FROM {tbl2} FORMAT TSV").strip()
    assert float(s1) == pytest.approx(float(s2))


# ===================================================================
# JOIN
# ===================================================================


def test_engine_join_two_engine_tables(node, manager, sales_table, partitioned_table):
    """JOIN two Iceberg engine tables produces correct results."""
    sales_eng = _create_engine_table(node, manager, sales_table)
    part_eng = _create_engine_table(node, manager, partitioned_table)

    result = node.query(
        f"SELECT count() FROM {sales_eng} AS s "
        f"INNER JOIN {part_eng} AS p ON s.id = p.id FORMAT TSV"
    ).strip()
    # sales has ids 1-20, partitioned has ids 1-12
    assert int(result) == 12


def test_engine_join_with_local_table(node, manager, sales_table):
    """JOIN between Iceberg engine table and a local MergeTree table."""
    src = _create_engine_table(node, manager, sales_table)
    local = f"local_join_{uuid.uuid4().hex[:8]}"
    node.query(
        f"CREATE TABLE {local} (product String, discount Float64) "
        f"ENGINE = MergeTree ORDER BY product"
    )
    node.query(
        f"INSERT INTO {local} VALUES "
        f"('Widget A', 0.10), ('Widget B', 0.20), "
        f"('Gadget X', 0.15), ('Gadget Y', 0.05)"
    )
    result = node.query(
        f"SELECT s.id, s.product, l.discount "
        f"FROM {src} AS s "
        f"INNER JOIN {local} AS l ON s.product = l.product "
        f"ORDER BY s.id LIMIT 3 FORMAT TSV"
    ).strip()
    lines = result.split("\n")
    assert len(lines) == 3
    assert "Widget A" in lines[0]
    assert "0.1" in lines[0]


# ===================================================================
# Metadata resolution settings
# ===================================================================


def test_engine_iceberg_metadata_table_uuid(node, manager, sales_table):
    """`iceberg_metadata_table_uuid` setting is accepted at CREATE TABLE time.

    We extract the actual table UUID from the metadata and verify that
    CREATE TABLE with the correct UUID reads data successfully.
    """
    import json
    import os

    # Load the iceberg table from the local catalog to get the UUID
    local_catalog = manager._catalog
    tbl_meta = local_catalog.load_table(f"default.{sales_table}").metadata
    table_uuid = str(tbl_meta.table_uuid)

    ch_tbl = f"eng_uuid_{uuid.uuid4().hex[:8]}"
    sql = manager.create_engine_sql(ch_tbl, sales_table)
    # Replace IF NOT EXISTS and append SETTINGS
    sql = sql.replace("IF NOT EXISTS ", "")
    sql += f" SETTINGS iceberg_metadata_table_uuid = '{table_uuid}'"
    node.query(sql)

    result = node.query(f"SELECT count() FROM {ch_tbl} FORMAT TSV").strip()
    assert int(result) == 20


def test_engine_recent_metadata_by_last_updated_ms(node, manager, sales_table):
    """`iceberg_recent_metadata_file_by_last_updated_ms_field=1` selects metadata
    by the largest `last-updated-ms` value instead of the highest version number.
    Both strategies should yield the same (latest) snapshot for a fresh table.
    """
    tbl = _create_engine_table(
        node,
        manager,
        sales_table,
        settings="iceberg_recent_metadata_file_by_last_updated_ms_field=1",
    )
    result = node.query(f"SELECT count() FROM {tbl} FORMAT TSV").strip()
    assert int(result) == 20
