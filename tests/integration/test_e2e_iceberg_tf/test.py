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
        "note": pa.array(
            [
                "first sale", None, "bulk order", None, "repeat",
                None, "new customer", None, "discount", None,
                "online", None, "in-store", None, "wholesale",
                None, "promo", None, "referral", None,
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

COMPLEX_DATA = pa.table(
    {
        "id": pa.array([1, 2], type=pa.int64()),
        "col_array": pa.array(
            [[10, 20, 30], [40, 50]], type=pa.list_(pa.int32())
        ),
        "col_map": pa.array(
            [{"a": 1, "b": 2}, {"c": 3}],
            type=pa.map_(pa.string(), pa.int32()),
        ),
        "col_struct": pa.array(
            [{"x": 10, "y": "foo"}, {"x": 20, "y": "bar"}],
            type=pa.struct([("x", pa.int32()), ("y", pa.string())]),
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


_COMMON_MAIN_CONFIGS = ["configs/merge_tree.xml", "configs/cluster.xml"]
_COMMON_USER_CONFIGS = ["configs/allow_experimental.xml"]


@pytest.fixture(scope="module")
def started_cluster(manager):
    cluster = ClickHouseCluster(__file__, name=f"test_e2e_iceberg_tf_{manager}")
    cluster.add_instance(
        "node",
        main_configs=_COMMON_MAIN_CONFIGS,
        user_configs=_COMMON_USER_CONFIGS,
        stay_alive=True,
    )
    cluster.add_instance(
        "node2",
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
def complex_table(manager):
    name = manager.create_table(COMPLEX_DATA)
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


@pytest.fixture(scope="module")
def schema_evolved_table(manager):
    initial = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "value": pa.array(["a", "b"], type=pa.string()),
        }
    )
    evolved = pa.table(
        {
            "id": pa.array([3, 4], type=pa.int64()),
            "value": pa.array(["c", "d"], type=pa.string()),
            "extra": pa.array([100, 200], type=pa.int64()),
        }
    )
    name = manager.create_table_with_schema_evolution(
        initial_data=initial,
        evolved_data=evolved,
        new_column_name="extra",
        new_column_type=LongType(),
    )
    yield name
    manager.cleanup_table(name)


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


@pytest.fixture(scope="module")
def partitioned_table(manager):
    name = manager.create_partitioned_table(
        PARTITIONED_DATA, partition_column="region"
    )
    yield name
    manager.cleanup_table(name)


def _tf(mgr, table_name):
    """Shorthand for table function SQL expression."""
    return mgr.table_function_sql(table_name)


# ===================================================================
# Basic connectivity
# ===================================================================


def test_table_function_basic(node, manager, sales_table):
    """Read via Iceberg table function returns correct row count."""
    tf = _tf(manager, sales_table)
    result = node.query(f"SELECT count() FROM {tf} FORMAT TSV").strip()
    assert int(result) == 20


def test_table_engine_basic(node, manager, sales_table):
    """Read via Iceberg table engine returns correct row count."""
    tbl = f"iceberg_engine_{uuid.uuid4().hex[:8]}"
    node.query(manager.create_engine_sql(tbl, sales_table))
    result = node.query(f"SELECT count() FROM {tbl} FORMAT TSV").strip()
    assert int(result) == 20


def test_describe_table_function(node, manager, sales_table):
    """DESCRIBE on Iceberg table function lists all columns."""
    tf = _tf(manager, sales_table)
    result = node.query(f"DESCRIBE {tf} FORMAT TSV").strip()
    columns = {line.split("\t")[0] for line in result.splitlines()}
    for expected_col in ("id", "product", "category", "amount", "quantity", "region", "note"):
        assert expected_col in columns, f"Column '{expected_col}' missing from DESCRIBE"


def test_describe_table_engine(node, manager, sales_table):
    """DESCRIBE on Iceberg engine table lists all columns."""
    tbl = f"iceberg_desc_{uuid.uuid4().hex[:8]}"
    node.query(manager.create_engine_sql(tbl, sales_table))
    result = node.query(f"DESCRIBE {tbl} FORMAT TSV").strip()
    columns = {line.split("\t")[0] for line in result.splitlines()}
    assert "id" in columns
    assert "product" in columns


@only_azure
def test_azure_connection_string_auth(node, manager, sales_table):
    """Auth via connection string reads data correctly."""
    tf = manager.table_function_conn_string_sql(sales_table)
    result = node.query(f"SELECT count() FROM {tf} FORMAT TSV").strip()
    assert int(result) == 20


# ===================================================================
# Query patterns
# ===================================================================


def test_select_all(node, manager, sales_table):
    """SELECT * returns all 20 rows with all columns."""
    tf = _tf(manager, sales_table)
    result = node.query(f"SELECT * FROM {tf} FORMAT TSV").strip()
    lines = result.split("\n")
    assert len(lines) == 20


def test_select_limit(node, manager, sales_table):
    """SELECT * LIMIT 1 returns exactly one row."""
    tf = _tf(manager, sales_table)
    result = node.query(f"SELECT * FROM {tf} LIMIT 1 FORMAT TSV").strip()
    lines = result.split("\n")
    assert len(lines) == 1
    cols = lines[0].split("\t")
    assert len(cols) >= 7


def test_projection_pushdown(node, manager, sales_table):
    """SELECT specific columns returns correct subset."""
    tf = _tf(manager, sales_table)
    result = node.query(
        f"SELECT id, product FROM {tf} ORDER BY id FORMAT TSV"
    ).strip()
    lines = result.split("\n")
    assert len(lines) == 20
    first = lines[0].split("\t")
    assert first[0] == "1"
    assert first[1] == "Widget A"


def test_where_filter(node, manager, sales_table):
    """WHERE on string column returns correct filtered count."""
    tf = _tf(manager, sales_table)
    east = node.query(
        f"SELECT count() FROM {tf} WHERE region = 'East' FORMAT TSV"
    ).strip()
    west = node.query(
        f"SELECT count() FROM {tf} WHERE region = 'West' FORMAT TSV"
    ).strip()
    assert int(east) == 10
    assert int(west) == 10


def test_where_non_partition_column(node, manager, sales_table):
    """WHERE on non-partition column returns correct results."""
    tf = _tf(manager, sales_table)
    result = node.query(
        f"SELECT id FROM {tf} WHERE product = 'Widget A' ORDER BY id FORMAT TSV"
    ).strip()
    ids = [int(x) for x in result.split("\n")]
    assert ids == [1, 5, 9, 13, 17]


def test_aggregations(node, manager, sales_table):
    """COUNT, SUM, AVG, MAX, MIN match reference values."""
    tf = _tf(manager, sales_table)
    result = node.query(
        f"SELECT count(*), sum(amount), avg(amount), max(amount), min(amount) "
        f"FROM {tf} FORMAT TSV"
    ).strip()
    parts = result.split("\t")
    assert int(parts[0]) == 20
    assert float(parts[1]) == pytest.approx(569.75)
    assert float(parts[2]) == pytest.approx(28.4875)
    assert float(parts[3]) == pytest.approx(48.0)
    assert float(parts[4]) == pytest.approx(10.5)


def test_group_by(node, manager, sales_table):
    """GROUP BY category, region returns correct groupings and sums."""
    tf = _tf(manager, sales_table)
    result = node.query(
        f"SELECT category, region, count(*), sum(amount) "
        f"FROM {tf} GROUP BY category, region "
        f"ORDER BY category, region FORMAT TSV"
    ).strip()
    lines = result.split("\n")
    assert len(lines) == 4

    groups = {}
    for line in lines:
        p = line.split("\t")
        groups[(p[0], p[1])] = (int(p[2]), float(p[3]))

    assert groups[("Gadgets", "East")] == (5, pytest.approx(183.75))
    assert groups[("Gadgets", "West")] == (5, pytest.approx(201.25))
    assert groups[("Widgets", "East")] == (5, pytest.approx(92.25))
    assert groups[("Widgets", "West")] == (5, pytest.approx(92.50))


def test_order_by(node, manager, sales_table):
    """ORDER BY is stable across repeated runs."""
    tf = _tf(manager, sales_table)
    sql = f"SELECT id FROM {tf} ORDER BY amount DESC LIMIT 5 FORMAT TSV"
    r1 = node.query(sql).strip()
    r2 = node.query(sql).strip()
    assert r1 == r2
    ids = [int(x) for x in r1.split("\n")]
    assert ids == [16, 8, 12, 20, 4]


def test_nullable_handling(node, manager, sales_table):
    """NULLs are preserved; IS NULL / IS NOT NULL predicates work."""
    tf = _tf(manager, sales_table)
    null_count = node.query(
        f"SELECT count(*) FROM {tf} WHERE note IS NULL FORMAT TSV"
    ).strip()
    not_null_count = node.query(
        f"SELECT count(*) FROM {tf} WHERE note IS NOT NULL FORMAT TSV"
    ).strip()
    assert int(null_count) == 10
    assert int(not_null_count) == 10

    result = node.query(
        f"SELECT id, note FROM {tf} ORDER BY id LIMIT 4 FORMAT TSV"
    ).strip()
    lines = result.split("\n")
    assert lines[0].split("\t")[1] == "first sale"
    assert lines[1].split("\t")[1] == "\\N"
    assert lines[2].split("\t")[1] == "bulk order"
    assert lines[3].split("\t")[1] == "\\N"


# ===================================================================
# Type mapping
# ===================================================================


def test_primitive_types(node, manager, types_table):
    """Iceberg to ClickHouse type mapping for all primitive types."""
    tf = _tf(manager, types_table)
    prim_cols = [
        "col_bool", "col_int32", "col_int64", "col_float32", "col_float64",
        "col_decimal", "col_date", "col_timestamp", "col_timestamptz",
        "col_string", "col_binary",
    ]
    type_expr = ", ".join(f"toTypeName({c})" for c in prim_cols)
    types = node.query(
        f"SELECT {type_expr} FROM {tf} LIMIT 1 FORMAT TSV"
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


def test_primitive_values(node, manager, types_table):
    """Actual primitive values match the expected reference data."""
    tf = _tf(manager, types_table)
    cols = ", ".join([
        "col_bool", "col_int32", "col_int64", "col_float32", "col_float64",
        "col_decimal", "col_date", "col_timestamp", "col_timestamptz",
        "col_string",
    ])
    result = node.query(
        f"SELECT {cols} FROM {tf} ORDER BY col_int32 FORMAT TSV"
    ).strip()
    lines = result.split("\n")
    assert len(lines) == 2

    r = lines[0].split("\t")
    assert r[0] in ("false", "0")
    assert int(r[1]) == -100000
    assert int(r[2]) == -4294967296
    assert float(r[3]) == pytest.approx(-1.5)
    assert float(r[4]) == pytest.approx(-3.141592653589793)
    assert decimal.Decimal(r[5]) == decimal.Decimal("-12345.67")
    assert r[6] == "2000-06-15"
    assert "2025-06-15" in r[7]
    assert "2025-06-15" in r[8]
    assert r[9] == "world"

    r = lines[1].split("\t")
    assert r[0] in ("true", "1")
    assert int(r[1]) == 100000
    assert int(r[2]) == 4294967296
    assert float(r[3]) == pytest.approx(1.5)
    assert float(r[4]) == pytest.approx(3.141592653589793)
    assert decimal.Decimal(r[5]) == decimal.Decimal("12345.67")
    assert r[6] == "2025-01-01"
    assert "2025-06-15" in r[7]
    assert "2025-06-15" in r[8]
    assert r[9] == "hello"


def test_complex_types(node, manager, complex_table):
    """Array, Map, Struct types map correctly."""
    tf = _tf(manager, complex_table)

    type_result = node.query(
        f"SELECT toTypeName(col_array), toTypeName(col_map), toTypeName(col_struct) "
        f"FROM {tf} LIMIT 1 FORMAT TSV"
    ).strip().split("\t")
    assert "Array" in type_result[0]
    assert "Map" in type_result[1]
    assert "Tuple" in type_result[2]


def test_complex_type_values(node, manager, complex_table):
    """Complex type values round-trip correctly."""
    tf = _tf(manager, complex_table)
    result = node.query(
        f"SELECT id, col_array, col_struct FROM {tf} ORDER BY id FORMAT TSV"
    ).strip()
    lines = result.split("\n")
    assert len(lines) == 2
    assert lines[0].split("\t")[1] == "[10,20,30]"
    assert lines[1].split("\t")[1] == "[40,50]"
    assert "10" in lines[0].split("\t")[2] and "foo" in lines[0].split("\t")[2]
    assert "20" in lines[1].split("\t")[2] and "bar" in lines[1].split("\t")[2]


# ===================================================================
# Virtual columns
# ===================================================================


def test_virtual_columns(node, manager, sales_table):
    """Virtual columns _path and _file are populated."""
    tf = _tf(manager, sales_table)
    result = node.query(
        f"SELECT _path, _file FROM {tf} LIMIT 1 FORMAT TSV"
    ).strip()
    parts = result.split("\t")
    assert len(parts) == 2
    assert parts[0] != ""
    assert parts[1] != ""
    assert ".parquet" in parts[1].lower() or "parquet" in parts[0].lower()


# ===================================================================
# CTAS and data movement
# ===================================================================


def test_ctas(node, manager, sales_table):
    """CREATE TABLE AS SELECT from Iceberg table function copies all rows."""
    tf = _tf(manager, sales_table)
    local = f"local_ctas_{uuid.uuid4().hex[:8]}"
    node.query(
        f"CREATE TABLE {local} ENGINE = MergeTree ORDER BY id "
        f"AS SELECT * FROM {tf}"
    )
    count = node.query(f"SELECT count() FROM {local} FORMAT TSV").strip()
    assert int(count) == 20


def test_ctas_with_cast(node, manager, sales_table):
    """CTAS with CAST type override produces correct types in local table."""
    tf = _tf(manager, sales_table)
    local = f"local_cast_{uuid.uuid4().hex[:8]}"
    node.query(
        f"CREATE TABLE {local} ENGINE = MergeTree ORDER BY id AS "
        f"SELECT id, CAST(amount AS Decimal(10,2)) AS amount_dec, "
        f"CAST(quantity AS UInt64) AS qty FROM {tf}"
    )
    types = node.query(
        f"SELECT toTypeName(amount_dec), toTypeName(qty) "
        f"FROM {local} LIMIT 1 FORMAT TSV"
    ).strip().split("\t")
    assert "Decimal" in types[0]
    assert "UInt64" in types[1]
    assert int(node.query(f"SELECT count() FROM {local}").strip()) == 20


def test_incremental_insert(node, manager, sales_table):
    """INSERT INTO local SELECT ... WHERE incremental load works."""
    tf = _tf(manager, sales_table)
    local = f"local_inc_{uuid.uuid4().hex[:8]}"
    node.query(
        f"CREATE TABLE {local} (id Int64, product String, amount Float64) "
        f"ENGINE = MergeTree ORDER BY id"
    )
    node.query(
        f"INSERT INTO {local} SELECT id, product, amount "
        f"FROM {tf} WHERE region = 'East'"
    )
    assert int(node.query(f"SELECT count() FROM {local}").strip()) == 10

    node.query(
        f"INSERT INTO {local} SELECT id, product, amount "
        f"FROM {tf} WHERE region = 'West'"
    )
    assert int(node.query(f"SELECT count() FROM {local}").strip()) == 20


# ===================================================================
# Schema evolution (read side)
# ===================================================================


def test_schema_evolution_add_column(node, manager, schema_evolved_table):
    """Reading a table after ADD COLUMN shows NULLs for old rows."""
    tf = _tf(manager, schema_evolved_table)
    result = node.query(
        f"SELECT id, value, extra FROM {tf} ORDER BY id FORMAT TSV"
    ).strip()
    lines = result.split("\n")
    assert len(lines) == 4

    assert lines[0].split("\t")[2] == "\\N"
    assert lines[1].split("\t")[2] == "\\N"
    assert int(lines[2].split("\t")[2]) == 100
    assert int(lines[3].split("\t")[2]) == 200


def test_schema_evolution_describe(node, manager, schema_evolved_table):
    """DESCRIBE shows the evolved schema with the new column."""
    tf = _tf(manager, schema_evolved_table)
    result = node.query(f"DESCRIBE {tf} FORMAT TSV").strip()
    columns = {line.split("\t")[0] for line in result.splitlines()}
    assert "id" in columns
    assert "value" in columns
    assert "extra" in columns


# ===================================================================
# Time travel
# ===================================================================


def test_time_travel_snapshot_id(node, manager, multi_snapshot_table):
    """Query with iceberg_snapshot_id returns correct historical data."""
    table_name, snapshot_ids = multi_snapshot_table
    tf = _tf(manager, table_name)

    result = node.query(
        f"SELECT count() FROM {tf} FORMAT TSV",
        settings={"iceberg_snapshot_id": snapshot_ids[0]},
    ).strip()
    assert int(result) == 3

    result = node.query(
        f"SELECT count() FROM {tf} FORMAT TSV",
        settings={"iceberg_snapshot_id": snapshot_ids[1]},
    ).strip()
    assert int(result) == 6


def test_time_travel_values(node, manager, multi_snapshot_table):
    """Time travel returns correct values for the first snapshot."""
    table_name, snapshot_ids = multi_snapshot_table
    tf = _tf(manager, table_name)

    result = node.query(
        f"SELECT value FROM {tf} ORDER BY id FORMAT TSV",
        settings={"iceberg_snapshot_id": snapshot_ids[0]},
    ).strip()
    assert result.split("\n") == ["a", "b", "c"]

    result = node.query(
        f"SELECT value FROM {tf} ORDER BY id FORMAT TSV"
    ).strip()
    assert result.split("\n") == ["a", "b", "c", "d", "e", "f"]


# ===================================================================
# Writes: INSERT
# ===================================================================


def test_insert_into_iceberg(node, manager):
    """INSERT INTO Iceberg table and read back."""
    ch_tbl = f"iceberg_write_{uuid.uuid4().hex[:8]}"
    sql, _ = manager.create_writable_table_sql(
        ch_tbl,
        "x Nullable(String), y Nullable(Int32)",
    )
    node.query(sql)
    node.query(f"INSERT INTO {ch_tbl} VALUES ('hello', 1), ('world', 2)")
    result = node.query(
        f"SELECT x, y FROM {ch_tbl} ORDER BY y FORMAT TSV"
    ).strip()
    assert result == "hello\t1\nworld\t2"


def test_insert_multiple_batches(node, manager):
    """Multiple INSERTs create multiple snapshots, all data visible."""
    ch_tbl = f"iceberg_multi_{uuid.uuid4().hex[:8]}"
    sql, _ = manager.create_writable_table_sql(
        ch_tbl,
        "id Nullable(Int64), val Nullable(String)",
    )
    node.query(sql)
    node.query(f"INSERT INTO {ch_tbl} VALUES (1, 'a'), (2, 'b')")
    node.query(f"INSERT INTO {ch_tbl} VALUES (3, 'c'), (4, 'd')")
    result = node.query(
        f"SELECT count() FROM {ch_tbl} FORMAT TSV"
    ).strip()
    assert int(result) == 4

    result = node.query(
        f"SELECT val FROM {ch_tbl} ORDER BY id FORMAT TSV"
    ).strip()
    assert result == "a\nb\nc\nd"


# ===================================================================
# Writes: DELETE
# ===================================================================


def test_delete_from_iceberg(node, manager):
    """DELETE from Iceberg table removes rows correctly."""
    ch_tbl = f"iceberg_del_{uuid.uuid4().hex[:8]}"
    sql, _ = manager.create_writable_table_sql(
        ch_tbl,
        "x Nullable(String), y Nullable(Int32)",
    )
    node.query(sql)
    node.query(
        f"INSERT INTO {ch_tbl} VALUES ('keep', 1), ('drop', 2), ('keep2', 3)"
    )
    node.query(
        f"ALTER TABLE {ch_tbl} DELETE WHERE y = 2",
        settings={"mutations_sync": 1},
    )
    result = node.query(
        f"SELECT x FROM {ch_tbl} ORDER BY y FORMAT TSV"
    ).strip()
    assert result == "keep\nkeep2"


# ===================================================================
# Writes: Schema evolution
# ===================================================================


def test_schema_evolution_add_column_write(node, manager):
    """ALTER TABLE ADD COLUMN on Iceberg table."""
    ch_tbl = f"iceberg_addcol_{uuid.uuid4().hex[:8]}"
    sql, _ = manager.create_writable_table_sql(
        ch_tbl,
        "x Nullable(String), y Nullable(Int32)",
    )
    node.query(sql)
    node.query(f"INSERT INTO {ch_tbl} VALUES ('a', 1)")

    node.query(f"ALTER TABLE {ch_tbl} ADD COLUMN z Nullable(Int64)")
    ddl = node.query(f"SHOW CREATE TABLE {ch_tbl}").strip()
    assert "z" in ddl

    result = node.query(
        f"SELECT x, y, z FROM {ch_tbl} FORMAT TSV"
    ).strip()
    parts = result.split("\t")
    assert parts[0] == "a"
    assert parts[1] == "1"
    assert parts[2] == "\\N"


def test_schema_evolution_drop_column_write(node, manager):
    """ALTER TABLE DROP COLUMN on Iceberg table."""
    ch_tbl = f"iceberg_dropcol_{uuid.uuid4().hex[:8]}"
    sql, _ = manager.create_writable_table_sql(
        ch_tbl,
        "x Nullable(String), y Nullable(Int32), z Nullable(Int64)",
    )
    node.query(sql)
    node.query(f"INSERT INTO {ch_tbl} VALUES ('a', 1, 100)")

    node.query(f"ALTER TABLE {ch_tbl} DROP COLUMN z")
    ddl = node.query(f"SHOW CREATE TABLE {ch_tbl}").strip()
    assert "`z`" not in ddl

    result = node.query(
        f"SELECT x, y FROM {ch_tbl} FORMAT TSV"
    ).strip()
    assert result == "a\t1"


def test_schema_evolution_rename_column_write(node, manager):
    """ALTER TABLE RENAME COLUMN on Iceberg table."""
    ch_tbl = f"iceberg_rename_{uuid.uuid4().hex[:8]}"
    sql, _ = manager.create_writable_table_sql(
        ch_tbl,
        "x Nullable(String), y Nullable(Int32)",
    )
    node.query(sql)
    node.query(f"INSERT INTO {ch_tbl} VALUES ('a', 1)")

    node.query(f"ALTER TABLE {ch_tbl} RENAME COLUMN y TO value")
    ddl = node.query(f"SHOW CREATE TABLE {ch_tbl}").strip()
    assert "value" in ddl

    result = node.query(
        f"SELECT x, value FROM {ch_tbl} FORMAT TSV"
    ).strip()
    assert result == "a\t1"


def test_schema_evolution_modify_type_write(node, manager):
    """ALTER TABLE MODIFY COLUMN type promotion (Int32 -> Int64)."""
    ch_tbl = f"iceberg_modify_{uuid.uuid4().hex[:8]}"
    sql, _ = manager.create_writable_table_sql(
        ch_tbl,
        "x Nullable(String), y Nullable(Int32)",
    )
    node.query(sql)
    node.query(f"INSERT INTO {ch_tbl} VALUES ('a', 1)")

    node.query(f"ALTER TABLE {ch_tbl} MODIFY COLUMN y Nullable(Int64)")
    ddl = node.query(f"SHOW CREATE TABLE {ch_tbl}").strip()
    assert "Int64" in ddl

    result = node.query(
        f"SELECT x, y FROM {ch_tbl} FORMAT TSV"
    ).strip()
    assert result == "a\t1"


# ===================================================================
# Compaction
# ===================================================================


def test_compaction(request, node, manager):
    """OPTIMIZE TABLE on Iceberg table after DELETE preserves data."""
    if request.node.callspec.params.get("manager") == "azure":
        pytest.xfail(
            "Compaction fails on Azure HNS-enabled storage: "
            "'This operation is not permitted on a non-empty directory' (ClickHouse bug)"
        )
    ch_tbl = f"iceberg_compact_{uuid.uuid4().hex[:8]}"
    sql, _ = manager.create_writable_table_sql(
        ch_tbl,
        "x Nullable(String), y Nullable(Int32)",
    )
    node.query(sql)
    node.query(f"INSERT INTO {ch_tbl} VALUES ('a', 1), ('b', 2)")
    node.query(
        f"ALTER TABLE {ch_tbl} DELETE WHERE y = 2",
        settings={"mutations_sync": 1},
    )
    node.query(
        f"OPTIMIZE TABLE {ch_tbl}",
        settings={"allow_experimental_iceberg_compaction": 1},
    )
    result = node.query(
        f"SELECT x, y FROM {ch_tbl} ORDER BY y FORMAT TSV"
    ).strip()
    assert result == "a\t1"


# ===================================================================
# Error handling
# ===================================================================


@only_azure
def test_wrong_account_key(node, manager, sales_table):
    """Wrong account key produces an error."""
    cfg = manager.config
    bp = manager.blob_path(sales_table)
    tf = (
        f"icebergAzure("
        f"'{cfg.storage_account_url}', "
        f"'{cfg.container_name}', "
        f"'{bp}', "
        f"'{cfg.account_name}', "
        f"'INVALID_KEY_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaQ==')"
    )
    error = node.query_and_get_error(f"SELECT * FROM {tf}")
    assert error, "Expected error with invalid account key"


@only_azure
def test_wrong_container(node, manager, sales_table):
    """Non-existent container produces an error."""
    cfg = manager.config
    bp = manager.blob_path(sales_table)
    tf = (
        f"icebergAzure("
        f"'{cfg.storage_account_url}', "
        f"'nonexistent_container_xyz_{uuid.uuid4().hex[:8]}', "
        f"'{bp}', "
        f"'{cfg.account_name}', "
        f"'{cfg.account_key}')"
    )
    error = node.query_and_get_error(f"SELECT * FROM {tf}")
    assert error, "Expected error with non-existent container"


@only_azure
def test_azure_nonexistent_blob_path(node, manager):
    """Wrong blob path produces an error."""
    cfg = manager.config
    tf = (
        f"icebergAzure("
        f"'{cfg.storage_account_url}', "
        f"'{cfg.container_name}', "
        f"'nonexistent/path/{uuid.uuid4().hex[:8]}/', "
        f"'{cfg.account_name}', "
        f"'{cfg.account_key}')"
    )
    error = node.query_and_get_error(f"SELECT * FROM {tf}")
    assert error, "Expected error with non-existent blob path"


# ===================================================================
# Named collection
# ===================================================================


@only_azure
def test_named_collection_azure(node, manager, sales_table):
    """Reading via named collection works correctly on Azure."""
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
    result = node.query(
        f"SELECT count() FROM icebergAzure({nc_name}, blob_path = '{bp}') FORMAT TSV"
    ).strip()
    assert int(result) == 20


@only_s3
def test_named_collection_s3(node, manager, sales_table):
    """Reading via named collection works correctly on S3."""
    cfg = manager.config
    url = manager.s3_url(sales_table)
    nc_name = f"nc_{uuid.uuid4().hex[:8]}"
    node.query(
        f"CREATE NAMED COLLECTION {nc_name} AS "
        f"url = '{url}', "
        f"access_key_id = '{cfg.access_key_id}', "
        f"secret_access_key = '{cfg.secret_access_key}'"
    )
    result = node.query(
        f"SELECT count() FROM icebergS3({nc_name}) FORMAT TSV"
    ).strip()
    assert int(result) == 20


# ===================================================================
# Join patterns
# ===================================================================


def test_join_iceberg_and_local(node, manager, sales_table):
    """JOIN between Iceberg table function and local MergeTree table."""
    tf = _tf(manager, sales_table)
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
        f"FROM {tf} AS s "
        f"INNER JOIN {local} AS l ON s.product = l.product "
        f"ORDER BY s.id LIMIT 3 FORMAT TSV"
    ).strip()
    lines = result.split("\n")
    assert len(lines) == 3
    assert "Widget A" in lines[0]
    assert "0.1" in lines[0]


# ===================================================================
# System tables
# ===================================================================

def test_system_tables_metadata(node, manager, sales_table):
    """system.columns lists all columns with correct types for Iceberg table."""
    tbl = f"iceberg_cols_{uuid.uuid4().hex[:8]}"
    node.query(manager.create_engine_sql(tbl, sales_table))


    row = node.query(
        f"SELECT engine, total_rows, total_bytes "
        f"FROM system.tables "
        f"WHERE database = 'default' AND name = '{tbl}' "
        f"FORMAT TSV"
    ).strip()
    assert row != "", "Table not found in system.tables"
    parts = row.split("\t")
    assert "Iceberg" in parts[0]


    result = node.query(
        f"SELECT name, type "
        f"FROM system.columns "
        f"WHERE database = 'default' AND table = '{tbl}' "
        f"ORDER BY position FORMAT TSV"
    ).strip()
    columns = {}
    for line in result.splitlines():
        name, col_type = line.split("\t")
        columns[name] = col_type

    assert "id" in columns
    assert "Int" in columns["id"]
    assert "product" in columns
    assert "String" in columns["product"]
    assert "amount" in columns
    assert "Float" in columns["amount"]
    assert "quantity" in columns
    assert "region" in columns
    assert "note" in columns
    assert len(columns) == 7


# ===================================================================
# Metadata cache
# ===================================================================


def test_metadata_cache(node, manager, sales_table):
    """Repeated queries with metadata cache enabled return consistent results."""
    tf = _tf(manager, sales_table)
    r1 = node.query(
        f"SELECT count() FROM {tf} FORMAT TSV",
        settings={"use_iceberg_metadata_files_cache": 1},
    ).strip()
    r2 = node.query(
        f"SELECT count() FROM {tf} FORMAT TSV",
        settings={"use_iceberg_metadata_files_cache": 1},
    ).strip()
    assert r1 == r2 == "20"


def test_metadata_cache_toggle(node, manager, sales_table):
    """Cache on vs off returns same results; multiple runs stay consistent."""
    tf = _tf(manager, sales_table)

    results_no_cache = []
    for _ in range(3):
        r = node.query(
            f"SELECT count() FROM {tf} FORMAT TSV",
            settings={"use_iceberg_metadata_files_cache": 0},
        ).strip()
        results_no_cache.append(r)

    results_with_cache = []
    for _ in range(3):
        r = node.query(
            f"SELECT count() FROM {tf} FORMAT TSV",
            settings={"use_iceberg_metadata_files_cache": 1},
        ).strip()
        results_with_cache.append(r)

    assert all(r == "20" for r in results_no_cache)
    assert all(r == "20" for r in results_with_cache)

    sum_no_cache = node.query(
        f"SELECT sum(amount) FROM {tf} FORMAT TSV",
        settings={"use_iceberg_metadata_files_cache": 0},
    ).strip()
    sum_with_cache = node.query(
        f"SELECT sum(amount) FROM {tf} FORMAT TSV",
        settings={"use_iceberg_metadata_files_cache": 1},
    ).strip()
    assert float(sum_no_cache) == pytest.approx(float(sum_with_cache))

    data_no_cache = node.query(
        f"SELECT id, product FROM {tf} ORDER BY id FORMAT TSV",
        settings={"use_iceberg_metadata_files_cache": 0},
    ).strip()
    data_with_cache = node.query(
        f"SELECT id, product FROM {tf} ORDER BY id FORMAT TSV",
        settings={"use_iceberg_metadata_files_cache": 1},
    ).strip()
    assert data_no_cache == data_with_cache


# ===================================================================
# Partitioned tables
# ===================================================================


def test_partitioned_table_basic(node, manager, partitioned_table):
    """Partitioned Iceberg table returns all rows."""
    tf = _tf(manager, partitioned_table)
    result = node.query(f"SELECT count() FROM {tf} FORMAT TSV").strip()
    assert int(result) == 12


def test_partitioned_table_filter_partition_column(node, manager, partitioned_table):
    """WHERE on partition column returns correct filtered rows."""
    tf = _tf(manager, partitioned_table)
    us = node.query(
        f"SELECT count() FROM {tf} WHERE region = 'US' FORMAT TSV"
    ).strip()
    eu = node.query(
        f"SELECT count() FROM {tf} WHERE region = 'EU' FORMAT TSV"
    ).strip()
    assert int(us) == 6
    assert int(eu) == 6


def test_partitioned_table_filter_non_partition_column(node, manager, partitioned_table):
    """WHERE on non-partition column works on partitioned table."""
    tf = _tf(manager, partitioned_table)
    result = node.query(
        f"SELECT id FROM {tf} WHERE value > 60 ORDER BY id FORMAT TSV"
    ).strip()
    ids = [int(x) for x in result.split("\n")]
    assert ids == [7, 8, 9, 10, 11, 12]


def test_partitioned_table_aggregation(node, manager, partitioned_table):
    """GROUP BY on partition column returns correct sums."""
    tf = _tf(manager, partitioned_table)
    result = node.query(
        f"SELECT region, sum(value) FROM {tf} "
        f"GROUP BY region ORDER BY region FORMAT TSV"
    ).strip()
    lines = result.split("\n")
    assert len(lines) == 2
    eu_parts = lines[0].split("\t")
    us_parts = lines[1].split("\t")
    assert eu_parts[0] == "EU"
    assert int(eu_parts[1]) == 480
    assert us_parts[0] == "US"
    assert int(us_parts[1]) == 300


def test_partitioned_table_combined_filter(node, manager, partitioned_table):
    """Combining partition and non-partition filters works correctly."""
    tf = _tf(manager, partitioned_table)
    result = node.query(
        f"SELECT id, value FROM {tf} "
        f"WHERE region = 'EU' AND value >= 100 ORDER BY id FORMAT TSV"
    ).strip()
    lines = result.split("\n")
    assert len(lines) == 3
    assert lines[0].split("\t") == ["10", "100"]
    assert lines[1].split("\t") == ["11", "110"]
    assert lines[2].split("\t") == ["12", "120"]


def test_partitioned_table_describe(node, manager, partitioned_table):
    """DESCRIBE on a partitioned Iceberg table shows all columns."""
    tf = _tf(manager, partitioned_table)
    result = node.query(f"DESCRIBE {tf} FORMAT TSV").strip()
    columns = {line.split("\t")[0] for line in result.splitlines()}
    assert "id" in columns
    assert "region" in columns
    assert "value" in columns


def test_partitioned_table_engine(node, manager, partitioned_table):
    """Iceberg engine on a partitioned table reads correctly."""
    tbl = f"iceberg_part_eng_{uuid.uuid4().hex[:8]}"
    node.query(manager.create_engine_sql(tbl, partitioned_table))
    result = node.query(f"SELECT count() FROM {tbl} FORMAT TSV").strip()
    assert int(result) == 12

    us = node.query(
        f"SELECT count() FROM {tbl} WHERE region = 'US' FORMAT TSV"
    ).strip()
    assert int(us) == 6


# ===================================================================
# Time travel: timestamp
# ===================================================================


def test_time_travel_timestamp_ms(node, manager, multi_snapshot_table):
    """Query with iceberg_timestamp_ms returns historical data."""
    table_name, snapshot_ids = multi_snapshot_table
    tf = _tf(manager, table_name)

    import time
    future_ts = int(time.time() * 1000) + 86400_000

    result = node.query(
        f"SELECT count() FROM {tf} FORMAT TSV",
        settings={"iceberg_timestamp_ms": future_ts},
    ).strip()
    assert int(result) == 6


# ===================================================================
# Cluster table functions
# ===================================================================


def test_iceberg_cluster_basic(node, manager, sales_table):
    """Cluster table function reads all rows via distributed execution."""
    tf = manager.table_function_cluster_sql("test_cluster", sales_table)
    result = node.query(f"SELECT count() FROM {tf} FORMAT TSV").strip()
    assert int(result) == 20


def test_iceberg_cluster_filter(node, manager, sales_table):
    """Cluster table function with WHERE filter returns correct results."""
    tf = manager.table_function_cluster_sql("test_cluster", sales_table)
    result = node.query(
        f"SELECT count() FROM {tf} WHERE region = 'East' FORMAT TSV"
    ).strip()
    assert int(result) == 10


def test_iceberg_cluster_aggregation(node, manager, sales_table):
    """Cluster table function supports aggregations across the cluster."""
    tf = manager.table_function_cluster_sql("test_cluster", sales_table)
    result = node.query(
        f"SELECT sum(amount) FROM {tf} FORMAT TSV"
    ).strip()
    assert float(result) == pytest.approx(569.75)


def test_iceberg_cluster_order_by(node, manager, sales_table):
    """Cluster table function returns data in correct ORDER BY."""
    tf = manager.table_function_cluster_sql("test_cluster", sales_table)
    result = node.query(
        f"SELECT id FROM {tf} ORDER BY id LIMIT 5 FORMAT TSV"
    ).strip()
    ids = [int(x) for x in result.split("\n")]
    assert ids == [1, 2, 3, 4, 5]


def test_iceberg_cluster_partitioned(node, manager, partitioned_table):
    """Cluster table function works with partitioned tables."""
    tf = manager.table_function_cluster_sql("test_cluster", partitioned_table)
    result = node.query(f"SELECT count() FROM {tf} FORMAT TSV").strip()
    assert int(result) == 12

    result = node.query(
        f"SELECT region, count() FROM {tf} GROUP BY region ORDER BY region FORMAT TSV"
    ).strip()
    lines = result.split("\n")
    assert lines[0].split("\t") == ["EU", "6"]
    assert lines[1].split("\t") == ["US", "6"]


# ===================================================================
# IcebergS3 with Glue catalog settings
# ===================================================================


@only_s3
def test_iceberg_s3_glue_catalog_settings(node, manager, sales_table):
    """IcebergS3 with storage_catalog_type='glue' SETTINGS reads data."""
    tbl = f"iceberg_glue_{uuid.uuid4().hex[:8]}"
    node.query(manager.create_engine_with_glue_sql(tbl, sales_table))
    result = node.query(f"SELECT count() FROM {tbl} FORMAT TSV").strip()
    assert int(result) == 20


# ===================================================================
# Time travel: timestamp-based (historical)
# ===================================================================


def test_time_travel_timestamp_ms_historical(node, manager, multi_snapshot_table):
    """iceberg_timestamp_ms with a timestamp between snapshots returns only
    the data committed in the first snapshot."""
    table_name, snapshot_ids = multi_snapshot_table
    tf = _tf(manager, table_name)

    ts_map = manager.get_snapshot_timestamps(table_name)
    # Timestamp of the first snapshot; querying at exactly this point should
    # return 3 rows (the first batch only).
    ts_first = ts_map[snapshot_ids[0]]

    result = node.query(
        f"SELECT count() FROM {tf} FORMAT TSV",
        settings={"iceberg_timestamp_ms": ts_first},
    ).strip()
    assert int(result) == 3

    result = node.query(
        f"SELECT value FROM {tf} ORDER BY id FORMAT TSV",
        settings={"iceberg_timestamp_ms": ts_first},
    ).strip()
    assert result.split("\n") == ["a", "b", "c"]


def test_time_travel_both_params_error(node, manager, multi_snapshot_table):
    """Specifying both iceberg_snapshot_id and iceberg_timestamp_ms raises an exception."""
    import time

    table_name, snapshot_ids = multi_snapshot_table
    tf = _tf(manager, table_name)

    try:
        node.query(
            f"SELECT count() FROM {tf} FORMAT TSV",
            settings={
                "iceberg_snapshot_id": snapshot_ids[0],
                "iceberg_timestamp_ms": int(time.time() * 1000),
            },
        )
        assert False, "Expected an exception but query succeeded"
    except Exception as e:
        assert "iceberg_snapshot_id" in str(e) or "iceberg_timestamp_ms" in str(e) or "Cannot" in str(e) or "cannot" in str(e)


# ===================================================================
# Schema evolution + time travel
# ===================================================================


def test_schema_evolution_time_travel(node, manager):
    """After adding a column, a time-travel query to a pre-evolution snapshot
    must not include the new column."""
    batch1 = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "val": pa.array(["x", "y"], type=pa.string()),
        }
    )
    batch2 = pa.table(
        {
            "id": pa.array([3, 4], type=pa.int64()),
            "val": pa.array(["z", "w"], type=pa.string()),
            "extra": pa.array([10, 20], type=pa.int64()),
        }
    )
    from pyiceberg.types import LongType as _LongType

    table_name = manager.create_table_with_schema_evolution(
        initial_data=batch1,
        evolved_data=batch2,
        new_column_name="extra",
        new_column_type=_LongType(),
    )
    tf = _tf(manager, table_name)

    ts_map = manager.get_snapshot_timestamps(table_name)
    snapshot_ids = sorted(ts_map.keys(), key=lambda sid: ts_map[sid])
    first_snapshot_id = snapshot_ids[0]

    # At the first snapshot the 'extra' column does not exist yet.
    cols = node.query(
        f"DESCRIBE {tf} FORMAT TSV",
        settings={"iceberg_snapshot_id": first_snapshot_id},
    ).strip()
    col_names = [line.split("\t")[0] for line in cols.split("\n")]
    assert "extra" not in col_names, (
        f"Column 'extra' should not appear in pre-evolution snapshot; got: {col_names}"
    )

    # At the latest snapshot 'extra' is present.
    cols_current = node.query(f"DESCRIBE {tf} FORMAT TSV").strip()
    col_names_current = [line.split("\t")[0] for line in cols_current.split("\n")]
    assert "extra" in col_names_current


# ===================================================================
# Partition pruning
# ===================================================================


def test_partition_pruning_enabled(node, manager, partitioned_table):
    """use_iceberg_partition_pruning=1 correctly filters by partition column."""
    tf = _tf(manager, partitioned_table)

    result = node.query(
        f"SELECT count() FROM {tf} WHERE region = 'US' FORMAT TSV",
        settings={"use_iceberg_partition_pruning": 1},
    ).strip()
    assert int(result) == 6

    result = node.query(
        f"SELECT count() FROM {tf} WHERE region = 'EU' FORMAT TSV",
        settings={"use_iceberg_partition_pruning": 1},
    ).strip()
    assert int(result) == 6

    result = node.query(
        f"SELECT count() FROM {tf} WHERE region = 'NONEXISTENT' FORMAT TSV",
        settings={"use_iceberg_partition_pruning": 1},
    ).strip()
    assert int(result) == 0


# ===================================================================
# iceberg() alias for icebergS3()
# ===================================================================


@only_s3
def test_iceberg_alias(node, manager, sales_table):
    """Table function iceberg() is an alias for icebergS3()."""
    cfg = manager.config
    url = manager.s3_url(sales_table)
    alias_tf = (
        f"iceberg('{url}', '{cfg.access_key_id}', '{cfg.secret_access_key}')"
    )
    result = node.query(f"SELECT count() FROM {alias_tf} FORMAT TSV").strip()
    assert int(result) == 20


# ===================================================================
# Permission: allow_insert_into_iceberg
# ===================================================================


def test_insert_without_allow_setting(node, manager):
    """INSERT into an Iceberg table fails when allow_insert_into_iceberg=0."""
    ch_tbl = f"iceberg_noperm_{uuid.uuid4().hex[:8]}"
    sql, _ = manager.create_writable_table_sql(
        ch_tbl,
        "x Nullable(String)",
    )
    node.query(sql)
    try:
        node.query(
            f"INSERT INTO {ch_tbl} VALUES ('fail')",
            settings={"allow_insert_into_iceberg": 0},
        )
        assert False, "Expected an exception but INSERT succeeded"
    except Exception as e:
        assert "allow_insert_into_iceberg" in str(e) or "not allowed" in str(e).lower() or "SUPPORT_IS_DISABLED" in str(e)
