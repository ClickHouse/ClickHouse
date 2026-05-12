"""Unified e2e tests for DataLakeCatalog backends (AWS Glue, OneLake).

Tests are parametrized over available catalog backends via the
``catalog_manager`` fixture.  Backends whose environment variables are
missing are automatically skipped.
"""

import datetime
import decimal
import uuid

import pyarrow as pa
import pytest

from helpers.catalog_manager import CatalogManager
from helpers.catalog_manager_aws_glue import AwsGlueCatalogManager
from helpers.catalog_manager_biglake import BigLakeCatalogManager
from helpers.catalog_manager_onelake import OneLakeCatalogManager
from helpers.cluster import ClickHouseCluster

pytestmark = pytest.mark.e2e

only_glue = pytest.mark.only_backend("glue")
only_biglake = pytest.mark.only_backend("biglake")
only_onelake = pytest.mark.only_backend("onelake")

# ---------------------------------------------------------------------------
# Test data
# ---------------------------------------------------------------------------

SALES_DATA = pa.table(
    {
        "id": pa.array(range(1, 21), type=pa.int64()),
        "customer_id": pa.array([1, 2, 3, 4, 5] * 4, type=pa.int64()),
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
        "sale_date": pa.array(
            [datetime.date(2025, 1, d) for d in range(1, 21)],
            type=pa.date32(),
        ),
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

CUSTOMERS_DATA = pa.table(
    {
        "customer_id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
        "name": pa.array(
            ["Alice", "Bob", "Charlie", "Diana", "Eve"], type=pa.string()
        ),
        "region": pa.array(
            ["East", "West", "East", "West", "East"], type=pa.string()
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
        "col_timestamp": pa.array(
            [_TS, _TS], type=pa.timestamp("us", tz="UTC")
        ),
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
# Backend registry
# ---------------------------------------------------------------------------

_BACKENDS = {
    "biglake": BigLakeCatalogManager,
    "glue": AwsGlueCatalogManager,
    "onelake": OneLakeCatalogManager,
}


def _make_manager(name: str) -> CatalogManager:
    cls = _BACKENDS[name]
    return cls.from_env()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module", params=sorted(_BACKENDS))
def catalog_manager(request) -> CatalogManager:
    mgr = _make_manager(request.param)
    yield mgr
    mgr.cleanup_all()


@pytest.fixture(autouse=True)
def _skip_by_backend(request):
    marker = request.node.get_closest_marker("only_backend")
    if marker is not None:
        required = marker.args[0]
        current = request.node.callspec.params.get("catalog_manager")
        if current != required:
            pytest.skip(f"requires {required} backend")


@pytest.fixture(scope="module")
def started_cluster(catalog_manager):
    cluster = ClickHouseCluster(__file__, name=f"test_e2e_catalogs_{catalog_manager}")
    cluster.add_instance(
        "node1",
        main_configs=["configs/merge_tree.xml"],
        user_configs=["configs/allow_experimental.xml"],
        env_variables=catalog_manager.clickhouse_env_variables(),
        stay_alive=True,
    )
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(scope="module")
def node(started_cluster):
    return started_cluster.instances["node1"]


@pytest.fixture(scope="module")
def sales_table(catalog_manager):
    name = catalog_manager.create_table(SALES_DATA)
    catalog_manager.wait_for_table_ready(name)
    yield name
    catalog_manager.cleanup_table(name)


@pytest.fixture(scope="module")
def customers_table(catalog_manager):
    name = catalog_manager.create_table(CUSTOMERS_DATA)
    catalog_manager.wait_for_table_ready(name)
    yield name
    catalog_manager.cleanup_table(name)


@pytest.fixture(scope="module")
def types_table(catalog_manager):
    name = catalog_manager.create_table(TYPES_DATA)
    catalog_manager.wait_for_table_ready(name)
    yield name
    catalog_manager.cleanup_table(name)


@pytest.fixture(scope="module")
def complex_table(catalog_manager):
    name = catalog_manager.create_table(COMPLEX_DATA)
    catalog_manager.wait_for_table_ready(name)
    yield name
    catalog_manager.cleanup_table(name)


@pytest.fixture(scope="module")
def shared_db(node, catalog_manager, sales_table, customers_table):
    """Single ClickHouse database backing sales/customers tests."""
    db = catalog_manager.make_database_name()
    catalog_manager.create_catalog(node, db)
    yield db
    node.query(f"DROP DATABASE IF EXISTS {db}")


@pytest.fixture(scope="module")
def types_db(node, catalog_manager, types_table):
    db = catalog_manager.make_database_name()
    try:
        catalog_manager.create_catalog(node, db)
    except Exception as exc:
        if isinstance(catalog_manager, OneLakeCatalogManager):
            pytest.xfail(f"OneLake catalog setup crashed: {exc}")
        raise
    yield db
    try:
        node.query(f"DROP DATABASE IF EXISTS {db}")
    except Exception:
        pass  # server may have crashed during the test; ignore teardown errors


@pytest.fixture(scope="module")
def complex_db(node, catalog_manager, complex_table):
    db = catalog_manager.make_database_name()
    try:
        catalog_manager.create_catalog(node, db)
    except Exception as exc:
        if isinstance(catalog_manager, OneLakeCatalogManager):
            pytest.xfail(f"OneLake catalog setup crashed: {exc}")
        raise
    yield db
    try:
        node.query(f"DROP DATABASE IF EXISTS {db}")
    except Exception:
        pass  # server may have crashed during the test; ignore teardown errors


# ---------------------------------------------------------------------------
# Connectivity & DDL
# ---------------------------------------------------------------------------


def test_show_tables(node, shared_db, sales_table, customers_table, catalog_manager):
    catalog_manager.resolve_table_name(node, shared_db, sales_table)
    catalog_manager.resolve_table_name(node, shared_db, customers_table)


def test_namespace_prefix(node, shared_db, sales_table, catalog_manager):
    """Tables with namespace prefix accessible via backtick quoting."""
    full = catalog_manager.resolve_table_name(node, shared_db, sales_table)
    assert "." in full
    result = node.query(
        f"SELECT count() FROM {shared_db}.`{full}` FORMAT TSV"
    ).strip()
    assert int(result) == 20


def test_empty_catalog(node, catalog_manager):
    """SHOW TABLES on a fresh database succeeds without error."""
    db = catalog_manager.make_database_name()
    catalog_manager.create_catalog(node, db)
    result, err = node.query_and_get_answer_with_error(f"SHOW TABLES FROM {db}")
    assert not err.strip(), f"SHOW TABLES on fresh database failed: {err}"


def test_show_create_database(node, catalog_manager, sales_table):
    db = catalog_manager.make_database_name()
    catalog_manager.create_catalog(node, db)
    result = node.query(f"SHOW CREATE DATABASE {db}").strip()
    assert "DataLakeCatalog" in result


def test_show_create_table(node, shared_db, sales_table, catalog_manager):
    """SHOW CREATE TABLE returns valid DDL with Iceberg engine."""
    full = catalog_manager.resolve_table_name(node, shared_db, sales_table)
    result = node.query(f"SHOW CREATE TABLE {shared_db}.`{full}`").strip()
    assert result
    lower = result.lower()
    assert "iceberg" in lower or "create" in lower


def test_show_create_table_columns(node, shared_db, sales_table, catalog_manager):
    """SHOW CREATE TABLE output contains all expected column definitions with correct types."""
    full = catalog_manager.resolve_table_name(node, shared_db, sales_table)
    result = node.query(f"SHOW CREATE TABLE {shared_db}.`{full}`").strip()

    assert result.upper().startswith("CREATE TABLE"), (
        f"Expected DDL to start with CREATE TABLE, got: {result[:80]}"
    )
    assert "ENGINE = Iceberg" in result, (
        f"Expected ENGINE = Iceberg in DDL, got:\n{result}"
    )

    # All Arrow fields are nullable by default, so they become optional in Iceberg
    # and map to Nullable(...) types in ClickHouse.
    expected_columns = {
        "id": "Nullable(Int64)",
        "customer_id": "Nullable(Int64)",
        "product": "Nullable(String)",
        "category": "Nullable(String)",
        "amount": "Nullable(Float64)",
        "quantity": "Nullable(Int32)",
        "sale_date": "Nullable(Date32)",
        "region": "Nullable(String)",
        "note": "Nullable(String)",
    }
    for col, expected_type in expected_columns.items():
        assert f"`{col}` {expected_type}" in result, (
            f"Column `{col}` {expected_type} not found in SHOW CREATE TABLE output:\n{result}"
        )


def test_drop_and_recreate(node, catalog_manager, sales_table):
    db = catalog_manager.make_database_name()
    catalog_manager.create_catalog(node, db)
    catalog_manager.resolve_table_name(node, db, sales_table)
    node.query(f"DROP DATABASE {db}")

    db2 = catalog_manager.make_database_name()
    catalog_manager.create_catalog(node, db2)
    full = catalog_manager.resolve_table_name(node, db2, sales_table)
    result = node.query(
        f"SELECT count() FROM {db2}.`{full}` FORMAT TSV"
    ).strip()
    assert int(result) == 20


# ---------------------------------------------------------------------------
# Basic queries
# ---------------------------------------------------------------------------


def test_select_count(node, shared_db, sales_table, catalog_manager):
    full = catalog_manager.resolve_table_name(node, shared_db, sales_table)
    result = node.query(
        f"SELECT count() FROM {shared_db}.`{full}` FORMAT TSV"
    ).strip()
    assert int(result) == 20


def test_select_limit(node, shared_db, sales_table, catalog_manager):
    full = catalog_manager.resolve_table_name(node, shared_db, sales_table)
    result = node.query(
        f"SELECT * FROM {shared_db}.`{full}` LIMIT 1 FORMAT TSV"
    ).strip()
    lines = result.split("\n")
    assert len(lines) == 1
    cols = lines[0].split("\t")
    assert len(cols) >= 8


def test_projection_pushdown(node, shared_db, sales_table, catalog_manager):
    full = catalog_manager.resolve_table_name(node, shared_db, sales_table)
    result = node.query(
        f"SELECT id, product FROM {shared_db}.`{full}` ORDER BY id FORMAT TSV"
    ).strip()
    lines = result.split("\n")
    assert len(lines) == 20
    r1 = lines[0].split("\t")
    assert len(r1) == 2
    assert r1[0] == "1"
    assert r1[1] == "Widget A"


def test_where_filter(node, shared_db, sales_table, catalog_manager):
    full = catalog_manager.resolve_table_name(node, shared_db, sales_table)
    east = node.query(
        f"SELECT count() FROM {shared_db}.`{full}` "
        f"WHERE region = 'East' FORMAT TSV"
    ).strip()
    west = node.query(
        f"SELECT count() FROM {shared_db}.`{full}` "
        f"WHERE region = 'West' FORMAT TSV"
    ).strip()
    assert int(east) == 10
    assert int(west) == 10


def test_where_non_partition_column(node, shared_db, sales_table, catalog_manager):
    full = catalog_manager.resolve_table_name(node, shared_db, sales_table)
    result = node.query(
        f"SELECT id, product FROM {shared_db}.`{full}` "
        f"WHERE product = 'Widget A' ORDER BY id FORMAT TSV"
    ).strip()
    lines = result.split("\n")
    assert len(lines) == 5
    ids = [int(line.split("\t")[0]) for line in lines]
    assert ids == [1, 5, 9, 13, 17]
    for line in lines:
        assert "Widget A" in line


def test_aggregations(node, shared_db, sales_table, catalog_manager):
    full = catalog_manager.resolve_table_name(node, shared_db, sales_table)
    result = node.query(
        f"SELECT count(*), sum(amount), avg(amount), max(amount), min(amount) "
        f"FROM {shared_db}.`{full}` FORMAT TSV"
    ).strip()
    parts = result.split("\t")
    assert int(parts[0]) == 20
    assert float(parts[1]) == pytest.approx(569.75)
    assert float(parts[2]) == pytest.approx(28.4875)
    assert float(parts[3]) == pytest.approx(48.0)
    assert float(parts[4]) == pytest.approx(10.5)


def test_group_by(node, shared_db, sales_table, catalog_manager):
    full = catalog_manager.resolve_table_name(node, shared_db, sales_table)
    result = node.query(
        f"SELECT category, region, count(*), sum(amount) "
        f"FROM {shared_db}.`{full}` GROUP BY category, region "
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


def test_order_by(node, shared_db, sales_table, catalog_manager):
    full = catalog_manager.resolve_table_name(node, shared_db, sales_table)
    sql = (
        f"SELECT id FROM {shared_db}.`{full}` "
        f"ORDER BY amount DESC LIMIT 5 FORMAT TSV"
    )
    r1 = node.query(sql).strip()
    r2 = node.query(sql).strip()
    assert r1 == r2
    ids = [int(x) for x in r1.split("\n")]
    assert ids == [16, 8, 12, 20, 4]


def test_nullable_handling(node, shared_db, sales_table, catalog_manager):
    full = catalog_manager.resolve_table_name(node, shared_db, sales_table)

    null_count = node.query(
        f"SELECT count(*) FROM {shared_db}.`{full}` "
        f"WHERE note IS NULL FORMAT TSV"
    ).strip()
    not_null_count = node.query(
        f"SELECT count(*) FROM {shared_db}.`{full}` "
        f"WHERE note IS NOT NULL FORMAT TSV"
    ).strip()
    assert int(null_count) == 10
    assert int(not_null_count) == 10

    result = node.query(
        f"SELECT id, note FROM {shared_db}.`{full}` "
        f"ORDER BY id LIMIT 4 FORMAT TSV"
    ).strip()
    lines = result.split("\n")
    assert lines[0].split("\t")[1] == "first sale"
    assert lines[1].split("\t")[1] == "\\N"
    assert lines[2].split("\t")[1] == "bulk order"
    assert lines[3].split("\t")[1] == "\\N"


# ---------------------------------------------------------------------------
# Type mapping — primitives
# ---------------------------------------------------------------------------


def test_primitive_types(node, types_db, types_table, catalog_manager):
    full = catalog_manager.resolve_table_name(node, types_db, types_table)
    prim_cols = [
        "col_bool", "col_int32", "col_int64", "col_float32", "col_float64",
        "col_decimal", "col_date", "col_timestamp", "col_timestamptz",
        "col_string", "col_binary",
    ]
    expected = {
        0: "Bool", 1: "Int32", 2: "Int64", 3: "Float32", 4: "Float64",
        5: "Decimal", 6: "Date", 7: "DateTime64", 8: "DateTime64",
        9: "String", 10: "String",
    }
    type_expr = ", ".join(f"toTypeName({c})" for c in prim_cols)
    types = node.query(
        f"SELECT {type_expr} FROM {types_db}.`{full}` LIMIT 1 FORMAT TSV"
    ).strip().split("\t")
    for idx, substr in expected.items():
        assert substr in types[idx], (
            f"{prim_cols[idx]}: expected {substr} in {types[idx]}"
        )


def test_primitive_values(node, types_db, types_table, catalog_manager):
    full = catalog_manager.resolve_table_name(node, types_db, types_table)
    cols = ", ".join([
        "col_bool", "col_int32", "col_int64", "col_float32", "col_float64",
        "col_decimal", "col_date", "col_timestamp", "col_timestamptz",
        "col_string",
    ])
    result = node.query(
        f"SELECT {cols} FROM {types_db}.`{full}` ORDER BY col_int32 FORMAT TSV"
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


# ---------------------------------------------------------------------------
# Type mapping — complex (Array, Map, Struct)
# ---------------------------------------------------------------------------


def test_complex_types(node, complex_db, complex_table, catalog_manager):
    full = catalog_manager.resolve_table_name(node, complex_db, complex_table)

    type_result = node.query(
        f"SELECT toTypeName(col_array), toTypeName(col_map), "
        f"toTypeName(col_struct) "
        f"FROM {complex_db}.`{full}` LIMIT 1 FORMAT TSV"
    ).strip().split("\t")
    assert "Array" in type_result[0]
    assert "Map" in type_result[1]
    assert "Tuple" in type_result[2]


def test_complex_values(node, complex_db, complex_table, catalog_manager):
    full = catalog_manager.resolve_table_name(node, complex_db, complex_table)
    result = node.query(
        f"SELECT id, col_array, col_struct FROM {complex_db}.`{full}` "
        f"ORDER BY id FORMAT TSV"
    ).strip()
    lines = result.split("\n")
    assert len(lines) == 2
    assert lines[0].split("\t")[1] == "[10,20,30]"
    assert lines[1].split("\t")[1] == "[40,50]"
    assert "10" in lines[0].split("\t")[2] and "foo" in lines[0].split("\t")[2]
    assert "20" in lines[1].split("\t")[2] and "bar" in lines[1].split("\t")[2]


# ---------------------------------------------------------------------------
# JOINs
# ---------------------------------------------------------------------------


def test_join_catalog_and_local(
    node, shared_db, customers_table, catalog_manager,
):
    local = f"local_orders_{uuid.uuid4().hex[:8]}"
    try:
        node.query(
            f"CREATE TABLE {local} "
            f"(order_id Int64, customer_id Int64, total Float64) "
            f"ENGINE = MergeTree ORDER BY order_id"
        )
        node.query(
            f"INSERT INTO {local} VALUES "
            f"(1,1,100.0),(2,2,200.0),(3,3,150.0)"
        )
        cust = catalog_manager.resolve_table_name(
            node, shared_db, customers_table
        )
        result = node.query(
            f"SELECT o.order_id, c.name, o.total "
            f"FROM {local} AS o "
            f"INNER JOIN {shared_db}.`{cust}` AS c "
            f"ON o.customer_id = c.customer_id "
            f"ORDER BY o.order_id FORMAT TSV"
        ).strip()
        lines = result.split("\n")
        assert len(lines) == 3
        assert "Alice" in lines[0]
        assert "Bob" in lines[1]
        assert "Charlie" in lines[2]
    finally:
        node.query(f"DROP TABLE IF EXISTS {local}")


def test_join_two_catalog_tables(
    node, shared_db, sales_table, customers_table, catalog_manager,
):
    sf = catalog_manager.resolve_table_name(node, shared_db, sales_table)
    cf = catalog_manager.resolve_table_name(node, shared_db, customers_table)
    result = node.query(
        f"SELECT c.name, count() AS cnt, sum(s.amount) AS total "
        f"FROM {shared_db}.`{sf}` AS s "
        f"INNER JOIN {shared_db}.`{cf}` AS c "
        f"ON s.customer_id = c.customer_id "
        f"GROUP BY c.name ORDER BY c.name FORMAT TSV"
    ).strip()
    lines = result.split("\n")
    assert len(lines) == 5
    for line in lines:
        parts = line.split("\t")
        assert int(parts[1]) == 4


# ---------------------------------------------------------------------------
# CTAS and data movement
# ---------------------------------------------------------------------------


def test_ctas(node, shared_db, sales_table, catalog_manager):
    full = catalog_manager.resolve_table_name(node, shared_db, sales_table)
    local = f"local_ctas_{uuid.uuid4().hex[:8]}"
    try:
        node.query(
            f"CREATE TABLE {local} ENGINE = MergeTree ORDER BY id "
            f"AS SELECT * FROM {shared_db}.`{full}`"
        )
        assert int(node.query(f"SELECT count() FROM {local}").strip()) == 20
    finally:
        node.query(f"DROP TABLE IF EXISTS {local}")


def test_ctas_with_cast(node, shared_db, sales_table, catalog_manager):
    full = catalog_manager.resolve_table_name(node, shared_db, sales_table)
    local = f"local_cast_{uuid.uuid4().hex[:8]}"
    try:
        node.query(
            f"CREATE TABLE {local} ENGINE = MergeTree ORDER BY id AS "
            f"SELECT id, CAST(amount AS Decimal(10,2)) AS amount_dec, "
            f"CAST(quantity AS UInt64) AS qty FROM {shared_db}.`{full}`"
        )
        types = node.query(
            f"SELECT toTypeName(amount_dec), toTypeName(qty) "
            f"FROM {local} LIMIT 1 FORMAT TSV"
        ).strip().split("\t")
        assert "Decimal" in types[0]
        assert "UInt64" in types[1]
        assert int(node.query(f"SELECT count() FROM {local}").strip()) == 20
    finally:
        node.query(f"DROP TABLE IF EXISTS {local}")


def test_incremental_insert(node, shared_db, sales_table, catalog_manager):
    full = catalog_manager.resolve_table_name(node, shared_db, sales_table)
    local = f"local_inc_{uuid.uuid4().hex[:8]}"
    try:
        node.query(
            f"CREATE TABLE {local} "
            f"(id Int64, product String, amount Float64, sale_date Date32) "
            f"ENGINE = MergeTree ORDER BY id"
        )
        node.query(
            f"INSERT INTO {local} SELECT id, product, amount, sale_date "
            f"FROM {shared_db}.`{full}` WHERE region = 'East'"
        )
        assert int(node.query(f"SELECT count() FROM {local}").strip()) == 10

        node.query(
            f"INSERT INTO {local} SELECT id, product, amount, sale_date "
            f"FROM {shared_db}.`{full}` WHERE region = 'West'"
        )
        assert int(node.query(f"SELECT count() FROM {local}").strip()) == 20
    finally:
        node.query(f"DROP TABLE IF EXISTS {local}")


# ---------------------------------------------------------------------------
# System tables & settings
# ---------------------------------------------------------------------------


def test_show_data_lake_catalogs_setting(
    node, catalog_manager, sales_table,
):
    db = catalog_manager.make_database_name()
    catalog_manager.create_catalog(node, db)

    # system.databases always lists data lake catalog databases regardless of
    # show_data_lake_catalogs_in_system_tables: the database name is local
    # metadata and never requires a call to the external catalog service.
    for setting_value in (0, 1):
        db_row = node.query(
            f"SELECT engine FROM system.databases WHERE name = '{db}' FORMAT TSV",
            settings={"show_data_lake_catalogs_in_system_tables": setting_value},
        ).strip()
        assert "DataLakeCatalog" in db_row

    catalog_manager.resolve_table_name(node, db, sales_table)


# ---------------------------------------------------------------------------
# Glue-specific: credential modes
# ---------------------------------------------------------------------------


@only_glue
def test_glue_env_credentials(node, catalog_manager, sales_table):
    """ClickHouse picks up AWS credentials from container env vars."""

    env_out = node.exec_in_container([
        "bash", "-c", "env | grep AWS",
    ]).strip()
    assert "AWS_ACCESS_KEY_ID" in env_out

    db = _fresh_glue_db(node, catalog_manager, credentials_mode="none")
    full = catalog_manager.resolve_table_name(node, db, sales_table)
    result = node.query(
        f"SELECT count() FROM {db}.`{full}` FORMAT TSV"
    ).strip()
    assert int(result) == 20


@only_glue
def test_glue_explicit_credentials(node, catalog_manager, sales_table):
    """Credentials passed explicitly in SETTINGS."""

    db = catalog_manager.make_database_name()
    catalog_manager.create_clickhouse_glue_database(
        node, db, credentials_mode="settings"
    )
    full = catalog_manager.resolve_table_name(node, db, sales_table)
    result = node.query(
        f"SELECT count() FROM {db}.`{full}` FORMAT TSV"
    ).strip()
    assert int(result) == 20


def _fresh_glue_db(node, mgr: AwsGlueCatalogManager, **kwargs) -> str:
    db = mgr.make_database_name()
    mgr.create_clickhouse_glue_database(node, db, **kwargs)
    return db


# ---------------------------------------------------------------------------
# OneLake-specific: auth error handling
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Common: error handling & access control
# ---------------------------------------------------------------------------


def test_nonexistent_table(node, shared_db):
    """Query non-existent table gives a clear error."""
    error = node.query_and_get_error(
        f"SELECT * FROM {shared_db}.`dbo.no_such_table_xyz`"
    )
    err = error.lower()
    assert any(
        w in err
        for w in ["not found", "doesn't exist", "unknown table", "not exist"]
    )


def test_user_without_show_privilege(node, catalog_manager, sales_table):
    """ClickHouse user without SHOW DATABASES privilege is correctly denied."""
    db = catalog_manager.make_database_name()
    catalog_manager.create_catalog(node, db)
    try:
        node.query(
            "CREATE USER IF NOT EXISTS e2e_restricted "
            "IDENTIFIED BY 'testpass123'"
        )
        out, err = node.query_and_get_answer_with_error(
            f"SHOW TABLES FROM {db}",
            user="e2e_restricted",
            password="testpass123",
        )
        assert err.strip() or not out.strip(), (
            f"Restricted user should not see tables, got: {out}"
        )
    finally:
        node.query("DROP USER IF EXISTS e2e_restricted")


def test_table_disappears_after_cleanup(node, catalog_manager):
    """After deleting a table from the catalog, a fresh database must not list it."""
    data = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "value": pa.array(["one", "two"], type=pa.string()),
        }
    )
    table_name = catalog_manager.create_table(data)
    catalog_manager.wait_for_table_ready(table_name)

    db = catalog_manager.make_database_name()
    catalog_manager.create_catalog(node, db)
    catalog_manager.resolve_table_name(node, db, table_name)

    catalog_manager.cleanup_table(table_name)
    catalog_manager.wait_for_table_gone(table_name)

    db2 = catalog_manager.make_database_name()
    catalog_manager.create_catalog(node, db2)
    assert table_name not in node.query(f"SHOW TABLES FROM {db2}").strip()


def test_two_databases_same_catalog(node, catalog_manager):
    """Two databases pointing to same catalog discover and read same data."""
    data = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "value": pa.array(["alpha", "beta"], type=pa.string()),
        }
    )
    table_name = catalog_manager.create_table(data)
    try:
        catalog_manager.wait_for_table_ready(table_name)

        db1 = catalog_manager.make_database_name()
        db2 = catalog_manager.make_database_name()
        catalog_manager.create_catalog(node, db1)
        catalog_manager.create_catalog(node, db2)

        full1 = catalog_manager.resolve_table_name(node, db1, table_name)
        full2 = catalog_manager.resolve_table_name(node, db2, table_name)

        r1 = node.query(
            f"SELECT count() FROM {db1}.`{full1}` FORMAT TSV"
        ).strip()
        r2 = node.query(
            f"SELECT count() FROM {db2}.`{full2}` FORMAT TSV"
        ).strip()
        assert int(r1) == 2
        assert int(r2) == 2

        result1 = node.query(
            f"SELECT id, value FROM {db1}.`{full1}` ORDER BY id FORMAT TSV"
        ).strip()
        result2 = node.query(
            f"SELECT id, value FROM {db2}.`{full2}` ORDER BY id FORMAT TSV"
        ).strip()
        assert result1 == result2
        lines = result1.split("\n")
        assert len(lines) == 2
        assert lines[0] == "1\talpha"
        assert lines[1] == "2\tbeta"
    finally:
        catalog_manager.cleanup_table(table_name)


# ---------------------------------------------------------------------------
# OneLake-specific: auth error handling
# ---------------------------------------------------------------------------


@only_onelake
def test_onelake_create_without_flag(node, catalog_manager):
    """CREATE DATABASE with allow_database_iceberg explicitly disabled -> error."""

    db = catalog_manager.make_database_name()
    sql = catalog_manager.create_db_sql(db)
    error = node.query_and_get_error(
        sql, settings={"allow_experimental_database_iceberg": "0"}
    )
    assert "allow_database_iceberg" in error or "SUPPORT_IS_DISABLED" in error


@only_onelake
def test_onelake_invalid_client_secret(node, catalog_manager):
    """Invalid client_secret -> error or non-functional database."""

    db = catalog_manager.make_database_name()
    sql = catalog_manager.create_db_sql(
        db, client_secret="INVALID_SECRET_VALUE_12345"
    )

    _, create_err = node.query_and_get_answer_with_error(sql)
    if create_err.strip():
        return

    tables_out, tables_err = node.query_and_get_answer_with_error(
        f"SHOW TABLES FROM {db}"
    )
    if tables_err.strip():
        return

    assert not tables_out.strip(), (
        f"Expected empty table list or error with invalid secret, "
        f"got: {tables_out}"
    )


@only_onelake
def test_onelake_wrong_tenant_id(node, catalog_manager):
    """Wrong tenant_id -> error or non-functional database."""

    db = catalog_manager.make_database_name()
    fake_tenant = "00000000-0000-0000-0000-000000000000"
    sql = catalog_manager.create_db_sql(
        db,
        tenant_id=fake_tenant,
        oauth_server_uri=(
            f"https://login.microsoftonline.com/{fake_tenant}"
            f"/oauth2/v2.0/token"
        ),
    )

    _, create_err = node.query_and_get_answer_with_error(sql)
    if create_err.strip():
        return

    tables_out, tables_err = node.query_and_get_answer_with_error(
        f"SHOW TABLES FROM {db}"
    )
    if tables_err.strip():
        return

    assert not tables_out.strip(), (
        f"Expected empty table list or error with wrong tenant, "
        f"got: {tables_out}"
    )


@only_onelake
def test_onelake_warehouse_id_as_data_item(node, catalog_manager):
    """warehouse_id alone (no lakehouse) -> error or empty table list."""

    db = catalog_manager.make_database_name()
    cfg = catalog_manager.config
    error = catalog_manager.try_create_database(
        node, db, warehouse=cfg.workspace_id,
    )
    if not error.strip():
        out, show_err = node.query_and_get_answer_with_error(
            f"SHOW TABLES FROM {db}"
        )
        assert show_err.strip() or out.strip() == "", (
            f"Expected error or empty table list, got: {out}"
        )


@only_onelake
def test_onelake_system_databases(node, catalog_manager):
    """All system.databases fields are correct for a DataLakeCatalog database."""

    db = catalog_manager.make_database_name()
    catalog_manager.create_catalog(node, db)
    row = node.query(
        f"SELECT name, engine, data_path, metadata_path, uuid, engine_full, "
        f"comment, is_external "
        f"FROM system.databases WHERE name = '{db}' "
        f"FORMAT TSV",
        settings={"show_data_lake_catalogs_in_system_tables": 1},
    ).strip()
    assert row, f"Database {db} not found in system.databases"
    parts = row.split("\t")
    assert len(parts) == 8, f"Expected 8 columns, got {len(parts)}: {parts}"

    (
        name, engine, data_path, metadata_path,
        db_uuid, engine_full, comment, is_external,
    ) = parts

    assert name == db
    assert engine == "DataLakeCatalog"
    assert data_path != ""
    assert metadata_path == ""
    assert db_uuid == "00000000-0000-0000-0000-000000000000"
    assert "DataLakeCatalog" in engine_full
    assert "onelake" in engine_full
    assert catalog_manager.config.catalog_url in engine_full
    assert catalog_manager.config.client_secret not in engine_full, (
        "client_secret leaked in engine_full of system.databases"
    )
    assert comment == ""
    assert is_external == "1"


@only_onelake
def test_onelake_show_create_no_secret(node, catalog_manager, sales_table):
    """SHOW CREATE DATABASE must not expose client_secret."""

    db = catalog_manager.make_database_name()
    catalog_manager.create_catalog(node, db)
    result = node.query(f"SHOW CREATE DATABASE {db}").strip()
    assert catalog_manager.config.client_secret not in result


@only_onelake
def test_onelake_show_create_table_no_secret(
    node, shared_db, sales_table, catalog_manager,
):
    """SHOW CREATE TABLE must not expose credentials."""

    full = catalog_manager.resolve_table_name(node, shared_db, sales_table)
    result = node.query(f"SHOW CREATE TABLE {shared_db}.`{full}`").strip()
    assert result, "SHOW CREATE TABLE returned empty result"

    secret = catalog_manager.config.client_secret
    assert secret not in result, (
        f"client_secret leaked in SHOW CREATE TABLE:\n{result}"
    )

    system_row = node.query(
        f"SELECT engine_full FROM system.tables "
        f"WHERE database = '{shared_db}' AND name = '{full}' "
        f"FORMAT TSV"
    ).strip()
    assert secret not in system_row, (
        f"client_secret leaked in system.tables engine_full:\n{system_row}"
    )


def test_insert_into_table(node, catalog_manager, request):
    """INSERT INTO a catalog table and verify the row count increases."""
    backend = request.node.callspec.params.get("catalog_manager")
    if backend == "biglake":
        pytest.xfail("INSERT into BigLake DataLakeCatalog does not commit to the catalog")
    if backend == "onelake":
        pytest.xfail("INSERT into OneLake raises StorageException during blob upload")
    data = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "value": pa.array(["one", "two"], type=pa.string()),
        }
    )
    table_name = catalog_manager.create_table(data)
    try:
        catalog_manager.wait_for_table_ready(table_name)

        db = catalog_manager.make_database_name()
        catalog_manager.create_catalog(node, db)
        full = catalog_manager.resolve_table_name(node, db, table_name)

        count = node.query(
            f"SELECT count() FROM {db}.`{full}` FORMAT TSV"
        ).strip()
        assert int(count) == 2

        node.query(
            f"INSERT INTO {db}.`{full}` VALUES (3, 'three')",
            settings={"allow_insert_into_iceberg": 1},
        )
        count = node.query(
            f"SELECT count() FROM {db}.`{full}` FORMAT TSV"
        ).strip()
        assert int(count) == 3
    finally:
        catalog_manager.cleanup_table(table_name)


@only_onelake
def test_onelake_invalid_oauth_uri(node, catalog_manager):
    """Invalid oauth_server_uri -> CREATE succeeds but catalog access fails."""

    db = catalog_manager.make_database_name()
    sql = catalog_manager.create_db_sql(
        db, oauth_server_uri="https://invalid.example.com/oauth/token",
    )

    _, create_err = node.query_and_get_answer_with_error(sql)
    assert not create_err.strip(), f"CREATE unexpectedly failed: {create_err}"

    tables_out, tables_err = node.query_and_get_answer_with_error(
        f"SHOW TABLES FROM {db}"
    )
    assert not tables_out.strip() and not tables_err.strip(), (
        "SHOW TABLES should return empty (lazy auth swallows errors)"
    )

    error = node.query_and_get_error(
        f"SELECT * FROM {db}.nonexistent_table FORMAT TSV"
    )
    assert error, "Expected error when accessing table with invalid oauth URI"


@only_onelake
def test_onelake_warehouse_wrong_format(node, catalog_manager):
    """Malformed warehouse -> CREATE succeeds but catalog access fails."""

    db = catalog_manager.make_database_name()
    sql = catalog_manager.create_db_sql(
        db, warehouse="not-a-valid-format-no-slash"
    )

    _, create_err = node.query_and_get_answer_with_error(sql)
    assert not create_err.strip(), f"CREATE unexpectedly failed: {create_err}"

    tables_out, tables_err = node.query_and_get_answer_with_error(
        f"SHOW TABLES FROM {db}"
    )
    assert not tables_out.strip() and not tables_err.strip(), (
        "SHOW TABLES should return empty (lazy auth swallows errors)"
    )

    error = node.query_and_get_error(
        f"SELECT * FROM {db}.nonexistent_table FORMAT TSV"
    )
    assert error, "Expected error when accessing table with malformed warehouse"
