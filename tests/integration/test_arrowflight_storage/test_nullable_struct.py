import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", with_arrowflight=True)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize("source_kind", ["function", "engine", "as_function"])
@pytest.mark.parametrize(
    "nullable_tuple_setting", [None, 1, 0], ids=["default", "enabled", "disabled"]
)
def test_nullable_struct_schema_inference(source_kind, nullable_tuple_setting):
    settings = {"print_pretty_type_names": 0}
    if nullable_tuple_setting is not None:
        settings["enable_nullable_tuple_type"] = nullable_tuple_setting
    source = "arrowFlight('arrowflight1:5005', 'STRUCTS')"
    if source_kind != "function":
        table = f"nullable_struct_{source_kind}"
        clause = (
            "ENGINE = ArrowFlight('arrowflight1:5005', 'STRUCTS')"
            if source_kind == "engine"
            else f"AS {source}"
        )
        node.query(f"CREATE TABLE {table} {clause}", settings=settings)
        source = table

    try:
        nullable_tuple_enabled = nullable_tuple_setting != 0
        record_type = (
            "Nullable(Tuple(x Nullable(Int64)))"
            if nullable_tuple_enabled
            else "Tuple(x Nullable(Int64))"
        )
        nested_type = (
            "Tuple(child Nullable(Tuple(x Int64)))"
            if nullable_tuple_enabled
            else "Tuple(child Tuple(x Int64))"
        )
        assert node.query(
            f"SELECT toTypeName(record), toTypeName(required_record), toTypeName(nested_record) "
            f"FROM {source} LIMIT 1",
            settings=settings,
        ) == TSV([[record_type, "Tuple(x Int64)", nested_type]])

        # Nullable tuples distinguish an absent record from a present record with a `NULL` child.
        assert node.query(
            f"SELECT id, isNull(record), tupleElement(record, 'x'), "
            f"tupleElement(required_record, 'x'), isNull(tupleElement(nested_record, 'child')), "
            f"tupleElement(tupleElement(nested_record, 'child'), 'x') "
            f"FROM {source} ORDER BY id",
            settings=settings,
        ) == TSV(
            [
                [
                    0,
                    int(nullable_tuple_enabled),
                    "\\N" if nullable_tuple_enabled else 0,
                    10,
                    int(nullable_tuple_enabled),
                    "\\N" if nullable_tuple_enabled else 0,
                ],
                [1, 0, "\\N", 11, 0, 0],
                [2, 0, 7, 12, 0, 8],
            ]
        )
    finally:
        if source_kind != "function":
            node.query(f"DROP TABLE {table} SYNC")
