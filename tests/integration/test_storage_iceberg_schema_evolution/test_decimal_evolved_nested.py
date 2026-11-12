import pytest

from helpers.iceberg_utils import (
    default_upload_directory,
    get_uuid_str,
    get_creation_expression,
    check_schema_and_data
)


@pytest.mark.parametrize("format_version", ["2"])
@pytest.mark.parametrize("storage_type", ["local"])
@pytest.mark.parametrize("is_table_function", [False])
def test_decimal_evolved_nested(
    started_cluster_iceberg_schema_evolution, format_version, storage_type, is_table_function
):
    instance = started_cluster_iceberg_schema_evolution.instances["node1"]
    spark = started_cluster_iceberg_schema_evolution.spark_session
    TABLE_NAME = (
        "test_decimal_evolved_nested_"
        + format_version
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    def execute_spark_query(query: str):
        spark.sql(query)
        default_upload_directory(
            started_cluster_iceberg_schema_evolution,
            storage_type,
            f"/iceberg_data/default/{TABLE_NAME}/",
            f"/iceberg_data/default/{TABLE_NAME}/",
        )
        return

    execute_spark_query(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    execute_spark_query(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INT,
            a ARRAY<DECIMAL(9, 2)>,
            b MAP<INT, DECIMAL(12, 4)>,
            c STRUCT<x: DECIMAL(12, 4), y: INT>,
            d ARRAY<STRUCT<z: DECIMAL(9, 2)>>
        )
        USING iceberg
        OPTIONS ('format-version'='2')
    """)

    execute_spark_query(f"""
        INSERT INTO {TABLE_NAME} VALUES (
            1,
            array(CAST(1.25 AS DECIMAL(9, 2)), CAST(2.50 AS DECIMAL(9, 2))),
            map(1, CAST(10.1234 AS DECIMAL(12, 4))),
            named_struct('x', CAST(3.1415 AS DECIMAL(12, 4)), 'y', 7),
            array(named_struct('z', CAST(4.75 AS DECIMAL(9, 2))))
        )
    """)

    table_creation_expression = get_creation_expression(
        storage_type,
        TABLE_NAME,
        started_cluster_iceberg_schema_evolution,
        table_function=is_table_function,
    )

    table_select_expression = (
        TABLE_NAME if not is_table_function else table_creation_expression
    )

    if not is_table_function:
        instance.query(table_creation_expression)

    # Each promotion changes the ClickHouse storage type: Decimal32 -> Decimal64 or Decimal64 -> Decimal128.
    execute_spark_query(f"ALTER TABLE {TABLE_NAME} ALTER COLUMN a.element TYPE DECIMAL(18, 2)")
    execute_spark_query(f"ALTER TABLE {TABLE_NAME} ALTER COLUMN b.value TYPE DECIMAL(20, 4)")
    execute_spark_query(f"ALTER TABLE {TABLE_NAME} ALTER COLUMN c.x TYPE DECIMAL(38, 4)")
    execute_spark_query(f"ALTER TABLE {TABLE_NAME} ALTER COLUMN d.element.z TYPE DECIMAL(10, 2)")

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ['id', 'Nullable(Int32)'],
            ['a', 'Array(Nullable(Decimal(18, 2)))'],
            ['b', 'Map(Int32, Nullable(Decimal(20, 4)))'],
            ['c', 'Tuple(\\n    x Nullable(Decimal(38, 4)),\\n    y Nullable(Int32))'],
            ['d', 'Array(Tuple(\\n    z Nullable(Decimal(10, 2))))']
        ],
        [
            ['1', '[1.25,2.5]', '{1:10.1234}', '(3.1415,7)', '[(4.75)]']
        ],
    )

    # The promoted fields are matched by id, also after a rename, a reorder and an added sibling field.
    execute_spark_query(f"ALTER TABLE {TABLE_NAME} RENAME COLUMN c.x TO w")
    execute_spark_query(f"ALTER TABLE {TABLE_NAME} ALTER COLUMN c.y FIRST")
    execute_spark_query(f"ALTER TABLE {TABLE_NAME} ADD COLUMN d.element.n INT")

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ['id', 'Nullable(Int32)'],
            ['a', 'Array(Nullable(Decimal(18, 2)))'],
            ['b', 'Map(Int32, Nullable(Decimal(20, 4)))'],
            ['c', 'Tuple(\\n    y Nullable(Int32),\\n    w Nullable(Decimal(38, 4)))'],
            ['d', 'Array(Tuple(\\n    z Nullable(Decimal(10, 2)),\\n    n Nullable(Int32)))']
        ],
        [
            ['1', '[1.25,2.5]', '{1:10.1234}', '(7,3.1415)', '[(4.75,NULL)]']
        ],
    )

    # A row written with the new schema, with values that do not fit the old precisions.
    execute_spark_query(f"""
        INSERT INTO {TABLE_NAME} VALUES (
            2,
            array(CAST(12345678.91 AS DECIMAL(18, 2))),
            map(2, CAST(1234567890123.4567 AS DECIMAL(20, 4))),
            named_struct('y', 8, 'w', CAST(12345678901234567890.1234 AS DECIMAL(38, 4))),
            array(named_struct('z', CAST(12345678.91 AS DECIMAL(10, 2)), 'n', 5))
        )
    """)

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ['id', 'Nullable(Int32)'],
            ['a', 'Array(Nullable(Decimal(18, 2)))'],
            ['b', 'Map(Int32, Nullable(Decimal(20, 4)))'],
            ['c', 'Tuple(\\n    y Nullable(Int32),\\n    w Nullable(Decimal(38, 4)))'],
            ['d', 'Array(Tuple(\\n    z Nullable(Decimal(10, 2)),\\n    n Nullable(Int32)))']
        ],
        [
            ['1', '[1.25,2.5]', '{1:10.1234}', '(7,3.1415)', '[(4.75,NULL)]'],
            ['2', '[12345678.91]', '{2:1234567890123.4567}', '(8,12345678901234567890.1234)', '[(12345678.91,5)]']
        ],
    )
