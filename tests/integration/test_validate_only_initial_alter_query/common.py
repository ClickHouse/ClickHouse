from helpers.cluster import ClickHouseInstance

# Used also in private, so when changing this please verify that it still works in private
def run_test(node1: ClickHouseInstance, node2: ClickHouseInstance, database_engine_spec: str, table_engine: str):
    database_name = "test_validate_only_initial_query"
    nodes = [node1, node2]
    create_table_query = f"""
        SET cast_ipv4_ipv6_default_on_conversion_error=1;

        CREATE TABLE {database_name}.test
        (
            value IPv4 DEFAULT '',
        ) ENGINE={table_engine} ORDER BY tuple();
    """

    for node in nodes:
        node.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")

    create_database_query = f"CREATE DATABASE {database_name} ENGINE = {database_engine_spec.format(database_name=database_name)};"

    node1.query(create_database_query)
    if not database_engine_spec.startswith("Shared"):
        node2.query(create_database_query)

    node1.query(create_table_query)

    expected_format = "{column_name}\tIPv4\tDEFAULT\t\\'\\'\n"
    expected_single_column = expected_format.format(column_name="value")
    for node in nodes:
        result = node.query(f"SELECT name, type, default_kind, default_expression FROM system.columns WHERE database = '{database_name}' AND table = 'test'")
        assert result == expected_single_column, f"Table is not altered correctly on node {node.name}"

    # We cannot use a setting that is validated inside `parseColumnsListForTableFunction`, because that would make the query fail on node2
    # and it is not connected to the function `validateCreateQuery`
    node1.query(f"""
        SET cast_ipv4_ipv6_default_on_conversion_error=1;
        ALTER TABLE {database_name}.test ADD COLUMN value2 IPv4 DEFAULT '' SETTINGS alter_sync=0;
    """,
        timeout=10)

    node1.query(f"""
        SET cast_ipv4_ipv6_default_on_conversion_error=1;
        ALTER TABLE {database_name}.test ADD COLUMN value3 IPv4 DEFAULT '' SETTINGS alter_sync=2;
    """,
        timeout=10)

    expected_final = expected_single_column + expected_format.format(column_name="value2") + expected_format.format(column_name="value3")
    for node in nodes:
        result = node.query(f"SELECT name, type, default_kind, default_expression FROM system.columns WHERE database = '{database_name}' AND table = 'test' ORDER BY name")
        assert result == expected_final, f"Table is not altered correctly on node {node.name}"
