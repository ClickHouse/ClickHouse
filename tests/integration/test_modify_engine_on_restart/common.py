from helpers.cluster import ClickHouseCluster


def get_table_path(node, table, database):
    return (
        node.query(
            sql=f"SELECT data_paths FROM system.tables WHERE table = '{table}' and database = '{database}' LIMIT 1"
        )
        .split(",")[0]
        .strip("'[]\n")
    )


def check_flags_deleted(node, database_name, tables):
    for table in tables:
        assert "convert_to_replicated" not in node.exec_in_container(
            [
                "bash",
                "-c",
                f"ls {get_table_path(node, table, database_name)}",
            ]
        )


def set_convert_flags(node, database_name, tables):
    for table in tables:
        node.exec_in_container(
            [
                "bash",
                "-c",
                f"touch {get_table_path(node, table, database_name)}convert_to_replicated",
            ]
        )
