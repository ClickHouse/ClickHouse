from helpers.cluster import ClickHouseCluster


def check_flags_deleted(node, database_name, tables):
    for table in tables:
        assert "convert_to_replicated" not in node.exec_in_container(
            [
                "bash",
                "-c",
                f"ls /var/lib/clickhouse/data/{database_name}/{table}/",
            ]
        )


def set_convert_flags(node, database_name, tables):
    for table in tables:
        node.exec_in_container(
            [
                "bash",
                "-c",
                f"touch /var/lib/clickhouse/data/{database_name}/{table}/convert_to_replicated",
            ]
        )
