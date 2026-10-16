

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


FAILPOINT_CONFIG = "/etc/clickhouse-server/config.d/convert_failpoint.xml"


def pause_at(node, failpoint):
    # Enabled from the config, so that the conversion run at startup pauses.
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"echo '<clickhouse><fail_points_active><{failpoint}>1</{failpoint}></fail_points_active></clickhouse>' > {FAILPOINT_CONFIG}",
        ]
    )
    node.restart_clickhouse()
    node.query(f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE")


def clear_failpoint(node):
    node.exec_in_container(["rm", "-f", FAILPOINT_CONFIG])


def kill_paused(node):
    node.stop_clickhouse(kill=True)
    clear_failpoint(node)


def prepare_versioned_table(node, zk, database_name):
    """Creates the MergeTree `mt` with two parts of metadata version 2 and the convert flag. Returns its UUID."""
    node.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")
    node.query(f"CREATE DATABASE {database_name}")

    def q(query):
        return node.query(database=database_name, sql=query)

    q(
        f"CREATE TABLE mt ( A Int64, D Date ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database_name}_src/{{shard}}', '{{replica}}') PARTITION BY toYYYYMM(D) ORDER BY A"
    )
    q("ALTER TABLE mt ADD COLUMN B Int64")
    q("ALTER TABLE mt DROP COLUMN B")
    q("INSERT INTO mt SELECT number, today() FROM numbers(100)")
    q("INSERT INTO mt SELECT number, today() - 60 FROM numbers(100)")
    q("DETACH TABLE mt")
    q("ATTACH TABLE mt AS NOT REPLICATED")
    zk.delete(f"/clickhouse/tables/{database_name}_src", recursive=True)

    set_convert_flags(node, database_name, ["mt"])
    return node.query(
        f"SELECT uuid FROM system.tables WHERE database = '{database_name}' AND name = 'mt'"
    ).strip()
