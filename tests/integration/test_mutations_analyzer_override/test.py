#!/usr/bin/env python3

"""
Verifies the server-side `<use_analyzer_for_mutations>` config option.

`MutationsInterpreter` emits `Will use old analyzer to prepare mutation` at
`test` level only when the old path is taken. The integration test captures
that signal from `system.text_log`:
  * with the override set to `true`, the message must not appear;
  * with the override set to `false`, the message must appear.
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/use_analyzer_for_mutations.xml",
        "configs/text_log.xml",
    ],
    stay_alive=True,
)

OLD_ANALYZER_LOG_MESSAGE = "Will use old analyzer to prepare mutation"
CONFIG_PATH = "/etc/clickhouse-server/config.d/use_analyzer_for_mutations.xml"


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def run_mutation(table_name: str) -> None:
    node.query(f"DROP TABLE IF EXISTS {table_name} SYNC")
    node.query(
        f"CREATE TABLE {table_name} (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id"
    )
    node.query(f"INSERT INTO {table_name} VALUES (1, 10), (2, 20), (3, 30)")
    node.query(
        f"ALTER TABLE {table_name} UPDATE v = v + 1 WHERE id = 2 SETTINGS mutations_sync = 2"
    )


def count_old_analyzer_log_lines(table_name: str) -> int:
    node.query("SYSTEM FLUSH LOGS")
    result = node.query(
        f"""
        SELECT count()
        FROM system.text_log
        WHERE logger_name = 'MutationsInterpreter(default.{table_name})'
          AND message = '{OLD_ANALYZER_LOG_MESSAGE}'
        """
    ).strip()
    return int(result)


def set_override(value: str) -> None:
    """Rewrite the override config to `<use_analyzer_for_mutations>{value}</...>` and reload."""
    node.exec_in_container(
        [
            "bash",
            "-c",
            (
                "cat > {path} <<'EOF'\n"
                "<clickhouse>\n"
                "    <use_analyzer_for_mutations>{value}</use_analyzer_for_mutations>\n"
                "</clickhouse>\n"
                "EOF"
            ).format(path=CONFIG_PATH, value=value),
        ]
    )
    node.query("SYSTEM RELOAD CONFIG")


def test_override_forces_new_analyzer(start_cluster):
    set_override("true")
    table = "t_override_true"
    run_mutation(table)
    assert count_old_analyzer_log_lines(table) == 0


def test_override_forces_old_analyzer(start_cluster):
    set_override("false")
    try:
        table = "t_override_false"
        run_mutation(table)
        assert count_old_analyzer_log_lines(table) >= 1
    finally:
        set_override("true")


def test_queued_statistics_mutation_drains(start_cluster):
    """A MATERIALIZE STATISTICS queued while the column was still physical must drain after the
    column becomes an alias. Only the old mutation analyzer reaches the readonly stage that
    resolves the name, so this needs the server-level override."""
    set_override("false")
    try:
        table = "t_queued_statistics"
        node.query(f"DROP TABLE IF EXISTS {table} SYNC")
        node.query(
            f"""
            CREATE TABLE {table} (a UInt64 STATISTICS(tdigest), b UInt64 STATISTICS(tdigest))
            ENGINE = MergeTree ORDER BY tuple()
            SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0
            """
        )
        node.query(f"INSERT INTO {table} VALUES (1, 1)")
        node.query(f"SYSTEM STOP MERGES {table}")
        node.query(
            f"ALTER TABLE {table} MATERIALIZE STATISTICS b SETTINGS mutations_sync = 0"
        )
        node.query(
            f"ALTER TABLE {table} MODIFY COLUMN b UInt64 ALIAS a + 1 STATISTICS(tdigest) SETTINGS mutations_sync = 0"
        )
        node.query(f"SYSTEM START MERGES {table}")

        # query_with_retry returns the last result rather than raising when the callback never
        # holds, so the assertion below is the oracle: a wedged mutation reports a non-zero count.
        node.query_with_retry(
            f"SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = '{table}' AND NOT is_done",
            check_callback=lambda result: result.strip() == "0",
            retry_count=60,
            sleep_time=1,
        )
        assert (
            node.query(
                f"SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = '{table}' AND NOT is_done"
            ).strip()
            == "0"
        )
        assert node.query(f"SELECT a, b FROM {table}").strip() == "1\t2"

        node.query("SYSTEM FLUSH LOGS")
        assert (
            int(
                node.query(
                    f"""
                    SELECT count()
                    FROM system.text_log
                    WHERE logger_name = 'MutationsInterpreter(default.{table})'
                      AND message = 'Column b is not physically stored, skipping statistics materialization'
                    """
                ).strip()
            )
            >= 1
        )
    finally:
        set_override("true")
