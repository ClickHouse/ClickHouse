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


def test_old_analyzer_keeps_equality_chain_semantics(start_cluster):
    """
    The old analyzer folds `x = c1 OR x = c2 OR x = c3` into `x IN (c1, c2, c3)` in
    `LogicalExpressionsOptimizer`. That fold must keep the semantics of `equals`: `IN`
    matches by set membership, which disagrees with `equals` on a floating-point NaN
    (`nan = nan` is 0, `nan IN (nan)` is 1) and on the signed zero (`-0.0 = 0.0` is 1,
    `-0.0 IN (0.0)` is 0). And `in` rejects a `Dynamic` / `JSON` argument outright, so
    such a chain has to stay a comparison for the mutation to run at all.
    """
    set_override("false")
    try:
        table = "t_override_false_chain"
        node.query(f"DROP TABLE IF EXISTS {table} SYNC")
        node.query(
            f"CREATE TABLE {table} (id UInt64, f Float64, d Dynamic) "
            "ENGINE = MergeTree ORDER BY id"
        )
        node.query(
            f"INSERT INTO {table} VALUES "
            "(1, nan, '1'), (2, 1., '2'), (3, 2., '3'), (4, 5., '4'), (5, -0.0, '5')"
        )

        # `f = nan` matches nothing, so only rows 2 and 3 go.
        node.query(
            f"ALTER TABLE {table} DELETE WHERE (f = nan) OR (f = 1.) OR (f = 2.) "
            "SETTINGS mutations_sync = 2"
        )
        assert node.query(f"SELECT id FROM {table} ORDER BY id") == "1\n4\n5\n"

        # `-0.0 = 0.0` holds, so row 5 goes too.
        node.query(
            f"ALTER TABLE {table} DELETE WHERE (f = 0.) OR (f = 5.) OR (f = 7.) "
            "SETTINGS mutations_sync = 2"
        )
        assert node.query(f"SELECT id FROM {table} ORDER BY id") == "1\n"

        # A string chain on a `Dynamic` column must not be folded into `IN`, which rejects the type.
        node.query(f"INSERT INTO {table} VALUES (6, 6., '6'), (7, 7., '7')")
        node.query(
            f"ALTER TABLE {table} DELETE WHERE (d = '1') OR (d = '6') OR (d = '9') "
            "SETTINGS mutations_sync = 2"
        )
        assert node.query(f"SELECT id FROM {table} ORDER BY id") == "7\n"

        assert count_old_analyzer_log_lines(table) >= 1
        node.query(f"DROP TABLE {table} SYNC")
    finally:
        set_override("true")
