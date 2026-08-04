"""Parameterization layer for the `lazy_load_tables` tests.

The test bodies are written against `LazyEnv` so the same tests can run over more than one
database/table engine pair. The public build covers `Atomic` with `MergeTree` and
`ReplicatedMergeTree`; a private build can register a `Shared` / `SharedMergeTree` variant by
adding an entry to `ENVIRONMENTS` without touching the tests.

`LazyEnv.supports` lets a test skip a combination that the engine pair cannot express.
"""

import pytest

# Capabilities a test can require via LazyEnv.supports().
REPLICATION = "replication"  # system.replicas, SYSTEM SYNC REPLICA, /replicas_status
BACKUP = "backup"
MOVE_PARTITION = "move_partition"


class LazyEnv:
    def __init__(self, node, db_engine, table_engine, capabilities):
        self.node = node
        self.db_engine = db_engine
        self.table_engine = table_engine
        self.capabilities = capabilities
        self._counter = 0

    def supports(self, capability):
        return capability in self.capabilities

    def require(self, capability):
        if not self.supports(capability):
            pytest.skip(f"{self.db_engine}/{self.table_engine} does not support {capability}")

    def create_database(self, name, lazy=True):
        self.node.query(f"DROP DATABASE IF EXISTS {name} SYNC")
        settings = " SETTINGS lazy_load_tables = 1" if lazy else ""
        self.node.query(f"CREATE DATABASE {name} ENGINE = {self.db_engine}{settings}")

    def engine_clause(self, db, table, args=""):
        """The ENGINE clause for the parameterized table engine, with a unique coordination path."""
        if self.table_engine == "MergeTree":
            return f"MergeTree({args})" if args else "MergeTree"
        self._counter += 1
        path = f"/clickhouse/test_lazy/{db}/{table}_{self._counter}"
        joined = f"'{path}', 'r1'" + (f", {args}" if args else "")
        return f"{self.table_engine}({joined})"

    def create_table(self, db, table, columns, order_by="id", extra="", engine_args=""):
        engine = self.engine_clause(db, table, engine_args)
        self.node.query(
            f"CREATE TABLE {db}.{table} ({columns}) ENGINE = {engine} ORDER BY {order_by} {extra}"
        )

    def reload(self, db):
        """Make the database re-read its metadata, so lazy tables become proxies again."""
        self.node.query(f"DETACH DATABASE {db}")
        self.node.query(f"ATTACH DATABASE {db}")

    def engine_of(self, db, table):
        return self.node.query(
            f"SELECT engine FROM system.tables WHERE database = '{db}' AND name = '{table}'"
        ).strip()

    def is_deferred(self, db, table):
        return self.engine_of(db, table) == "TableProxy"


def build_environments(node):
    """Engine pairs covered in this build. A private build extends this list."""
    return {
        "atomic_mergetree": LazyEnv(
            node, "Atomic", "MergeTree", {BACKUP, MOVE_PARTITION}
        ),
        "atomic_replicated": LazyEnv(
            node, "Atomic", "ReplicatedMergeTree", {REPLICATION, BACKUP, MOVE_PARTITION}
        ),
    }


ENVIRONMENT_IDS = ["atomic_mergetree", "atomic_replicated"]
