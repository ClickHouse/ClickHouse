import random
from typing import Optional
from integration.helpers.client import Client
from integration.helpers.cluster import ClickHouseCluster, ClickHouseInstance


class ClickHouseTable:
    FINAL_SUPPORTED_ENGINES = [
        "ReplacingMergeTree",
        "CoalescingMergeTree",
        "SummingMergeTree",
        "AggregatingMergeTree",
        "CollapsingMergeTree",
        "VersionedCollapsingMergeTree",
        "GraphiteMergeTree",
        "Buffer",
        "Distributed",
    ]

    def __init__(
        self, _node_name: str, _schema_name: str, _table_name: str, _table_engine: str
    ):
        self.node_name = _node_name
        self.schema_name = _schema_name
        self.table_name = _table_name
        self.table_engine = _table_engine
        self.first_hash = None

    def set_first_hash(self, _first_hash: Optional[str]):
        self.first_hash = _first_hash

    def get_sql_escaped_full_name(self, rename=None) -> str:
        return f"`{self.schema_name}`.`{rename if rename is not None else self.table_name}`"

    def is_shared_or_replicated_merge_tree(self):
        return self.table_engine.startswith("Shared") or self.table_engine.startswith(
            "Replicated"
        )

    def get_hash_query(self, rename=None):
        return f"SELECT cityHash64(groupArray(sipHash128(*))) FROM (SELECT * FROM {self.get_sql_escaped_full_name(rename)}{" FINAL" if self.supports_final() else ""} ORDER BY ALL);"

    def supports_final(self) -> bool:
        to_check: str = self.table_engine
        if to_check.startswith("Shared"):
            to_check = to_check[6:]
        elif to_check.startswith("Replicated"):
            to_check = to_check[10:]
        return to_check in ClickHouseTable.FINAL_SUPPORTED_ENGINES


class ElOraculoDeTablas:
    """Translates to `The Oracle of Tables`. Just keeping the naming convention"""

    def __init__(self):
        self.table_counter = 0

    def increment_counter(self):
        self.table_counter += 1

    def get_current_table_name(self) -> str:
        return f"mytest{self.table_counter}"

    def collect_table_hash_before_shutdown(
        self, cluster: ClickHouseCluster, logger
    ) -> Optional[ClickHouseTable]:
        next_node_name: str = random.choice(list(cluster.instances.keys()))
        next_node: ClickHouseInstance = cluster.instances[next_node_name]
        next_tbl: Optional[ClickHouseTable] = None
        client = Client(
            host=next_node.ip_address, port=9000, command=cluster.client_bin_path
        )

        try:
            # Limit to tables only, exclude not deterministic tables, and tables not persisted after restarts
            tables_str = client.query(
                """
                SELECT database, name, engine
                FROM system.tables
                WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
                AND NOT is_temporary
                AND NOT match(engine, '.*View.*|Dictionary|Merge$|GenerateRandom|Memory|.*Set');
                """
            )
            if not isinstance(tables_str, str) or tables_str == "":
                logger.warn(f"No tables found to fetch on node {next_node.name}")
                return None

            fetched_tables: list[tuple[str, ...]] = [
                tuple(line.split("\t")) for line in tables_str.split("\n") if line
            ]
            random_table: tuple[str, ...] = random.choice(fetched_tables)
            next_tbl = ClickHouseTable(
                next_node_name, random_table[0], random_table[1], random_table[2]
            )

            # Rename table, so the client generator, won't update it
            self.increment_counter()
            client.query(
                f"RENAME TABLE {next_tbl.get_sql_escaped_full_name()} TO {next_tbl.get_sql_escaped_full_name(self.get_current_table_name())};"
            )
        except Exception as ex:
            logger.warn(
                f"Error occurred while picking a table for hashing before restarting server: {ex}"
            )
            return None
        # Table was renamed, so it has to be renamed back later
        try:
            # Make sure all replicas are in sync
            if next_tbl.is_shared_or_replicated_merge_tree():
                client.query(
                    f"SYSTEM SYNC REPLICA {next_tbl.get_sql_escaped_full_name(self.get_current_table_name())};"
                )
            # Fetch table data and hash it
            logger.info(
                f"Collecting table {next_tbl.get_sql_escaped_full_name()} hash from node {next_node.name} before shutdown"
            )
            next_hash = client.query(
                next_tbl.get_hash_query(self.get_current_table_name())
            )
            if isinstance(next_hash, str) and next_hash != "":
                next_tbl.set_first_hash(next_hash)
        except Exception as ex:
            logger.warn(
                f"Error occurred while processing hash for a table before restarting server: {ex}"
            )
        return next_tbl

    def collect_table_hash_after_shutdown(
        self, cluster: ClickHouseCluster, logger, next_tbl: Optional[ClickHouseTable]
    ):
        if next_tbl is not None:
            next_hash = None
            next_node: ClickHouseInstance = cluster.instances[next_tbl.node_name]
            client = Client(
                host=next_node.ip_address, port=9000, command=cluster.client_bin_path
            )

            if next_tbl.first_hash is not None:
                logger.info(
                    f"Collecting table {next_tbl.get_sql_escaped_full_name()} hash from node {next_node.name} after shutdown"
                )
                try:
                    # Fetch table data and hash it
                    next_hash = client.query(
                        next_tbl.get_hash_query(self.get_current_table_name())
                    )
                except Exception as ex:
                    logger.warn(
                        f"Error occurred while hashing a table after restarting server: {ex}"
                    )
                if isinstance(next_hash, str) and next_hash != next_tbl.first_hash:
                    message: str = (
                        f"Hash mismatch for table {next_tbl.get_sql_escaped_full_name()}"
                    )
                    logger.warn(message)
                    raise ValueError(message)

            try:
                # Rename back
                client.query(
                    f"RENAME TABLE {next_tbl.get_sql_escaped_full_name(self.get_current_table_name())} TO {next_tbl.get_sql_escaped_full_name()};"
                )
            except Exception as ex:
                logger.warn(f"Error while renaming table after restarting server: {ex}")
