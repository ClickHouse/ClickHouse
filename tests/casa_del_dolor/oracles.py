import random
from typing import Optional
from integration.helpers.client import Client
from integration.helpers.cluster import ClickHouseInstance


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
        self, _node_index: int, _schema_name: str, _table_name: str, _table_engine: str
    ):
        self.node_index = _node_index
        self.schema_name = _schema_name
        self.table_name = _table_name
        self.table_engine = _table_engine
        self.first_hash = ""

    def set_first_hash(self, _first_hash: str):
        self.first_hash = _first_hash

    def get_sql_escaped_full_name(self) -> str:
        return f"`{self.schema_name}`.`{self.table_name}`"

    def supports_final(self) -> bool:
        to_check: str = self.table_engine
        if to_check.startswith("Shared"):
            to_check = to_check[6:]
        elif to_check.startswith("Replicated"):
            to_check = to_check[10:]
        return to_check in ClickHouseTable.FINAL_SUPPORTED_ENGINES


def dump_table(
    instances: list[ClickHouseInstance], logger
) -> Optional[ClickHouseTable]:
    next_node_index = random.choice(range(0, len(instances)))
    next_node = instances[next_node_index]
    client = Client(host=next_node.ip_address)

    try:
        tables_str = client.query(
            """
            SELECT database, name, engine
            FROM system.tables
            WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA') AND NOT is_temporary AND engine NOT IN ('Merge', 'GenerateRandom', 'Memory');
            """
        )
        if not isinstance(tables_str, str) or tables_str == "":
            return None

        fetched_tables: list[tuple[str, ...]] = [
            tuple(line.split("\t")) for line in tables_str.split("\n") if line
        ]
        random_table: tuple[str, ...] = random.choice(fetched_tables)
        next_tbl: ClickHouseTable = ClickHouseTable(
            next_node_index, random_table[0], random_table[1], random_table[2]
        )
        logger.info(
            f"Collecting table: {random_table[0]}.{random_table[1]} hash before a shutdown"
        )

        next_hash = client.query(
            f"SELECT cityHash64(groupArray(sipHash128(*))) FROM {next_tbl.get_sql_escaped_full_name()}{" FINAL" if next_tbl.supports_final() else ""} ORDER BY ALL;"
        )
        if not isinstance(next_hash, str) or next_hash == "":
            return None
        next_tbl.set_first_hash(next_hash)

        return next_tbl
    except Exception as ex:
        logger.warn(f"Error occurred when picking a table dor dumping: {ex}")
    return None
