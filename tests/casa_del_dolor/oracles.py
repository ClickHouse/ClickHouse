import random
from typing import Optional
from tests.integration.helpers.client import Client
from tests.integration.helpers.cluster import ClickHouseCluster, ClickHouseInstance


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
        self.mutation_ids = set()
        self.write_marker = 0

    def set_first_hash(self, _first_hash: Optional[str]):
        self.first_hash = _first_hash

    def get_mutations(self, client: Client, rename=None) -> tuple[set[str], int]:
        """Mutation ids of the table and how many of them are unfinished"""
        res = client.query(
            f"SELECT mutation_id, is_done FROM system.mutations WHERE database = '{self.schema_name}' AND table = '{rename if rename is not None else self.table_name}';"
        )
        rows = [line.split("\t") for line in res.split("\n") if line]
        return {r[0] for r in rows}, sum(1 for r in rows if r[1] == "0")

    def get_write_marker(self, client: Client, rename=None) -> int:
        """Grows with every insert: the highest block number of a MergeTree table (merges keep it), the rows of a Log one"""
        table = rename if rename is not None else self.table_name
        if "MergeTree" in self.table_engine:
            query = f"SELECT max(max_block_number) FROM system.parts WHERE database = '{self.schema_name}' AND table = '{table}' AND active"
        else:
            query = f"SELECT total_rows FROM system.tables WHERE database = '{self.schema_name}' AND name = '{table}'"
        res = client.query(query).strip()
        return int(res) if res.isdigit() else 0

    def get_sql_escaped_full_name(self, rename=None) -> str:
        return f"`{self.schema_name}`.`{rename if rename is not None else self.table_name}`"

    def is_shared_or_replicated_merge_tree(self):
        return self.table_engine.startswith("Shared") or self.table_engine.startswith(
            "Replicated"
        )

    def get_hash_query(self, rename=None):
        return f"SELECT cityHash64(groupArray(sipHash128(*))) FROM (SELECT * FROM {self.get_sql_escaped_full_name(rename)}{' FINAL' if self.supports_final() else ''} ORDER BY ALL);"

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
        self, cluster: ClickHouseCluster, logger, next_node: ClickHouseInstance
    ) -> Optional[ClickHouseTable]:
        next_node_name: str = next_node.name
        next_tbl: Optional[ClickHouseTable] = None
        client = Client(
            host=next_node.ip_address, port=9000, command=cluster.client_bin_path
        )

        try:
            # Local engines only, without TTL; Summing/Aggregating FINAL depends on the part layout, and Collapsing
            # FINAL on the merge order of arbitrary signs. RENAME is local, so replicas outside a Replicated database
            # keep getting inserts under the old name, and materialized views keep writing into their targets and
            # inner tables. A merge recompresses lossy codecs, which may change the values.
            tables_str = client.query("""
                SELECT database, name, engine
                FROM system.tables
                WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
                AND NOT is_temporary
                AND match(engine, '^(Shared|Replicated)?(|Replacing|Coalescing)MergeTree$|^(Log|TinyLog|StripeLog)$')
                AND position(create_table_query, ' TTL ') = 0
                AND NOT match(create_table_query, '[^A-Za-z0-9_](SZ3|Quantized)[^A-Za-z0-9_]')
                AND (NOT startsWith(engine, 'Replicated') OR database IN (SELECT name FROM system.databases WHERE engine = 'Replicated'))
                AND NOT startsWith(name, '.inner')
                AND NOT arrayExists(q -> position(q, name) > 0, (SELECT groupArray(create_table_query) FROM system.tables
                    WHERE engine = 'MaterializedView' AND position(create_table_query, ' TO ') > 0));
                """)
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
            # A mutation finishing between the two hashes changes the data, and RENAME keeps the queued ones.
            # A mutation or insert submitted before the RENAME may still land later, the second hash checks for it.
            next_tbl.mutation_ids, unfinished = next_tbl.get_mutations(
                client, self.get_current_table_name()
            )
            next_tbl.write_marker = next_tbl.get_write_marker(
                client, self.get_current_table_name()
            )
            if unfinished > 0:
                logger.info(
                    f"Skipping hash of table {next_tbl.get_sql_escaped_full_name()}: {unfinished} unfinished mutation(s)"
                )
                return next_tbl
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
                    second_hash = client.query(
                        next_tbl.get_hash_query(self.get_current_table_name())
                    )
                    new_mutations = (
                        next_tbl.get_mutations(client, self.get_current_table_name())[
                            0
                        ]
                        - next_tbl.mutation_ids
                    )
                    write_marker = next_tbl.get_write_marker(
                        client, self.get_current_table_name()
                    )
                    if new_mutations:
                        logger.info(
                            f"Skipping hash comparison of table {next_tbl.get_sql_escaped_full_name()}: mutation(s) {', '.join(sorted(new_mutations))} registered after the first hash"
                        )
                    elif write_marker > next_tbl.write_marker:
                        logger.info(
                            f"Skipping hash comparison of table {next_tbl.get_sql_escaped_full_name()}: an insert landed after the first hash"
                        )
                    else:
                        next_hash = second_hash
                except Exception as ex:
                    logger.warn(
                        f"Error occurred while hashing a table after restarting server: {ex}"
                    )
            try:
                # Rename back
                client.query(
                    f"RENAME TABLE {next_tbl.get_sql_escaped_full_name(self.get_current_table_name())} TO {next_tbl.get_sql_escaped_full_name()};"
                )
            except Exception as ex:
                logger.warn(f"Error while renaming table after restarting server: {ex}")

            if isinstance(next_hash, str) and next_hash != next_tbl.first_hash:
                message: str = (
                    f"Hash mismatch for table {next_tbl.get_sql_escaped_full_name()}"
                )
                logger.warn(message)
                raise ValueError(message)

    # Errors caused by the data, not the server, e.g. a mutation stuck on `MODIFY COLUMN` to a non-Nullable type
    # over NULLs retries forever. `CANNOT_PARSE_*`, `CANNOT_CONVERT_TYPE`, out of range, `CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN`
    DATA_CONVERSION_ERRORS = (
        "(6, 25, 26, 27, 38, 41, 70, 72, 321, 349, 376, 407, 441, 467, 675, 676)"
    )
    # The same errors in a replicated mutation, which records them in its queue entry too. The code is only in the text.
    UNEXPECTED_QUEUE_ERROR = (
        "last_exception != '' AND NOT (type = 'MUTATE_PART' AND "
        f"toUInt32OrZero(extract(last_exception, '^Code: ([0-9]+)[.]')) IN {DATA_CONVERSION_ERRORS})"
    )
    # Temporary data only in the distributed cache fails merges while no cache server is registered, e.g. after a restart
    UNEXPECTED_PART_ERROR = (
        f"exception != '' AND error NOT IN {DATA_CONVERSION_ERRORS} "
        "AND NOT (error = 900 AND exception LIKE '%no server is available%')"
    )

    HEALTH_CHECKS = [
        "broken detached part(s)",
        "broken replica(s)",
        "broken data part(s)",
        "shared catalog replica(s) needing recovery",
        "shared catalog drop/detach error(s)",
        "readonly replica(s)",
        "part(s) with excessive errors",
        "replica(s) with REPLICA_ALREADY_EXISTS errors",
        "replication queue exception(s)",
        "LOGICAL_ERROR(s) in text_log",
        "CORRUPTED_DATA(s) in text_log",
        "CHECKSUM_DOESNT_MATCH error(s) in text_log",
        "DATA_AFTER_MERGE_DIFF_FROM_EXPECTED error(s) in text_log",
        "stuck replication queue task(s) (>5 retries)",
    ]

    DETAIL_QUERIES = [
        "SELECT database, table, name FROM system.detached_parts WHERE startsWith(name, 'broken') LIMIT 3;",
        "SELECT database, table, lost_part_count FROM system.replicas WHERE lost_part_count > 0 LIMIT 3;",
        "SELECT message FROM system.text_log WHERE event_time >= now() - toIntervalSecond(60) AND level <= 'Information' AND splitByString(' in scope ', splitByString('(query: ', splitByString('(in query: ', message)[1])[1])[1] ILIKE concat('%', 'POTENTIALLY', '_BROKEN', '_DATA', '_PART', '%') ORDER BY event_time DESC LIMIT 3;",
        "",
        "",
        "SELECT database, table, last_exception FROM system.replicas WHERE readonly_start_time IS NOT NULL LIMIT 3;",
        f"SELECT database, table, part_name, exception FROM system.part_log WHERE {UNEXPECTED_PART_ERROR} AND event_time > (now() - toIntervalSecond(60)) ORDER BY event_time DESC LIMIT 3;",
        "SELECT message FROM system.text_log WHERE event_time >= now() - toIntervalSecond(60) AND level <= 'Information' AND splitByString(' in scope ', splitByString('(query: ', splitByString('(in query: ', message)[1])[1])[1] ILIKE concat('%', 'REPLICA', '_ALREADY', '_EXISTS', '%') ORDER BY event_time DESC LIMIT 3;",
        f"SELECT database, table, last_exception FROM system.replication_queue WHERE {UNEXPECTED_QUEUE_ERROR} LIMIT 3;",
        "SELECT message FROM system.text_log WHERE event_time >= now() - toIntervalSecond(60) AND level <= 'Information' AND splitByString(' in scope ', splitByString('(query: ', splitByString('(in query: ', message)[1])[1])[1] ILIKE concat('%', 'LOGICAL', '_ERROR', '%') ORDER BY event_time DESC LIMIT 3;",
        "SELECT message FROM system.text_log WHERE event_time >= now() - toIntervalSecond(60) AND level <= 'Information' AND splitByString(' in scope ', splitByString('(query: ', splitByString('(in query: ', message)[1])[1])[1] ILIKE concat('%', 'CORRUPTED', '_DATA', '%') ORDER BY event_time DESC LIMIT 3;",
        "SELECT message FROM system.text_log WHERE event_time >= now() - toIntervalSecond(60) AND level <= 'Information' AND splitByString(' in scope ', splitByString('(query: ', splitByString('(in query: ', message)[1])[1])[1] ILIKE concat('%', 'CHECKSUM', '_DOESNT', '_MATCH', '%') ORDER BY event_time DESC LIMIT 3;",
        "SELECT message FROM system.text_log WHERE event_time >= now() - toIntervalSecond(60) AND level <= 'Information' AND splitByString(' in scope ', splitByString('(query: ', splitByString('(in query: ', message)[1])[1])[1] ILIKE concat('%', 'DATA', '_AFTER', '_MERGE', '_DIFF', '_FROM', '_EXPECTED', '%') ORDER BY event_time DESC LIMIT 3;",
        f"SELECT database, table, type, last_exception, num_tries FROM system.replication_queue WHERE {UNEXPECTED_QUEUE_ERROR} AND num_tries > 5 ORDER BY num_tries DESC LIMIT 3;",
    ]

    def run_health_check(
        self, cluster: ClickHouseCluster, servers: list[ClickHouseInstance], logger
    ):
        for next_node in servers:
            logger.info(f"Collecting monitoring information for node {next_node.name}")
            client = Client(
                host=next_node.ip_address, port=9000, command=cluster.client_bin_path
            )
            info_str = ""
            try:
                info_str = client.query(f"""
                    SELECT x FROM (
                    (SELECT count() x, 1 y FROM system.detached_parts WHERE startsWith("name", 'broken'))
                     UNION ALL
                    (SELECT ifNull(sum(lost_part_count), 0), 2 y FROM system.replicas)
                     UNION ALL
                    -- Single scan of text_log for all pattern-based checks (3, 8, 10, 11, 12)
                    (SELECT t.1 x, t.2 y FROM (
                     SELECT arrayJoin(arrayZip(
                       [countIf(msg ILIKE concat('%','POTENTIALLY','_BROKEN','_DATA','_PART','%')),
                        countIf(msg ILIKE concat('%','REPLICA','_ALREADY','_EXISTS','%')),
                        countIf(msg ILIKE concat('%','LOGICAL','_ERROR','%')),
                        countIf(msg ILIKE concat('%','CORRUPTED','_DATA','%')),
                        countIf(msg ILIKE concat('%','CHECKSUM','_DOESNT','_MATCH','%')),
                        countIf(msg ILIKE concat('%','DATA','_AFTER','_MERGE','_DIFF','_FROM','_EXPECTED','%'))],
                       [toUInt64(3), toUInt64(8), toUInt64(10), toUInt64(11), toUInt64(12), toUInt64(13)]
                     )) AS t
                     -- Match the server's own words: it echoes the statement at Debug/Trace and quotes it after `(in query:`
                     FROM (SELECT splitByString(' in scope ', splitByString('(query: ', splitByString('(in query: ', message)[1])[1])[1] AS msg
                           FROM system.text_log
                           WHERE event_time >= now() - toIntervalSecond(60) AND level <= 'Information')) tlog)
                     UNION ALL
                    (SELECT count() x, 4 y FROM clusterAllReplicas(default, system.clusters)
                     WHERE is_shared_catalog_cluster = true AND is_local = true AND recovery_time > 10000)
                     UNION ALL
                    -- Aggregated like every other check: unaggregated it emits one row per replica and shifts all later checks
                    (SELECT greatest(sum(value), 0)::UInt64 x, 5 y FROM clusterAllReplicas(default, system.metrics) WHERE "name" = 'SharedCatalogDropDetachLocalTablesErrors')
                     UNION ALL
                    (SELECT count() x, 6 y FROM clusterAllReplicas(default, system.replicas) WHERE readonly_start_time IS NOT NULL)
                     UNION ALL
                    (SELECT count() x, 7 y FROM (SELECT part_name FROM clusterAllReplicas(default, system.part_log)
                     WHERE {self.UNEXPECTED_PART_ERROR} AND event_time > (now() - toIntervalSecond(60))
                     GROUP BY part_name HAVING count() > 10) tx)
                     UNION ALL
                    (SELECT count() x, 9 y FROM system.replication_queue WHERE {self.UNEXPECTED_QUEUE_ERROR})
                     UNION ALL
                    (SELECT count() x, 14 y FROM system.replication_queue
                     WHERE {self.UNEXPECTED_QUEUE_ERROR} AND num_tries > 5)
                    ) tx ORDER BY y SETTINGS use_query_condition_cache = 0, use_query_cache = 0;
                    """)
            except Exception as ex:
                logger.warn(
                    f"Error occurred while fetching monitoring information for node {next_node.name}: {ex}"
                )
                continue
            if not isinstance(info_str, str) or info_str == "":
                logger.warn(
                    f"No monitoring information found for node {next_node.name}"
                )
                continue

            fetched_info: list[int] = [
                int(line) for line in info_str.split("\n") if line
            ]
            for idx, check_name in enumerate(ElOraculoDeTablas.HEALTH_CHECKS):
                if fetched_info[idx] != 0:
                    details = ""
                    detail_query = ElOraculoDeTablas.DETAIL_QUERIES[idx]
                    if detail_query:
                        try:
                            detail_str = client.query(detail_query)
                            if isinstance(detail_str, str) and detail_str:
                                details = "\nDetails:\n  " + detail_str.replace(
                                    "\n", "\n  "
                                )
                        except Exception as ex:
                            logger.warn(
                                f"Failed to fetch details for '{check_name}': {ex}"
                            )
                    message: str = (
                        f"Health check '{check_name}' failed on node {next_node.name}: {fetched_info[idx]} issues found{details}"
                    )
                    logger.warn(message)
                    raise ValueError(message)
