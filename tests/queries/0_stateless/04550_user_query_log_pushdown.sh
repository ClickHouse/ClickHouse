#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

test_dir="${CLICKHOUSE_TMP}/04550_user_query_log_pushdown_${CLICKHOUSE_DATABASE}"
config="${test_dir}/config.xml"

rm -rf "${test_dir}"
mkdir -p "${test_dir}/data" "${test_dir}/tmp" "${test_dir}/user_files" "${test_dir}/format_schemas"

cat > "${config}" <<EOF
<clickhouse>
    <path>${test_dir}/data/</path>
    <tmp_path>${test_dir}/tmp/</tmp_path>
    <user_files_path>${test_dir}/user_files/</user_files_path>
    <format_schema_path>${test_dir}/format_schemas/</format_schema_path>
    <logger>
        <level>none</level>
        <console>false</console>
    </logger>
    <query_log>
        <database>system</database>
        <table>query_log</table>
        <engine>ENGINE = MergeTree PARTITION BY event_date ORDER BY event_time</engine>
        <enable_user_query_log>true</enable_user_query_log>
    </query_log>
</clickhouse>
EOF

${CLICKHOUSE_LOCAL} --config-file "${config}" --query "
    SELECT 4550 FORMAT Null;
    SYSTEM FLUSH LOGS query_log;

    SELECT 'correctness';

    -- Predicates on the partition, key and identity columns compared with constants are pushed down
    -- into the internal query over the query log table, and return the correct rows.
    SELECT count() >= 1 FROM system.user_query_log
    WHERE event_date >= yesterday() AND query_start_time <= now() AND type = 'QueryFinish' AND user = currentUser();

    -- A predicate on a column that is not pushed down is still applied by the outer filter.
    SELECT count() >= 1 FROM system.user_query_log WHERE event_date >= yesterday() AND query LIKE '%4550%';

    -- The shape from the review: a point lookup with ORDER BY and LIMIT.
    SELECT count() FROM
    (
        SELECT query_id FROM system.user_query_log
        WHERE event_date >= yesterday() AND query_id = 'nonexistent'
        ORDER BY query_start_time DESC LIMIT 10
    );

    SELECT 'pushdown';

    -- The pushed-down predicate prunes all partitions of the backing table (it is partitioned by
    -- event_date here): the query reads no rows at all, while the unrestricted scan reads the whole
    -- retained log. This holds with both analyzers.
    SET log_comment = '04550_full';
    SELECT count() FROM system.user_query_log FORMAT Null;
    SET log_comment = '04550_pruned_analyzer';
    SELECT count() FROM system.user_query_log WHERE event_date > today() + 1 SETTINGS enable_analyzer = 1 FORMAT Null;
    SET log_comment = '04550_pruned_old_analyzer';
    SELECT count() FROM system.user_query_log WHERE event_date > today() + 1 SETTINGS enable_analyzer = 0 FORMAT Null;

    -- The '>=' and '<=' operators (named 'greaterOrEquals' / 'lessOrEquals') must also be pushed down,
    -- so a bounded predicate that excludes every partition still reads no rows from the backing table.
    SET log_comment = '04550_pruned_ge';
    SELECT count() FROM system.user_query_log WHERE event_date >= yesterday() + 3 FORMAT Null;
    SET log_comment = '04550_pruned_le';
    SELECT count() FROM system.user_query_log WHERE event_date <= today() - 2 FORMAT Null;
    SET log_comment = '';
    SYSTEM FLUSH LOGS query_log;

    SELECT read_rows >= 1 FROM system.query_log
    WHERE type = 'QueryFinish' AND query_kind = 'Select' AND current_database = currentDatabase() AND log_comment = '04550_full';
    SELECT read_rows FROM system.query_log
    WHERE type = 'QueryFinish' AND query_kind = 'Select' AND current_database = currentDatabase() AND log_comment = '04550_pruned_analyzer';
    SELECT read_rows FROM system.query_log
    WHERE type = 'QueryFinish' AND query_kind = 'Select' AND current_database = currentDatabase() AND log_comment = '04550_pruned_old_analyzer';
    SELECT read_rows FROM system.query_log
    WHERE type = 'QueryFinish' AND query_kind = 'Select' AND current_database = currentDatabase() AND log_comment = '04550_pruned_ge';
    SELECT read_rows FROM system.query_log
    WHERE type = 'QueryFinish' AND query_kind = 'Select' AND current_database = currentDatabase() AND log_comment = '04550_pruned_le';
"

rm -rf "${test_dir}"
