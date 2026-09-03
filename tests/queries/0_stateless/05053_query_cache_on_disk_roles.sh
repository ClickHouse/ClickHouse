#!/usr/bin/env bash
# A non-shared entry of the query cache on disk belongs not only to the user who wrote it, but also to the set of roles that was current
# at the time of the write: the same user under a different set of current roles must neither be served that entry nor be blocked from
# storing its own one. This matters because roles carry privileges and row policies, so a result computed under one role may contain
# rows that must not be visible under another role.
# Only the on-disk backend is exercised, reads from and writes to the in-memory query cache are disabled.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_DATABASE}"
role_a="role_a_${CLICKHOUSE_DATABASE}"
role_b="role_b_${CLICKHOUSE_DATABASE}"
table="${CLICKHOUSE_DATABASE}.tab"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${table} (a UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO ${table} SELECT number FROM numbers(100);
    DROP USER IF EXISTS ${user};
    DROP ROLE IF EXISTS ${role_a}, ${role_b};
    CREATE USER ${user};
    CREATE ROLE ${role_a}, ${role_b};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${role_a}, ${role_b};
    GRANT ${role_a}, ${role_b} TO ${user};
    -- The row policies make the two roles see different rows, i.e. serving one role the entry of the other would leak rows.
    CREATE ROW POLICY ${role_a} ON ${table} USING a < 50 TO ${role_a};
    CREATE ROW POLICY ${role_b} ON ${table} USING a < 30 TO ${role_b};
"

# `cache_for_query_results` is the filesystem cache preconfigured for tests, see tests/config/config.d/query_result_cache_on_disk.xml
settings="use_query_cache = true, query_cache_on_disk_cache_name = 'cache_for_query_results', enable_reads_from_query_cache = false, enable_writes_to_query_cache = false"

query="SELECT count() FROM ${table}"

run() # query_id
{
    # A fresh connection picks up the current default roles of the user, so switching them below switches the current roles of the query.
    # ${CLICKHOUSE_CLIENT} already selects the test database, so the user runs with it as its current database: it is part of the cache
    # key, and it is what restricts the `system.query_log` lookups below.
    ${CLICKHOUSE_CLIENT} --user "${user}" --query_id "$1" --query "${query} SETTINGS ${settings}"
}

hits() # query_id
{
    ${CLICKHOUSE_CLIENT} --query "
        SELECT ProfileEvents['QueryCacheOnDiskHits'] FROM system.query_log
        WHERE current_database = currentDatabase() AND query_id = '$1' AND type = 'QueryFinish'
        ORDER BY event_time_microseconds DESC LIMIT 1
    "
}

echo "-- Under role A, the second run of the query is a hit"
${CLICKHOUSE_CLIENT} --query "SET DEFAULT ROLE ${role_a} TO ${user}"
echo -n "role A, first run, rows: "; run "05053_a_1_${CLICKHOUSE_DATABASE}"
echo -n "role A, second run, rows: "; run "05053_a_2_${CLICKHOUSE_DATABASE}"

echo "-- Under role B, the same query misses on the entry of role A and stores its own entry"
${CLICKHOUSE_CLIENT} --query "SET DEFAULT ROLE ${role_b} TO ${user}"
echo -n "role B, first run, rows: "; run "05053_b_1_${CLICKHOUSE_DATABASE}"
echo -n "role B, second run, rows: "; run "05053_b_2_${CLICKHOUSE_DATABASE}"

echo "-- The entry of role A was not overwritten"
${CLICKHOUSE_CLIENT} --query "SET DEFAULT ROLE ${role_a} TO ${user}"
echo -n "role A, third run, rows: "; run "05053_a_3_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

echo -n "role A, first run, hits: "; hits "05053_a_1_${CLICKHOUSE_DATABASE}"
echo -n "role A, second run, hits: "; hits "05053_a_2_${CLICKHOUSE_DATABASE}"
echo -n "role B, first run, hits: "; hits "05053_b_1_${CLICKHOUSE_DATABASE}"
echo -n "role B, second run, hits: "; hits "05053_b_2_${CLICKHOUSE_DATABASE}"
echo -n "role A, third run, hits: "; hits "05053_a_3_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
    DROP ROW POLICY ${role_a}, ${role_b} ON ${table};
    DROP USER ${user};
    DROP ROLE ${role_a}, ${role_b};
"
