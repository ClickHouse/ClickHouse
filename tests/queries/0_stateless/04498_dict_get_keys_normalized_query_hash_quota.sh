#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `dictGetKeys` scans the whole dictionary through a bespoke `ReadProgressCallback` (not the usual
# `QueryPipeline`), so the scan's `read_rows`/`read_bytes` are accounted against the query's quota.
# Under a `KEYED BY normalized_query_hash` quota each distinct `dictGetKeys` query pattern must get its
# own bucket, just like a real query. Previously the callback never received the normalized query hash,
# so every `dictGetKeys` pattern shared the single hash-`0` bucket; once one pattern exhausted it, a
# structurally different pattern was rejected too. This checks that two structurally different
# `dictGetKeys` patterns get independent read-progress buckets. (`read_rows` and `read_bytes` are routed
# through the same callback, so a `read_rows` limit is enough to prove the hash is propagated.)
#
# Quotas and users are server-global, so their names are suffixed with the unique database name to keep
# the test isolated when it runs in parallel with itself (e.g. in the flaky check).

user="u_04498_${CLICKHOUSE_DATABASE}"
quota="q_04498_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${quota}"
${CLICKHOUSE_CLIENT} -q "DROP DICTIONARY IF EXISTS dict_04498"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS src_04498"

${CLICKHOUSE_CLIENT} -q "CREATE USER ${user}"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON *.* TO ${user}"
${CLICKHOUSE_CLIENT} -q "GRANT dictGet ON *.* TO ${user}"

# One 1000-row source; the const-path `dictGetKeys` scan reads the whole dictionary (~1000 rows) for
# every call. The dictionary is pre-loaded so that the loading read (which does not go through this
# callback) cannot perturb the accounting.
${CLICKHOUSE_CLIENT} -n -q "
    CREATE TABLE src_04498 (id UInt64, attr Int32) ENGINE = Memory;
    INSERT INTO src_04498 SELECT number, number % 5 FROM numbers(1000);
    CREATE DICTIONARY dict_04498 (id UInt64, attr Int32)
        PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'src_04498' DB currentDatabase())) LIFETIME(0) LAYOUT(HASHED());
    SYSTEM RELOAD DICTIONARY dict_04498;
"

# read_rows limit of 1500: one scan (~1000 rows) fits, a second one in the same bucket (~2000) exceeds it.
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${quota} KEYED BY normalized_query_hash FOR INTERVAL 100 YEAR MAX read_rows = 1500 TO ${user}"

# shellcheck disable=SC2086  # CLICKHOUSE_CLIENT must be word-split.
dgk() { ${CLICKHOUSE_CLIENT} --user "${user}" --send_logs_level=none -q "$1" 2>&1 | grep -m1 -o QUOTA_EXCEEDED || echo allowed; }

echo "--- pattern A, first dictGetKeys (~1000 read rows) is within the per-pattern limit ---"
dgk "SELECT dictGetKeys('dict_04498', 'attr', toInt32(1))"
echo "--- pattern A, second dictGetKeys (cumulative ~2000 > 1500) exceeds the limit of its own bucket ---"
dgk "SELECT dictGetKeys('dict_04498', 'attr', toInt32(2))"
echo "--- pattern B (a structurally different dictGetKeys) has its own bucket and is still allowed ---"
dgk "SELECT length(dictGetKeys('dict_04498', 'attr', toInt32(1)))"

${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${quota}"
${CLICKHOUSE_CLIENT} -q "DROP DICTIONARY IF EXISTS dict_04498"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS src_04498"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}"
