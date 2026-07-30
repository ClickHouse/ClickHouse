#!/usr/bin/env bash
# Rows re-read from the temporary files of an external sort must not be charged to the
# `READ_ROWS` quota. That quota is charged in its own branch of `ReadProgressCallback::onProgress`,
# separate from the branch that increments `SelectedRows`, so it needs its own coverage: before
# the fix a spilling query was charged 165409 rows for a 100000-row read, and a legitimate query
# under `MAX READ ROWS = 150000` was rejected purely because it spilled.
#
# The limit sits strictly between the true 100000 and that formerly doubled count, so the
# spilling run below is the assertion. The in-memory run is a control against a change that
# deflated every path, and it shares the quota so its own charge is asserted exactly too.
#
# `ast_fuzzer_runs` is pinned because the stress profile enables the server-side AST fuzzer for
# any query; a fuzzed re-execution would be charged to the same quota and move the totals.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

ROLE="r_${CLICKHOUSE_TEST_UNIQUE_NAME}"
USER="u_${CLICKHOUSE_TEST_UNIQUE_NAME}"
QUOTA="q_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${QUOTA}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS ${ROLE}"

${CLICKHOUSE_CLIENT} -q "CREATE ROLE ${ROLE}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER}"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON system.* TO ${ROLE}"
${CLICKHOUSE_CLIENT} -q "GRANT ${ROLE} TO ${USER}"

SORT_SETTINGS="ast_fuzzer_runs = 0, max_threads = 1, max_memory_usage = 134217728, max_block_size = 65409, max_bytes_ratio_before_external_sort = 0"

# In-memory control. Reads 100000 rows and must be charged exactly that.
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${QUOTA} FOR INTERVAL 100 YEAR MAX READ ROWS = 150000 TO ${ROLE}"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SELECT number FROM numbers(100000) ORDER BY sipHash64(number)
    SETTINGS ${SORT_SETTINGS}, max_bytes_before_external_sort = 0
    FORMAT Null" 2>&1 | grep -m1 -o QUOTA_EXCEEDED
echo "memory $(${CLICKHOUSE_CLIENT} -q "SELECT read_rows FROM system.quotas_usage WHERE quota_name = '${QUOTA}'")"

# Same read, spilled. Must also be charged exactly 100000, so it must not be rejected.
${CLICKHOUSE_CLIENT} -q "DROP QUOTA ${QUOTA}"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${QUOTA} FOR INTERVAL 100 YEAR MAX READ ROWS = 150000 TO ${ROLE}"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SELECT number FROM numbers(100000) ORDER BY sipHash64(number)
    SETTINGS ${SORT_SETTINGS}, max_bytes_before_external_sort = 100000
    FORMAT Null" 2>&1 | grep -m1 -o QUOTA_EXCEEDED
echo "spill $(${CLICKHOUSE_CLIENT} -q "SELECT read_rows FROM system.quotas_usage WHERE quota_name = '${QUOTA}'")"

${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${QUOTA}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS ${ROLE}"
