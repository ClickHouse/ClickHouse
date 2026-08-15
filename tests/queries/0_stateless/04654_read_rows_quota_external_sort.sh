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
# The charge alone cannot carry either arm, because a quota is charged incrementally while the
# query runs: a query that reads all 100000 rows and then fails for any later reason has already
# been charged 100000, so the charge still looks right. Each arm therefore also asserts how its
# query terminated and how many temporary parts it wrote, and the client's own status is
# propagated. Without those, an arm that died after its read, or that quietly stopped spilling,
# would still print the expected charge and pass while covering nothing.
#
# `ast_fuzzer_runs` is pinned because the stress profile enables the server-side AST fuzzer for
# any query; a fuzzed re-execution would be charged to the same quota, and it inherits
# `log_comment` and would win the `argMax` below.
# `max_block_size` is pinned because it decides how much of the sort spills.

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

# Runs the sort as ${USER} so that it is charged to the quota. Keeps printing the `QUOTA_EXCEEDED`
# token that a pre-fix server produces, but reports the client's own exit status instead of
# discarding it the way a bare pipe into `grep` does. The output is captured into a variable rather
# than a temporary file because `CLICKHOUSE_TMP` is shared between concurrent copies of a test when
# the runner is given a fixed `--database`. $1 is the arm name, $2 is `max_bytes_before_external_sort`.
run_arm()
{
    local out rc
    out=$(${CLICKHOUSE_CLIENT} --user "${USER}" -q "SELECT number FROM numbers(100000) ORDER BY sipHash64(number)
        SETTINGS log_queries = 1, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_$1',
            ${SORT_SETTINGS}, max_bytes_before_external_sort = $2
        FORMAT Null" 2>&1)
    rc=$?
    case "${out}" in
        *QUOTA_EXCEEDED*)
            echo "QUOTA_EXCEEDED"
            ;;
        *)
            if [ "${rc}" -ne 0 ]; then
                echo "$1 query failed rc=${rc}"
                echo "${out}" | head -n 2
            fi
            ;;
    esac
}

# Reports how the arm's query terminated and how many temporary parts it wrote. The expected values
# are in the reference, so an arm that died after its read (`ExceptionWhileProcessing`), that never
# ran (empty), or that stopped spilling (`parts=0`) cannot satisfy it. `argMax` rather than `count`
# because a stress thread reuses one database, so a repeated run of this test logs a second row
# under the same `log_comment`; the latest terminal row is the one under test. $1 is the arm name.
check_arm()
{
    ${CLICKHOUSE_CLIENT} -q "SELECT '$1 status=' || argMax(toString(type), event_time_microseconds)
            || ' parts=' || toString(argMax(ProfileEvents['ExternalSortWritePart'], event_time_microseconds))
        FROM system.query_log
        WHERE type != 'QueryStart' AND event_date >= yesterday() AND event_time >= now() - 600
            AND current_database = currentDatabase()
            AND log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_$1'
        SETTINGS ast_fuzzer_runs = 0"
}

# In-memory control. Reads 100000 rows, must be charged exactly that, and must not spill.
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${QUOTA} FOR INTERVAL 100 YEAR MAX READ ROWS = 150000 TO ${ROLE}"
run_arm memory 0
echo "memory $(${CLICKHOUSE_CLIENT} -q "SELECT read_rows FROM system.quotas_usage WHERE quota_name = '${QUOTA}'")"

# Same read, spilled. Must also be charged exactly 100000, so it must not be rejected.
${CLICKHOUSE_CLIENT} -q "DROP QUOTA ${QUOTA}"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${QUOTA} FOR INTERVAL 100 YEAR MAX READ ROWS = 150000 TO ${ROLE}"
run_arm spill 100000
echo "spill $(${CLICKHOUSE_CLIENT} -q "SELECT read_rows FROM system.quotas_usage WHERE quota_name = '${QUOTA}'")"

# Read as the default user, so these do not consume the quota, which is granted only to ${ROLE}.
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
check_arm memory
check_arm spill

${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${QUOTA}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS ${ROLE}"
