#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/112096
# A failure reported by the _delta_log commit does not mean the commit is absent: a lost
# response, a timeout, or the object store retrying the conditional PUT and colliding with the
# entry its own first attempt stored all report an error over a durable commit. Both DeltaLake
# sinks used to delete every data file they wrote for any such error, so the committed version
# referenced objects that no longer existed: every SELECT failed and the acknowledged rows were
# gone. A commit whose outcome cannot be established now keeps its data files.
#
# Arm A injects a failure AFTER the log write (the commit is durable) and asserts the rows are
# still readable. Arm B injects one BEFORE it (the commit provably never happened) and asserts
# the data files are still cleaned up, so a fix that simply never deletes anything fails here.
#
# The empty Delta tables are bootstrapped by hand (a v0 _delta_log with only protocol +
# metaData), because ClickHouse cannot initialize a Delta transaction log itself, and because a
# partitioned Delta table cannot be created through CREATE at all.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_delta_unknown"
trap 'rm -rf "${ROOT}" 2>/dev/null' EXIT
rm -rf "${ROOT}"

# Create an empty Delta table at $1 with the given JSON schema string ($2) and partitionColumns
# array ($3), using a minimal v0 transaction log (what delta-rs writes for an empty overwrite).
bootstrap() {
    local path="$1"
    local schema="$2"
    local partition_cols="$3"
    mkdir -p "${path}/_delta_log"
    cat > "${path}/_delta_log/00000000000000000000.json" <<EOF
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"${CLICKHOUSE_DATABASE}-$(basename "${path}")","format":{"provider":"parquet","options":{}},"schemaString":"${schema}","partitionColumns":${partition_cols},"configuration":{},"createdTime":1700000000000}}
EOF
}

# yes/no rather than a match count, so an extra stack-trace line cannot move the number.
has() {
    if printf '%s' "$1" | grep -qF "$2"; then echo "yes"; else echo "no"; fi
}

# How many files the warning line actually names. The names themselves are per-writer UUIDs, so
# they cannot be matched literally; restricting the count to that one line keeps any other
# stderr line from contributing to it.
count_retained() {
    printf '%s' "$1" | grep -F 'keeping the' | grep -o '\.parquet' | wc -l | tr -d ' '
}

# Absolute number of data files physically present under the table root. Never a lower bound:
# "at least one survived" is satisfied by a single survivor out of many deleted files.
count_parquet() {
    find "$1" -name '*.parquet' -type f 2>/dev/null | wc -l | tr -d ' '
}

# Absolute number of commits in the log. The v0 bootstrap counts as one.
count_commits() {
    find "$1/_delta_log" -name '*.json' -type f 2>/dev/null | wc -l | tr -d ' '
}

SCHEMA_PLAIN='{\"type\":\"struct\",\"fields\":[{\"name\":\"c0\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}}]}'
SCHEMA_PART='{\"type\":\"struct\",\"fields\":[{\"name\":\"c0\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"part\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}'

# $1 = table dir, $2 = failpoint name, $3 = SELECT feeding the INSERT.
# The failpoint is armed in the same process as the INSERT: the registry is process-global, and
# clickhouse-local is a separate process, which is what keeps this test parallel-safe.
# CLICKHOUSE_LOCAL carries no log-level flag of its own (shell_config.sh only puts
# --send_logs_level into CLICKHOUSE_CLIENT_OPT), so ask for warnings explicitly to see the line
# naming the retained files. Every assertion over this output is a yes/no or comes from find, so
# the extra lines cannot move one.
insert_with_failpoint() {
    ${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --send_logs_level=warning --query "
        SYSTEM ENABLE FAILPOINT $2;
        INSERT INTO FUNCTION deltaLakeLocal('$1') $3;
    " 2>&1
}

# A FRESH clickhouse-local process for every read back: no failpoint left armed and no cached
# snapshot. sum(c0) and the row list must open the data files. count() alone would be a VACUOUS
# oracle here -- DeltaLakeMetadataDeltaKernel::totalRows answers it from the log's numRecords
# stats, so it returns 6 even with every parquet file deleted.
read_sum() {
    ${CLICKHOUSE_LOCAL} --query "SELECT sum(c0) FROM deltaLakeLocal('$1')" 2>&1 | tail -1
}
read_rows() {
    ${CLICKHOUSE_LOCAL} --query "SELECT arraySort(groupArray(c0)) FROM deltaLakeLocal('$1')" 2>&1 | tail -1
}

echo "==== Arm A: the commit reached the log and only the response was lost ===="
echo "-- the acknowledged rows must survive; deleting the data files here is the data loss"

for layout in plain partitioned; do
    T="${ROOT}/a_${layout}"
    if [ "${layout}" = "plain" ]; then
        bootstrap "${T}" "${SCHEMA_PLAIN}" '[]'
        # 6 rows (c0 = 0..5, sum 15) in one data file.
        ERR=$(insert_with_failpoint "${T}" delta_lake_commit_response_lost "SELECT number::Int32 AS c0 FROM numbers(6)")
        EXPECTED_PARQUET=1
    else
        bootstrap "${T}" "${SCHEMA_PART}" '["part"]'
        # The same 6 rows over 2 partitions, so 2 data files, sum still 15. A space needs URI
        # encoding but no Hive escaping, so the two forms of the partition directory differ.
        ERR=$(insert_with_failpoint "${T}" delta_lake_commit_response_lost "SELECT number::Int32 AS c0, if(number % 2 = 0, 'a b', 'c d') AS part FROM numbers(6)")
        EXPECTED_PARQUET=2
    fi

    echo "${layout}: reported as unknown outcome: $(has "${ERR}" 'UNKNOWN_STATUS_OF_TRANSACTION')"
    echo "${layout}: original error chained: $(has "${ERR}" 'Failpoint for a lost commit response')"
    # The retained objects are named in the server log, so an operator has a handle on them. The
    # count is asserted, the names are not: they are per-writer UUIDs.
    echo "${layout}: retained files logged: $(has "${ERR}" "keeping the ${EXPECTED_PARQUET} data file(s) written for it")"
    echo "${layout}: retained files named: $(count_retained "${ERR}") (expected ${EXPECTED_PARQUET})"
    if [ "${layout}" = "partitioned" ]; then
        # The names must be the object keys, not the committed `add.path` form: a space is
        # URI-encoded in `add.path` (`part=a%20b`) but not Hive-escaped in the directory.
        W=$(printf '%s' "${ERR}" | grep -F 'keeping the')
        echo "partitioned: retained files carry the physical partition dirs: $(has "${W}" 'part=a b/')/$(has "${W}" 'part=c d/'), encoded form absent: $(has "${W}" 'part=a%20b/')"
    fi
    echo "${layout}: sum(c0) after reopen: $(read_sum "${T}") (expected 15)"
    echo "${layout}: rows after reopen: $(read_rows "${T}") (expected [0,1,2,3,4,5])"
    echo "${layout}: data files kept: $(count_parquet "${T}") (expected ${EXPECTED_PARQUET})"
    echo "${layout}: commits in log: $(count_commits "${T}") (expected 2)"
done

echo
echo "==== Arm B: the commit never reached the log ===="
echo "-- the data files are unreferenced for certain, so they must still be cleaned up"

for layout in plain partitioned; do
    T="${ROOT}/b_${layout}"
    if [ "${layout}" = "plain" ]; then
        bootstrap "${T}" "${SCHEMA_PLAIN}" '[]'
        ERR=$(insert_with_failpoint "${T}" delta_lake_commit_fail_before_log_write "SELECT number::Int32 AS c0 FROM numbers(6)")
    else
        bootstrap "${T}" "${SCHEMA_PART}" '["part"]'
        ERR=$(insert_with_failpoint "${T}" delta_lake_commit_fail_before_log_write "SELECT number::Int32 AS c0, if(number % 2 = 0, 'a b', 'c d') AS part FROM numbers(6)")
    fi

    echo "${layout}: failed for the injected reason: $(has "${ERR}" 'Failpoint for a commit failure before the log write')"
    echo "${layout}: reported as unknown outcome: $(has "${ERR}" 'UNKNOWN_STATUS_OF_TRANSACTION')"
    echo "${layout}: data files removed: $(count_parquet "${T}") (expected 0)"
    echo "${layout}: commits in log: $(count_commits "${T}") (expected 1)"
    echo "${layout}: rows readable: $(read_rows "${T}") (expected [])"
done

echo
echo "==== control: an INSERT with no failpoint commits and reads back ===="
bootstrap "${ROOT}/ok" "${SCHEMA_PLAIN}" '[]'
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/ok') SELECT number::Int32 AS c0 FROM numbers(6);
"
echo "control: sum(c0): $(read_sum "${ROOT}/ok") (expected 15)"
echo "control: data files: $(count_parquet "${ROOT}/ok") (expected 1)"
echo "control: commits in log: $(count_commits "${ROOT}/ok") (expected 2)"
