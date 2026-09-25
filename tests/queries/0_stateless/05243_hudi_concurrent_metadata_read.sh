#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-parallel: the failpoints are process-wide, so concurrent copies of this test would release each
# other's paused queries.

# Concurrent readers of one `Hudi` table. The metadata object of a data lake table is shared by the
# queries that run on it, and `HudiMetadata` filled its file list on first use with no
# synchronization: two queries that reached an unlisted object together both listed the files and
# raced on that list.
#
# The overlap is made deterministic with failpoints (they are hit only by `Hudi` tables):
#  1. query A publishes its metadata object and pauses before it takes the current object;
#  2. query B publishes a newer object and pauses inside the listing of its data files;
#  3. query A resumes and reaches the object of B while its file list is not filled yet.
# With the fix, A waits for the list of B and makes no listing of its own. Before the fix, A made its
# own listing concurrently with B, so the test checks that A made no `S3ListObjects` calls.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="05243_hudi_concurrent/${CLICKHOUSE_TEST_UNIQUE_NAME}"
TABLE="t_${CLICKHOUSE_DATABASE}"
QUERY_ID_PREFIX="${CLICKHOUSE_TEST_UNIQUE_NAME}_${RANDOM}"
OUTPUT_A="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_a.out"
OUTPUT_B="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_b.out"

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT hudi_pause_before_iterate"
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT hudi_pause_in_listing_data_files"
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${TABLE}"
    rm -f "${OUTPUT_A}" "${OUTPUT_B}"
}
trap cleanup EXIT

# A Hudi data file is `[FileId]_[FileWriteToken]_[Timestamp].[ext]`; the reader keeps the newest
# write token of every file id.
for file_id in 0000 0001 0002
do
    ${CLICKHOUSE_CLIENT} -q "
        INSERT INTO FUNCTION s3(s3_conn, filename='${TABLE_PATH}/${file_id}_1-0-1_20260101000000.parquet')
        SETTINGS s3_truncate_on_insert = 1
        SELECT number AS a FROM numbers(10)
    "
done

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS ${TABLE};
    CREATE TABLE ${TABLE} ENGINE = Hudi(s3_conn, filename='${TABLE_PATH}/')
"

# A query alone lists the data files of its own metadata object.
${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID_PREFIX}_solo" -q "SELECT count() FROM ${TABLE}"

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT hudi_pause_before_iterate"
${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID_PREFIX}_a" -q "SELECT count() FROM ${TABLE}" > "${OUTPUT_A}" 2>&1 &
pid_a=$!
${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT hudi_pause_before_iterate PAUSE"

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT hudi_pause_in_listing_data_files"
${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID_PREFIX}_b" -q "SELECT count() FROM ${TABLE}" > "${OUTPUT_B}" 2>&1 &
pid_b=$!
${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT hudi_pause_in_listing_data_files PAUSE"

${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT hudi_pause_before_iterate"

# Before the fix, A finishes with a listing of its own while B is still paused; with the fix, A waits
# for B. Wait for A to finish, but not for long, as with the fix it cannot finish before B does.
for _ in {1..50}
do
    kill -0 "${pid_a}" 2>/dev/null || break
    sleep 0.1
done

${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT hudi_pause_in_listing_data_files"
wait "${pid_a}" "${pid_b}"

echo "A: $(cat "${OUTPUT_A}")"
echo "B: $(cat "${OUTPUT_B}")"

${CLICKHOUSE_CLIENT} -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT replaceOne(query_id, '${QUERY_ID_PREFIX}_', '') AS query, ProfileEvents['S3ListObjects'] > 0 AS listed
    FROM system.query_log
    WHERE event_date >= yesterday() AND type = 'QueryFinish' AND startsWith(query_id, '${QUERY_ID_PREFIX}_')
    ORDER BY query
"
