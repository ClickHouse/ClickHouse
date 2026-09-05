#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings, no-distributed-cache
# no-fasttest: reads from S3 (MinIO).
# no-random-settings / no-distributed-cache: the test asserts the read-ahead prefetch count,
# which depends on buffer sizes and the read method.

# The object-storage read path issues a from-start read-ahead prefetch for "small" objects. For
# column-oriented random-access formats (`Parquet`) - which read the `FileMetaData` footer at the
# tail first - that prefetch cannot reach the footer once the object is larger than one read
# buffer, so it is dropped: a wasted object read. The reader must gate it, prefetching only when
# the whole object fits a single read buffer.
#
# The query prunes every row group by footer min/max statistics (a predicate above the column
# maximum), so it answers from the footer alone and reads no data pages. That isolates the
# from-start prefetch as the only read-ahead that could fire, asserted via
# ProfileEvents['RemoteFSPrefetches']:
#   - big object   (> one read buffer)  -> prefetch suppressed        -> RemoteFSPrefetches = 0
#   - small object (<= one read buffer) -> whole-file prefetch fires  -> RemoteFSPrefetches >= 1

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

url="http://localhost:11111/test/${CLICKHOUSE_DATABASE}"

# Force the prefetch path deterministically:
#   - threadpool method + prefetch on, so the from-start prefetch is eligible.
#   - max_read_buffer_size = 1 MiB: the new gate prefetches a random-access object only when it
#     fits one read buffer, so the ~11 MiB "big" object is above it and must NOT prefetch.
#   - max_download_buffer_size = 10 MiB: the OLD heuristic prefetched objects up to
#     2 * max_download_buffer_size = 20 MiB, so the ~11 MiB object sits in the band where the old
#     code WOULD have prefetched - making RemoteFSPrefetches = 0 a proof of the gate, not just of
#     the object being too large for any prefetch.
read_settings="remote_filesystem_read_method='threadpool', remote_filesystem_read_prefetch=1, max_read_buffer_size=1048576, max_download_buffer_size=10485760, input_format_parquet_filter_push_down=1, optimize_count_from_files=0"

# big object: > 1 read buffer. The incompressible string column inflates it well past 1 MiB.
${CLICKHOUSE_CLIENT} --query "
INSERT INTO FUNCTION s3('${url}/big.parquet', 'test', 'testtest', 'Parquet')
SELECT number AS v, randomPrintableASCII(64) AS s FROM numbers(200000)
SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 50000
"

# small object: whole file fits one read buffer.
${CLICKHOUSE_CLIENT} --query "
INSERT INTO FUNCTION s3('${url}/small.parquet', 'test', 'testtest', 'Parquet')
SELECT number AS v, randomPrintableASCII(8) AS s FROM numbers(50)
SETTINGS s3_truncate_on_insert = 1
"

run() {
    local query_id=$1 file=$2 extra_settings=$3
    # The schema is passed explicitly on purpose: an inferred schema opens the object once more
    # through the schema-inference path (a separate read buffer), which would add its own prefetch
    # and mask the one under test. v is at most 199999, so the predicate prunes every row group by
    # footer statistics - the query answers from the footer alone, reading no data pages.
    ${CLICKHOUSE_CLIENT} --query_id "$query_id" --query "
    SELECT count() FROM s3('${url}/${file}', 'test', 'testtest', 'Parquet', 'v UInt64, s String') WHERE v > 100000000
    SETTINGS ${read_settings}${extra_settings:+, ${extra_settings}}
    "
}

qid_big="${CLICKHOUSE_DATABASE}_big"
qid_small="${CLICKHOUSE_DATABASE}_small"
qid_big_no_seeks="${CLICKHOUSE_DATABASE}_big_no_seeks"

run "$qid_big"   big.parquet
run "$qid_small" small.parquet
# The same big object with seeks disabled: the reader can no longer read the footer at the tail, it
# reads the object sequentially from the start, so the from-start prefetch must not be gated off.
run "$qid_big_no_seeks" big.parquet "input_format_allow_seeks=0"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

# big object -> from-start prefetch is gated off.
${CLICKHOUSE_CLIENT} --query "
SELECT ProfileEvents['RemoteFSPrefetches'] = 0
FROM system.query_log
WHERE current_database = currentDatabase() AND query_id = '${qid_big}' AND type = 'QueryFinish'
"

# small object -> whole-file prefetch still fires (and serves the footer from memory).
${CLICKHOUSE_CLIENT} --query "
SELECT ProfileEvents['RemoteFSPrefetches'] >= 1
FROM system.query_log
WHERE current_database = currentDatabase() AND query_id = '${qid_small}' AND type = 'QueryFinish'
"

# big object with input_format_allow_seeks=0 -> the read is sequential from the start, so the
# from-start prefetch is useful and must still fire.
${CLICKHOUSE_CLIENT} --query "
SELECT ProfileEvents['RemoteFSPrefetches'] >= 1
FROM system.query_log
WHERE current_database = currentDatabase() AND query_id = '${qid_big_no_seeks}' AND type = 'QueryFinish'
"
