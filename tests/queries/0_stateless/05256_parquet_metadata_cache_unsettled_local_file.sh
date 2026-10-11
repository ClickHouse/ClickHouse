#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs Parquet

# The Parquet footer cache keys a local file by its version token (sub-second mtime + inode +
# size). The token proves a rewrite only once the file has settled (`isFileCacheVersionSettled`):
# a rewrite that keeps the inode and the size and lands in the same filesystem timestamp tick as
# the previous write produces the same token. A footer cached while the token was unsettled must
# therefore never be served once it has settled, because settled-token reads are trusted by the
# query condition cache, which may then skip the whole file without opening it.
#
# The test reproduces such a same-token rewrite: it writes generation A, reads it while the token
# is unsettled (so its footer lands in the cache), rewrites the file in place with generation B of
# exactly the same size and restores the previous mtime, so the token is unchanged. Once the token
# has settled, a read must see the footer of B, not the cached footer of A - whose statistics
# (values 0..99) would prune the row group holding B's values (1000..1099).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR_RELATIVE="${CLICKHOUSE_TEST_UNIQUE_NAME}"
DIR="${CLICKHOUSE_USER_FILES_UNIQUE:?}"
WRITE_SETTINGS="engine_file_truncate_on_insert = 1, output_format_parquet_compression_method = 'none'"
READ_SETTINGS="use_parquet_metadata_cache = 1, use_cache_for_count_from_files = 0, max_threads = 1"

${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION file('${DIR_RELATIVE}/a.parquet', Parquet, 'x UInt64') SELECT number FROM numbers(100) SETTINGS ${WRITE_SETTINGS}"
${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION file('${DIR_RELATIVE}/b.parquet', Parquet, 'x UInt64') SELECT number + 1000 FROM numbers(100) SETTINGS ${WRITE_SETTINGS}"

# The rewrite must keep the size, or the token changes anyway and there is nothing to test.
[ "$(stat -c %s "${DIR}/a.parquet")" = "$(stat -c %s "${DIR}/b.parquet")" ] && echo "same size"

cp "${DIR}/a.parquet" "${DIR}/data.parquet"
# An mtime in the future keeps the token unsettled for the first read however slow the machine is.
SETTLED_AT=$(( $(date +%s) + 15 ))
touch -d "@${SETTLED_AT}" "${DIR}/data.parquet"
touch -r "${DIR}/data.parquet" "${DIR}/mtime.ref"
TOKEN_BEFORE=$(stat -c '%i %s %.9Y' "${DIR}/data.parquet")

# Generation A, read while the token is unsettled.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM file('${DIR_RELATIVE}/data.parquet', Parquet, 'x UInt64') WHERE x < 100 SETTINGS ${READ_SETTINGS}"

# Rewrite in place (same inode, same size) and restore the mtime: the token stays the same.
cat "${DIR}/b.parquet" > "${DIR}/data.parquet"
touch -r "${DIR}/mtime.ref" "${DIR}/data.parquet"
[ "${TOKEN_BEFORE}" = "$(stat -c '%i %s %.9Y' "${DIR}/data.parquet")" ] && echo "same token"

# Wait for the token to settle (`file_version_settle_seconds = 3`).
while [ "$(date +%s)" -lt $(( SETTLED_AT + 4 )) ]; do sleep 0.5; done

# Generation B, read with a settled token.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM file('${DIR_RELATIVE}/data.parquet', Parquet, 'x UInt64') WHERE x = 1050 SETTINGS ${READ_SETTINGS}"
${CLICKHOUSE_CLIENT} --query "SELECT min(x), max(x) FROM file('${DIR_RELATIVE}/data.parquet', Parquet, 'x UInt64') SETTINGS ${READ_SETTINGS}"

rm -r "${DIR:?}"
