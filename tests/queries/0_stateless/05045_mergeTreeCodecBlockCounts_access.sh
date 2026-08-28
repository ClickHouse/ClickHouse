#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Every column of `mergeTreeCodecBlockCounts` is derived from the source table's data, so reading any of
# them requires `SELECT` on all of its columns. Resolving the structure of the function requires the same.

username="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -m --query "
    DROP USER IF EXISTS ${username};
    DROP TABLE IF EXISTS t_codec_access;
    DROP TABLE IF EXISTS t_codec_access_log;

    -- Explicit codecs, CI randomises the server-level default compression codec.
    CREATE TABLE t_codec_access (a UInt64 CODEC(LZ4), b UInt64 CODEC(LZ4))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0;

    INSERT INTO t_codec_access SELECT number, number FROM numbers(1000);

    CREATE TABLE t_codec_access_log (a UInt64) ENGINE = Log;

    CREATE USER ${username} NOT IDENTIFIED;
"

echo "Without SELECT on the source table"
${CLICKHOUSE_CLIENT} --user="${username}" --query \
    "SELECT count() FROM mergeTreeCodecBlockCounts(currentDatabase(), t_codec_access);" 2>&1 |
    grep -o "ACCESS_DENIED" | uniq

echo "Without SELECT on the source table, structure only"
${CLICKHOUSE_CLIENT} --user="${username}" --query \
    "DESCRIBE mergeTreeCodecBlockCounts(currentDatabase(), t_codec_access);" 2>&1 |
    grep -o "ACCESS_DENIED" | uniq

echo "With SELECT on a single column of the source table"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT(a) ON t_codec_access TO ${username};"
${CLICKHOUSE_CLIENT} --user="${username}" --query \
    "SELECT count() FROM mergeTreeCodecBlockCounts(currentDatabase(), t_codec_access);" 2>&1 |
    grep -o "ACCESS_DENIED" | uniq
${CLICKHOUSE_CLIENT} --user="${username}" --query \
    "DESCRIBE mergeTreeCodecBlockCounts(currentDatabase(), t_codec_access);" 2>&1 |
    grep -o "ACCESS_DENIED" | uniq

echo "With SELECT on every column of the source table"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT(b) ON t_codec_access TO ${username};"
${CLICKHOUSE_CLIENT} --user="${username}" --query \
    "SELECT DISTINCT column, mapKeys(codec_block_counts) FROM mergeTreeCodecBlockCounts(currentDatabase(), t_codec_access) ORDER BY column;"
${CLICKHOUSE_CLIENT} --user="${username}" --query \
    "DESCRIBE mergeTreeCodecBlockCounts(currentDatabase(), t_codec_access) FORMAT TSV" | cut -f 1

# The engine of the source table is not something a user without `SELECT` on it is allowed to learn, so the
# access check has to run before the check that rejects a table of another engine with `BAD_ARGUMENTS`.

echo "Non-MergeTree source table, without SELECT on it"
${CLICKHOUSE_CLIENT} --user="${username}" --query \
    "DESCRIBE mergeTreeCodecBlockCounts(currentDatabase(), t_codec_access_log);" 2>&1 |
    grep -o "ACCESS_DENIED\|BAD_ARGUMENTS" | uniq

echo "Non-MergeTree source table, with SELECT on it"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON t_codec_access_log TO ${username};"
${CLICKHOUSE_CLIENT} --user="${username}" --query \
    "DESCRIBE mergeTreeCodecBlockCounts(currentDatabase(), t_codec_access_log);" 2>&1 |
    grep -o "ACCESS_DENIED\|BAD_ARGUMENTS" | uniq

${CLICKHOUSE_CLIENT} -m --query "
    DROP USER ${username};
    DROP TABLE t_codec_access;
    DROP TABLE t_codec_access_log;
"
