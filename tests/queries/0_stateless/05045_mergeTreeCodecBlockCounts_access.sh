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

    -- Explicit codecs, CI randomises the server-level default compression codec.
    CREATE TABLE t_codec_access (a UInt64 CODEC(LZ4), b UInt64 CODEC(LZ4))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0;

    INSERT INTO t_codec_access SELECT number, number FROM numbers(1000);

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

${CLICKHOUSE_CLIENT} -m --query "
    DROP USER ${username};
    DROP TABLE t_codec_access;
"
