#!/usr/bin/env bash
# Mixed-version `Native` compatibility for automatic `LowCardinality` serialization.
#
# Automatic `LowCardinality` serialization must be invisible on the wire: a column declared as
# `String` is dictionary-encoded on disk, but every reader - of any protocol revision - has to see
# exactly the bytes of a plain, unencoded `String` column. In particular a peer older than
# `DBMS_MIN_REVISION_WITH_AUTOMATIC_LOW_CARDINALITY_SERIALIZATION` (54493) does not understand the
# `LOW_CARDINALITY` entry of the serialization kind stack, so it must never be emitted to it.
#
# The complementary unit test `gtest_native_automatic_low_cardinality` covers the writer fallback
# in `NativeWriter::getSerializationAndColumn` directly, for a column that is still encoded in
# memory when it reaches the writer.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_statistics 1 --materialize_statistics_on_insert 1 --multiquery "
DROP TABLE IF EXISTS t_native_lc;
DROP TABLE IF EXISTS t_native_plain;
DROP TABLE IF EXISTS t_native_read;

CREATE TABLE t_native_lc (id UInt64, s String STATISTICS(uniq)) ENGINE = MergeTree ORDER BY id
SETTINGS max_uniq_number_for_low_cardinality = 1000, ratio_of_defaults_for_sparse_serialization = 1.0, min_bytes_for_wide_part = 0;

CREATE TABLE t_native_plain (id UInt64, s String) ENGINE = MergeTree ORDER BY id
SETTINGS max_uniq_number_for_low_cardinality = 0, ratio_of_defaults_for_sparse_serialization = 1.0, min_bytes_for_wide_part = 0;

CREATE TABLE t_native_read (id UInt64, s String) ENGINE = MergeTree ORDER BY id
SETTINGS max_uniq_number_for_low_cardinality = 0, ratio_of_defaults_for_sparse_serialization = 1.0, min_bytes_for_wide_part = 0;

INSERT INTO t_native_lc SELECT number, 'val_' || toString(number % 10) FROM numbers(10000);
INSERT INTO t_native_plain SELECT number, 'val_' || toString(number % 10) FROM numbers(10000);
"

echo 'serialization kinds'
${CLICKHOUSE_CLIENT} -q "
SELECT table, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table IN ('t_native_lc', 't_native_plain') AND active AND column = 's'
ORDER BY table"

# Reading without `ORDER BY` and with a single thread keeps the block layout of the two tables
# identical, so the dumps may be compared byte by byte.
dump()
{
    local table=$1
    local revision=$2
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&client_protocol_version=${revision}&max_threads=1&max_block_size=1000000" \
        --data-binary "SELECT id, s FROM ${table} FORMAT Native"
}

compare()
{
    local revision=$1
    dump t_native_lc "$revision" > "${CLICKHOUSE_TMP}/auto_lc_${revision}.native"
    dump t_native_plain "$revision" > "${CLICKHOUSE_TMP}/plain_${revision}.native"
    if cmp -s "${CLICKHOUSE_TMP}/auto_lc_${revision}.native" "${CLICKHOUSE_TMP}/plain_${revision}.native"
    then echo "revision ${revision}: identical to plain"
    else echo "revision ${revision}: differs from plain"
    fi
}

compare 0
compare 54492
compare 54493

# A revision-0 dump is exactly what a `Native` reader without custom serialization support expects,
# so it can be read back to prove the materialized fallback is not only identical, but also correct.
echo 'revision 0 round-trip'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+t_native_read+FORMAT+Native" \
    --data-binary @"${CLICKHOUSE_TMP}/auto_lc_0.native"
${CLICKHOUSE_CLIENT} -q "SELECT count(), uniqExact(s), sum(cityHash64(id, s)) FROM t_native_read"
${CLICKHOUSE_CLIENT} -q "SELECT count(), uniqExact(s), sum(cityHash64(id, s)) FROM t_native_lc"

rm -f "${CLICKHOUSE_TMP}"/auto_lc_*.native "${CLICKHOUSE_TMP}"/plain_*.native

${CLICKHOUSE_CLIENT} --multiquery "
DROP TABLE t_native_lc;
DROP TABLE t_native_plain;
DROP TABLE t_native_read;
"
