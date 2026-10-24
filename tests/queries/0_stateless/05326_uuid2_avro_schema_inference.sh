#!/usr/bin/env bash
# Tags: no-fasttest
# Avro has a single `uuid` logical type for both `UUID` and `UUID2`, so the writer records the paths of the
# `UUID2` values in the file metadata, and schema inference reads them back as `UUID2` without an explicit structure.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "
    SELECT
        '61f0c404-5cb3-11e7-907b-a6006ad3dba0'::UUID2 AS x,
        toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') AS y,
        [toNullable(x)] AS a,
        tuple(x, y)::Tuple(p UUID2, q UUID) AS t,
        map('k', x) AS m,
        toNullable(x) AS n
    FORMAT Avro" | $CLICKHOUSE_LOCAL --input-format Avro --table t -q "DESCRIBE TABLE t SETTINGS describe_compact_output = 1; SELECT * FROM t"

echo "-- Variant alternatives that differ only in UUID and UUID2"
$CLICKHOUSE_LOCAL -q "
    SELECT number = 0
        ? tuple('61f0c404-5cb3-11e7-907b-a6006ad3dba0'::UUID2)::Tuple(x UUID2)::Variant(Tuple(x UUID), Tuple(x UUID2))
        : tuple(toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'))::Tuple(x UUID)::Variant(Tuple(x UUID), Tuple(x UUID2)) AS v
    FROM numbers(2)
    FORMAT Avro SETTINGS allow_suspicious_variant_types = 1" \
    | $CLICKHOUSE_LOCAL --input-format Avro --table t -q "DESCRIBE TABLE t SETTINGS describe_compact_output = 1; SELECT v, variantType(v) FROM t"

$CLICKHOUSE_LOCAL -q "
    SELECT number = 0
        ? toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')::Variant(UUID, Array(UUID2))
        : ['61f0c404-5cb3-11e7-907b-a6006ad3dba0'::UUID2]::Variant(UUID, Array(UUID2)) AS v
    FROM numbers(2)
    FORMAT Avro SETTINGS allow_suspicious_variant_types = 1" \
    | $CLICKHOUSE_LOCAL --input-format Avro --table t -q "DESCRIBE TABLE t SETTINGS describe_compact_output = 1; SELECT v, variantType(v) FROM t"

echo "-- without UUID2 values"
$CLICKHOUSE_LOCAL -q "SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') AS y FORMAT Avro" \
    | $CLICKHOUSE_LOCAL --input-format Avro --table t -q "DESCRIBE TABLE t SETTINGS describe_compact_output = 1; SELECT * FROM t"
