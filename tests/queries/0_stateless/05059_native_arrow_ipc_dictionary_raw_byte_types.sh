#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the pyarrow Python module to build the dictionary<binary> files.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The ClickHouse Arrow writer stores UUID, IPv6 and the 128/256-bit integers as fixed_size_binary, and with
# `output_format_arrow_low_cardinality_as_dictionary` a `LowCardinality` column of such a type becomes an
# Arrow dictionary whose values are those raw bytes. Reading the file back must reinterpret the dictionary
# values as the requested type exactly like a flat column of raw bytes: the dictionary batch is decoded
# before any record batch, so the reader carries the requested type of the encoding field to it and declares
# the decoded column by the type the dictionary was actually decoded to. Otherwise the dictionary values stay
# `FixedString` and the cast text-parses the raw bytes, which fails for the big integers.
#
# Covers a top-level dictionary read as the plain type and as the `LowCardinality` type it was written as,
# a nullable dictionary, and dictionaries nested in Array/Tuple/Map (materialized rather than kept
# LowCardinality); then pyarrow-written `dictionary<binary>` values — variable binary, converted while the
# dictionary batch decodes — at the top level, inside a list, and extended by a delta dictionary in a stream.

PREFIX="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"

for FORMAT in Arrow ArrowStream; do
    FILE="${PREFIX}_lc.${FORMAT}"
    ${CLICKHOUSE_LOCAL} --allow_suspicious_low_cardinality_types=1 --query "
        INSERT INTO FUNCTION file('${FILE}', '${FORMAT}')
        SELECT
            toLowCardinality(toInt128(number) - 5) AS i128,
            toLowCardinality(toUInt128(number) * toUInt128('1000000000000000000000')) AS u128,
            toLowCardinality(-toInt256(number) * toInt256('1000000000000000000000000000000000000000')) AS i256,
            toLowCardinality(toUInt256(number) * toUInt256('1000000000000000000000000000000000000000')) AS u256,
            toLowCardinality(toUUID(concat('00000000-0000-0000-0000-00000000000', toString(number)))) AS uuid,
            toLowCardinality(toIPv6(concat('2001:db8::', toString(number)))) AS ip6,
            if(number = 1, NULL, toLowCardinality(toInt128(number)))::LowCardinality(Nullable(Int128)) AS ni128,
            [toLowCardinality(toInt128(number)), toLowCardinality(toInt128(-number))] AS arr,
            tuple(toLowCardinality(toUInt256(number)))::Tuple(v LowCardinality(UInt256)) AS tup,
            map(toString(number), toLowCardinality(toIPv6('::1'))) AS m
        FROM numbers(3)
        SETTINGS output_format_arrow_low_cardinality_as_dictionary = 1, engine_file_truncate_on_insert = 1"

    echo "--- ${FORMAT}: inferred schema, the natural uninterpreted types ---"
    ${CLICKHOUSE_LOCAL} --query "DESCRIBE file('${FILE}', '${FORMAT}')" | cut -f1,2
    echo "--- ${FORMAT}: read with the plain types ---"
    ${CLICKHOUSE_LOCAL} --query "
        SELECT * FROM file('${FILE}', '${FORMAT}',
            'i128 Int128, u128 UInt128, i256 Int256, u256 UInt256, uuid UUID, ip6 IPv6, ni128 Nullable(Int128),
             arr Array(Int128), tup Tuple(v UInt256), m Map(String, IPv6)')"
    echo "--- ${FORMAT}: read with the LowCardinality types it was written as ---"
    ${CLICKHOUSE_LOCAL} --allow_suspicious_low_cardinality_types=1 --query "
        SELECT * FROM file('${FILE}', '${FORMAT}',
            'i128 LowCardinality(Int128), u128 LowCardinality(UInt128), i256 LowCardinality(Int256), u256 LowCardinality(UInt256),
             uuid LowCardinality(UUID), ip6 LowCardinality(IPv6), ni128 LowCardinality(Nullable(Int128)),
             arr Array(LowCardinality(Int128)), tup Tuple(v LowCardinality(UInt256)), m Map(String, LowCardinality(IPv6))')"
    rm -f "${FILE}"

    python3 - "${PREFIX}" "${FORMAT}" <<'PY'
import sys
import pyarrow as pa
import pyarrow.ipc as ipc

prefix, fmt = sys.argv[1], sys.argv[2]
new_writer = ipc.new_file if fmt == "Arrow" else ipc.new_stream

def i128(v):
    return v.to_bytes(16, "little", signed=True)

values = pa.array([i128(12345), i128(-1), i128(67890)], type=pa.binary())
dict_col = pa.DictionaryArray.from_arrays(pa.array([0, 1, 2], type=pa.int32()), values)
list_col = pa.ListArray.from_arrays(
    pa.array([0, 2, 2, 4], type=pa.int32()),
    pa.DictionaryArray.from_arrays(pa.array([2, 0, 1, 1], type=pa.int32()), values))
schema = pa.schema([pa.field("d", dict_col.type), pa.field("l", list_col.type)])
with new_writer(f"{prefix}_bin.{fmt}", schema) as w:
    w.write_batch(pa.record_batch([dict_col, list_col], schema=schema))

if fmt == "ArrowStream":
    # Two batches whose dictionary grows: with deltas enabled the writer sends the new values as a delta
    # dictionary batch, which merges into the (already reinterpreted) base dictionary.
    v1 = pa.array([i128(1), i128(2)], type=pa.binary())
    v2 = pa.array([i128(1), i128(2), i128(3)], type=pa.binary())
    b1 = pa.record_batch([pa.DictionaryArray.from_arrays(pa.array([0, 1], type=pa.int32()), v1)], names=["d"])
    b2 = pa.record_batch([pa.DictionaryArray.from_arrays(pa.array([2, 0], type=pa.int32()), v2)], names=["d"])
    with ipc.new_stream(f"{prefix}_delta.{fmt}", b1.schema, options=ipc.IpcWriteOptions(emit_dictionary_deltas=True)) as w:
        w.write_batch(b1)
        w.write_batch(b2)
PY
    echo "--- ${FORMAT}: pyarrow dictionary<binary> read as Int128 / Array(Int128) ---"
    ${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${PREFIX}_bin.${FORMAT}', '${FORMAT}', 'd Int128, l Array(Int128)')"
    rm -f "${PREFIX}_bin.${FORMAT}"
    if [ "${FORMAT}" = "ArrowStream" ]; then
        echo "--- ${FORMAT}: delta dictionary read as Int128 ---"
        ${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${PREFIX}_delta.${FORMAT}', '${FORMAT}', 'd Int128') ORDER BY d"
        rm -f "${PREFIX}_delta.${FORMAT}"
    fi
done
