#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A column whose type has no Arrow mapping is written as an opaque column per
# `output_format_arrow_unsupported_types`, tagged `clickhouse.opaque` with the ClickHouse type it came from,
# and typed `Utf8` for the text form or `Binary` for the binary one. Reading it back into that same type has
# to undo whichever was written: the text form parses, and the binary form needs `deserializeBinary`, which
# is what the tag and the Arrow type together tell the reader to use.
#
# Without that, `binary` - the default - was not readable at all. `JSON` and `QBit` failed to parse, and a
# `Dynamic` silently came back holding the encoding's own type tag and length prefix as part of its value.

FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
COMMON="output_format_arrow_compression_method = 'none', engine_file_truncate_on_insert = 1"
TYPES="j JSON, d Dynamic, q QBit(BFloat16, 3), s AggregateFunction(sum, UInt64)"

values() {
    echo "SELECT '{\"a\":1,\"b\":\"s\"}'::JSON AS j,
                 42::Dynamic AS d,
                 [1,2,3]::QBit(BFloat16, 3) AS q,
                 (SELECT sumState(toUInt64(77))) AS s"
}
# `hex` on the `Dynamic` as well, because its corruption was invisible in the printed value: the two extra
# bytes are not printable.
read_back() {
    echo "SELECT '$1' AS mode, j, dynamicType(d) AS dynamic_type, hex(CAST(d AS String)) AS dynamic_hex,
                 q, finalizeAggregation(s) AS aggregate
          FROM file('$2', 'ArrowStream', '${TYPES}') FORMAT Vertical;"
}

echo "=== written as binary and as text, read back into the same types ==="
${CLICKHOUSE_LOCAL} --multiquery --query "
    INSERT INTO FUNCTION file('${FILE}.binary', 'ArrowStream') $(values)
        SETTINGS ${COMMON}, output_format_arrow_unsupported_types = 'binary';
    INSERT INTO FUNCTION file('${FILE}.text', 'ArrowStream') $(values)
        SETTINGS ${COMMON}, output_format_arrow_unsupported_types = 'text';
    $(read_back binary "${FILE}.binary")
    $(read_back text "${FILE}.text")
    SELECT 'original' AS mode, j, dynamicType(d) AS dynamic_type, hex(CAST(d AS String)) AS dynamic_hex,
           q, finalizeAggregation(s) AS aggregate
    FROM ($(values)) FORMAT Vertical;
    -- Without a requested type the tag is not acted on, so the payload still arrives as the raw bytes.
    SELECT 'as String' AS mode, hex(q) AS qbit FROM file('${FILE}.binary', 'ArrowStream', 'q String');"

# The tag sits on the field that owns the type, so for a container it is the child that carries it, and the
# rewrite has to find it there. A `Nullable` root keeps its null map across the rewrite; the second row is
# NULL to hold that.
WRAPPED="nj Nullable(JSON), nq Nullable(QBit(BFloat16, 3)), aj Array(JSON), tj Tuple(j JSON), mj Map(String, JSON)"

wrapped() {
    echo "SELECT if(number = 0, CAST('{\"a\":1}'::JSON, 'Nullable(JSON)'), NULL) AS nj,
                 if(number = 0, CAST([1,2,3]::QBit(BFloat16, 3), 'Nullable(QBit(BFloat16, 3))'), NULL) AS nq,
                 ['{\"b\":2}','{\"c\":3}']::Array(JSON) AS aj,
                 tuple('{\"d\":4}'::JSON)::Tuple(j JSON) AS tj,
                 CAST(map('k', '{\"e\":5}'), 'Map(String, JSON)') AS mj
          FROM numbers(2)"
}

echo "=== nullable and nested, written as binary and as text ==="
${CLICKHOUSE_LOCAL} --multiquery --query "
    INSERT INTO FUNCTION file('${FILE}.wbin', 'ArrowStream') $(wrapped)
        SETTINGS ${COMMON}, output_format_arrow_unsupported_types = 'binary';
    INSERT INTO FUNCTION file('${FILE}.wtxt', 'ArrowStream') $(wrapped)
        SETTINGS ${COMMON}, output_format_arrow_unsupported_types = 'text';
    SELECT 'binary' AS mode, * FROM file('${FILE}.wbin', 'ArrowStream', '${WRAPPED}') FORMAT Vertical;
    SELECT 'text' AS mode, * FROM file('${FILE}.wtxt', 'ArrowStream', '${WRAPPED}') FORMAT Vertical;
    SELECT 'original' AS mode, * FROM ($(wrapped)) FORMAT Vertical;"

rm -f "${FILE}".*
