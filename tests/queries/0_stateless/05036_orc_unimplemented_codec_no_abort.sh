#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Truncating this fixture to 289 bytes leaves byte 6 in the postscript's compression field. Codec 6
# is BROTLI, which the ORC format declares but this reader does not implement, so the library throws
# orc::NotImplementedYet - a std::logic_error, which the exception formatter used to treat as an
# internal-invariant violation and abort on.
DATA_FILE="$CLICKHOUSE_TMP/05036_orc_codec6_${CLICKHOUSE_DATABASE}.orc"
head -c 289 "$CUR_DIR"/data_orc/orc_nested_union_type.orc > "$DATA_FILE"

# Both the schema-inference path and the explicit-structure path reach the reader, so both are
# asserted here. Match on the error text, never on the exit status: ClickHouse exits with the error
# code truncated to a byte, so 636 and 124 are the same outcome.
for query in \
    "SELECT * FROM file('$DATA_FILE', ORC) FORMAT Null" \
    "SELECT * FROM file('$DATA_FILE', ORC, 'x Int32') FORMAT Null" \
    "SELECT count() FROM file('$DATA_FILE', ORC, 'x Int32')" \
    "DESCRIBE file('$DATA_FILE', ORC)" \
    "SELECT * FROM format(ORC, 'x Int32', (SELECT * FROM file('$DATA_FILE', RawBLOB))) FORMAT Null"
do
    # An abort prints nothing at all, so classify explicitly rather than letting the arm go silent:
    # a missing line is much harder to read in a diff than a named verdict.
    OUT=$($CLICKHOUSE_LOCAL --query "$query" 2>&1)
    if [ -z "$OUT" ]; then
        echo "no output at all"
    else
        echo "$OUT" | grep -o -m1 'NOT_IMPLEMENTED\|Logical error\|Code: 1001' || echo "unexpected: $OUT"
    fi
done

# The message must not call the file corrupt: a well-formed Brotli-compressed ORC file takes this
# same path.
$CLICKHOUSE_LOCAL --query "SELECT * FROM file('$DATA_FILE', ORC, 'x Int32') FORMAT Null" 2>&1 \
    | grep -o -m1 'is not implemented\|CORRUPTED\|INCORRECT_DATA'

# Neighbouring truncations are genuinely corrupt and must keep reporting corruption. Schema
# inference wraps every non-retryable cause in one outer code, so that arm alone cannot tell
# corruption from a feature gap; the explicit-structure arm bypasses the wrapper and surfaces the
# inner classification, and asserting NOT_IMPLEMENTED is absent there pins the two apart.
for length in 200 400
do
    NEIGHBOUR="$CLICKHOUSE_TMP/05036_orc_${length}_${CLICKHOUSE_DATABASE}.orc"
    head -c $length "$CUR_DIR"/data_orc/orc_nested_union_type.orc > "$NEIGHBOUR"
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('$NEIGHBOUR', ORC) FORMAT Null" 2>&1 \
        | grep -o -m1 'CANNOT_EXTRACT_TABLE_STRUCTURE\|Logical error'
    EXPLICIT=$($CLICKHOUSE_LOCAL --query "SELECT * FROM file('$NEIGHBOUR', ORC, 'x Int32') FORMAT Null" 2>&1)
    echo "$EXPLICIT" | grep -o -m1 'orc::ParseError\|Logical error' || echo "unexpected: $EXPLICIT"
    echo "$EXPLICIT" | grep -c 'NOT_IMPLEMENTED'
    rm "$NEIGHBOUR"
done

# Well-formed ORC must still read back unchanged.
$CLICKHOUSE_LOCAL --query "SELECT i FROM file('$CUR_DIR/data_orc/orc_union_type.orc', ORC) ORDER BY i"

rm "$DATA_FILE"
