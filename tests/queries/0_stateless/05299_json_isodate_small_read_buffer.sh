#!/usr/bin/env bash
# The `ISODate("...")` wrapper must parse the same whatever the read buffer splits: every row of a stream,
# including the delimiters right after the wrapper, has to survive a wrapper split across buffer boundaries.
# `format()` reads from a single in-memory buffer, so this needs `file()` with a small `max_read_buffer_size`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE=$CLICKHOUSE_TEST_UNIQUE_NAME.json
trap 'rm -f "$DATA_FILE"' EXIT

cat > "$DATA_FILE" <<'EOF'
{"id": 1, "t": ISODate("2024-05-29T23:16:12.256"), "n": new ISODate("2024-05-29T23:16:12.256Z"), "v": ISODate("2024-05-29T23:16:12.256")}
{"id": 2, "t": new ISODate("2024-05-29T23:16:12.256Z"), "n": null, "v": "plain string"}
{"id": 3, "t": ISODate("2024-05-29T23:16:12.256Z"),"n":ISODate("2024-05-29T23:16:12.256"),"v":new ISODate("2024-05-29T23:16:12.256Z")}
{"id": 4, "t": 1716938172.256, "n": "2024-05-29 23:16:12.256", "v": null}
EOF

STRUCTURE="id UInt8, t DateTime64(3, 'UTC'), n Nullable(DateTime64(3, 'UTC')), v Variant(DateTime64(3, 'UTC'), String)"
QUERY="SELECT id, t, n, v, variantType(v) FROM file('$DATA_FILE', 'JSONEachRow', \$\$$STRUCTURE\$\$) ORDER BY id"
SETTINGS="SETTINGS input_format_parallel_parsing = 0, storage_file_read_method = 'read'"

expected=$($CLICKHOUSE_LOCAL -q "$QUERY $SETTINGS")
echo "$expected"

for size in $(seq 1 16); do
    actual=$($CLICKHOUSE_LOCAL -q "$QUERY $SETTINGS, max_read_buffer_size = $size")
    [ "$actual" == "$expected" ] || echo "max_read_buffer_size = $size: got a different result"$'\n'"$actual"
done

# A malformed near-miss is rejected whatever the buffer size, instead of being read as the number 123.
echo '{"id": 1, "t": ISODate123}' > "$DATA_FILE"
for size in 1 2 3 4 5 8; do
    $CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA_FILE', 'JSONEachRow', 'id UInt8, t DateTime64(3)') $SETTINGS, max_read_buffer_size = $size" 2>&1 \
        | grep -q CANNOT_PARSE_INPUT_ASSERTION_FAILED || echo "max_read_buffer_size = $size: not rejected"
done
