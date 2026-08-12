#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/114462
# A partition column that the table schema does not declare must raise a catchable
# error on both Delta readers, not abort the server on assert-enabled builds.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}"
rm -rf "$DIR"

# "good" declares the partition column 'p' in the schema, "bad" omits it.
DATA="p=1/part-00000-c000.snappy.parquet"
for T in good bad; do
    mkdir -p "$DIR/$T/_delta_log" "$DIR/$T/p=1"
    $CLICKHOUSE_LOCAL -q "
        INSERT INTO FUNCTION file('$DIR/$T/$DATA', Parquet, 'id Int64, s String')
        SELECT number, toString(number) FROM numbers(5)
        SETTINGS engine_file_truncate_on_insert = 1"
done

python3 - "$DIR" "$DATA" <<'EOF'
import json, os, sys

directory, data = sys.argv[1], sys.argv[2]
field = lambda name, type_: {"name": name, "type": type_, "nullable": True, "metadata": {}}
fields = {"good": [field("id", "long"), field("s", "string"), field("p", "string")],
          "bad": [field("id", "long"), field("s", "string")]}

for table, schema_fields in fields.items():
    table_dir = os.path.join(directory, table)
    actions = [
        {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
        {"metaData": {"id": "114462",
                      "format": {"provider": "parquet", "options": {}},
                      "schemaString": json.dumps({"type": "struct", "fields": schema_fields}),
                      "partitionColumns": ["p"],
                      "configuration": {}, "createdTime": 1600000000000}},
        {"add": {"path": data, "partitionValues": {"p": "1"},
                 "size": os.path.getsize(os.path.join(table_dir, data)),
                 "modificationTime": 1600000000000, "dataChange": True,
                 "stats": json.dumps({"numRecords": 5})}},
    ]
    with open(os.path.join(table_dir, "_delta_log", "00000000000000000000.json"), "w") as log:
        for action in actions:
            log.write(json.dumps(action) + "\n")

with open(os.path.join(directory, "bad_schema.json"), "w") as out:
    out.write(json.dumps({"type": "struct", "fields": fields["bad"]}))
EOF

# "ckpt" reaches the legacy reader's checkpoint branch instead of its JSON log branch:
# a _last_checkpoint makes the reader parse the checkpoint parquet, whose single row
# carries both the schema without 'p' and the add action keyed by 'p'.
BAD_SCHEMA=$(cat "$DIR/bad_schema.json")
mkdir -p "$DIR/ckpt/_delta_log" "$DIR/ckpt/p=1"
$CLICKHOUSE_LOCAL -q "
    INSERT INTO FUNCTION file('$DIR/ckpt/$DATA', Parquet, 'id Int64, s String')
    SELECT number, toString(number) FROM numbers(5)
    SETTINGS engine_file_truncate_on_insert = 1"
$CLICKHOUSE_LOCAL -q "
    INSERT INTO FUNCTION file('$DIR/ckpt/_delta_log/00000000000000000001.checkpoint.parquet', Parquet,
        'add Tuple(path Nullable(String), partitionValues Map(String, Nullable(String)),
                   size Nullable(Int64), modificationTime Nullable(Int64), dataChange Nullable(Bool)),
         metaData Tuple(schemaString Nullable(String))')
    SELECT ('$DATA', map('p', '1'), toInt64($(stat -c%s "$DIR/ckpt/$DATA")), toInt64(1600000000000), true),
           tuple('$BAD_SCHEMA')
    SETTINGS engine_file_truncate_on_insert = 1"
echo '{"version":1,"size":1}' > "$DIR/ckpt/_delta_log/_last_checkpoint"

echo '-- control: a declared partition column reads fine on both readers'
$CLICKHOUSE_LOCAL -q "SELECT id, s, p FROM deltaLakeLocal('$DIR/good') WHERE id > 3"
$CLICKHOUSE_LOCAL -q "SELECT id, s, p FROM deltaLakeLocal('$DIR/good') WHERE id > 3 SETTINGS allow_delta_kernel_rs = 0"

echo '-- delta-kernel reader: rejected with or without a predicate'
$CLICKHOUSE_LOCAL -q "SELECT * FROM deltaLakeLocal('$DIR/bad') FORMAT Null" 2>&1 | grep -oF "BAD_ARGUMENTS"
$CLICKHOUSE_LOCAL -q "SELECT * FROM deltaLakeLocal('$DIR/bad') WHERE id > 3 FORMAT Null" 2>&1 | grep -oF "BAD_ARGUMENTS"
$CLICKHOUSE_LOCAL -q "SELECT * FROM deltaLakeLocal('$DIR/bad') WHERE id > 3 FORMAT Null SETTINGS delta_lake_enable_engine_predicate = 0" 2>&1 | grep -oF "BAD_ARGUMENTS"

echo '-- delta-kernel reader: an explicit schema does not answer count() from statistics'
$CLICKHOUSE_LOCAL --multiquery "
CREATE TABLE t (\`id\` Int64, \`s\` String) ENGINE = DeltaLakeLocal('$DIR/bad');
SELECT count() FROM t;
" 2>&1 | grep -oF "BAD_ARGUMENTS"

echo '-- delta-kernel reader: no row or byte statistics are published either'
$CLICKHOUSE_LOCAL --multiquery "
CREATE TABLE t2 (\`id\` Int64, \`s\` String) ENGINE = DeltaLakeLocal('$DIR/bad');
SELECT total_rows IS NULL, total_bytes IS NULL FROM system.tables WHERE name = 't2';
" 2>&1 | grep -E "^1\s+1$|BAD_ARGUMENTS"

echo '-- legacy reader: json log branch'
$CLICKHOUSE_LOCAL -q "SELECT * FROM deltaLakeLocal('$DIR/bad') FORMAT Null SETTINGS allow_delta_kernel_rs = 0" 2>&1 | grep -oF "INCORRECT_DATA"

echo '-- legacy reader: checkpoint branch'
$CLICKHOUSE_LOCAL -q "SELECT * FROM deltaLakeLocal('$DIR/ckpt') FORMAT Null SETTINGS allow_delta_kernel_rs = 0" 2>&1 | grep -oF "INCORRECT_DATA"

rm -rf "$DIR"
