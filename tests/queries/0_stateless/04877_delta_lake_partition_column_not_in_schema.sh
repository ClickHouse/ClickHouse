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

# "mapped" declares 'p' but stores it under a column-mapping physical name.
MAPPED_DATA="col_pppp=1/part-00000-c000.snappy.parquet"
mkdir -p "$DIR/mapped/_delta_log" "$DIR/mapped/col_pppp=1"
$CLICKHOUSE_LOCAL -q "
    INSERT INTO FUNCTION file('$DIR/mapped/$MAPPED_DATA', Parquet, 'col_iiii Int64')
    SELECT number FROM numbers(3)
    SETTINGS engine_file_truncate_on_insert = 1"

python3 - "$DIR" "$DATA" "$MAPPED_DATA" <<'EOF'
import json, os, sys

directory, data, mapped_data = sys.argv[1], sys.argv[2], sys.argv[3]
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

# "add_only" leaves partitionColumns empty, so the metaData check passes it and only the
# later partitionValues lookup can reject its unknown key.
add_only_dir = os.path.join(directory, "add_only")
os.makedirs(os.path.join(add_only_dir, "_delta_log"))
os.makedirs(os.path.join(add_only_dir, "p=1"))
os.link(os.path.join(directory, "bad", data), os.path.join(add_only_dir, data))
with open(os.path.join(add_only_dir, "_delta_log", "00000000000000000000.json"), "w") as log:
    for action in [
        {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
        {"metaData": {"id": "114462-add-only",
                      "format": {"provider": "parquet", "options": {}},
                      "schemaString": json.dumps({"type": "struct", "fields": fields["bad"]}),
                      "partitionColumns": [],
                      "configuration": {}, "createdTime": 1600000000000}},
        {"add": {"path": data, "partitionValues": {"p": "1"},
                 "size": os.path.getsize(os.path.join(add_only_dir, data)),
                 "modificationTime": 1600000000000, "dataChange": True,
                 "stats": json.dumps({"numRecords": 5})}},
    ]:
        log.write(json.dumps(action) + "\n")

# "empty" carries no add action at all, so only a metaData-time check can reject it.
os.makedirs(os.path.join(directory, "empty", "_delta_log"))
with open(os.path.join(directory, "empty", "_delta_log", "00000000000000000000.json"), "w") as log:
    for action in [
        {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
        {"metaData": {"id": "114462-empty",
                      "format": {"provider": "parquet", "options": {}},
                      "schemaString": json.dumps({"type": "struct", "fields": fields["bad"]}),
                      "partitionColumns": ["p"],
                      "configuration": {}, "createdTime": 1600000000000}},
    ]:
        log.write(json.dumps(action) + "\n")

# "mapped" is well formed: 'p' is declared, and column mapping stores it as 'col_pppp'.
# partitionColumns holds the logical name, so a check against physical names would
# wrongly reject this table.
mapped_dir = os.path.join(directory, "mapped")
mapped = lambda name, type_, physical, id_: {
    "name": name, "type": type_, "nullable": True,
    "metadata": {"delta.columnMapping.physicalName": physical, "delta.columnMapping.id": id_}}
with open(os.path.join(mapped_dir, "_delta_log", "00000000000000000000.json"), "w") as log:
    for action in [
        {"protocol": {"minReaderVersion": 2, "minWriterVersion": 5,
                      "readerFeatures": ["columnMapping"], "writerFeatures": ["columnMapping"]}},
        {"metaData": {"id": "114462-mapped",
                      "format": {"provider": "parquet", "options": {}},
                      "schemaString": json.dumps({"type": "struct", "fields": [
                          mapped("id", "long", "col_iiii", 1),
                          mapped("p", "string", "col_pppp", 2)]}),
                      "partitionColumns": ["p"],
                      "configuration": {"delta.columnMapping.mode": "name",
                                        "delta.columnMapping.maxColumnId": "2"},
                      "createdTime": 1600000000000}},
        {"add": {"path": mapped_data, "partitionValues": {"col_pppp": "1"},
                 "size": os.path.getsize(os.path.join(mapped_dir, mapped_data)),
                 "modificationTime": 1600000000000, "dataChange": True,
                 "stats": json.dumps({"numRecords": 3})}},
    ]:
        log.write(json.dumps(action) + "\n")
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

# "ckpt_empty" is the checkpoint counterpart of "empty": its single row carries the
# malformed metaData and no add action, so only a metaData-time check can reject it.
mkdir -p "$DIR/ckpt_empty/_delta_log"
$CLICKHOUSE_LOCAL -q "
    INSERT INTO FUNCTION file('$DIR/ckpt_empty/_delta_log/00000000000000000001.checkpoint.parquet', Parquet,
        'add Tuple(path Nullable(String), partitionValues Map(String, Nullable(String)),
                   size Nullable(Int64), modificationTime Nullable(Int64), dataChange Nullable(Bool)),
         metaData Tuple(schemaString Nullable(String), partitionColumns Array(Nullable(String)))')
    SELECT (NULL, map(), NULL, NULL, NULL), tuple('$BAD_SCHEMA', ['p'])
    SETTINGS engine_file_truncate_on_insert = 1"
echo '{"version":1,"size":1}' > "$DIR/ckpt_empty/_delta_log/_last_checkpoint"

echo '-- control: a declared partition column reads fine on both readers'
$CLICKHOUSE_LOCAL -q "SELECT id, s, p FROM deltaLakeLocal('$DIR/good') WHERE id > 3"
$CLICKHOUSE_LOCAL -q "SELECT id, s, p FROM deltaLakeLocal('$DIR/good') WHERE id > 3 SETTINGS allow_delta_kernel_rs = 0"

echo '-- control: a column-mapped partition column is declared under its logical name'
$CLICKHOUSE_LOCAL -q "SELECT sum(col_iiii), any(col_pppp) FROM deltaLakeLocal('$DIR/mapped') SETTINGS allow_delta_kernel_rs = 0"

echo '-- delta-kernel reader: rejected with or without a predicate'
$CLICKHOUSE_LOCAL -q "SELECT * FROM deltaLakeLocal('$DIR/bad') FORMAT Null" 2>&1 | grep -oF "BAD_ARGUMENTS"
$CLICKHOUSE_LOCAL -q "SELECT * FROM deltaLakeLocal('$DIR/bad') WHERE id > 3 FORMAT Null" 2>&1 | grep -oF "BAD_ARGUMENTS"
$CLICKHOUSE_LOCAL -q "SELECT * FROM deltaLakeLocal('$DIR/bad') WHERE id > 3 FORMAT Null SETTINGS delta_lake_enable_engine_predicate = 0" 2>&1 | grep -oF "BAD_ARGUMENTS"

echo '-- legacy reader: json log branch'
$CLICKHOUSE_LOCAL -q "SELECT * FROM deltaLakeLocal('$DIR/bad') FORMAT Null SETTINGS allow_delta_kernel_rs = 0" 2>&1 | grep -oF "INCORRECT_DATA"

echo '-- legacy reader: json log branch, unknown partitionValues key'
$CLICKHOUSE_LOCAL -q "SELECT * FROM deltaLakeLocal('$DIR/add_only') FORMAT Null SETTINGS allow_delta_kernel_rs = 0" 2>&1 | grep -oF "INCORRECT_DATA"

echo '-- legacy reader: checkpoint branch'
$CLICKHOUSE_LOCAL -q "SELECT * FROM deltaLakeLocal('$DIR/ckpt') FORMAT Null SETTINGS allow_delta_kernel_rs = 0" 2>&1 | grep -oF "INCORRECT_DATA"

echo '-- legacy reader: json log branch, partitionColumns with no add action to resolve'
$CLICKHOUSE_LOCAL -q "DESCRIBE TABLE deltaLakeLocal('$DIR/empty') SETTINGS allow_delta_kernel_rs = 0" 2>&1 | grep -oF "INCORRECT_DATA"

echo '-- legacy reader: checkpoint branch, partitionColumns with no add action to resolve'
$CLICKHOUSE_LOCAL -q "DESCRIBE TABLE deltaLakeLocal('$DIR/ckpt_empty') SETTINGS allow_delta_kernel_rs = 0" 2>&1 | grep -oF "INCORRECT_DATA"

rm -rf "$DIR"
