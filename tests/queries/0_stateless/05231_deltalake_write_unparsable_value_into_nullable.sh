#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WRITE="SET allow_delta_kernel_rs = 1; SET allow_delta_lake_writes = 1; SET allow_delta_lake_create_table = 1;"

SCALAR_PATH="${CLICKHOUSE_USER_FILES_UNIQUE}_scalar"
ARRAY_PATH="${CLICKHOUSE_USER_FILES_UNIQUE}_array"
PART_PATH="${CLICKHOUSE_USER_FILES_UNIQUE}_partitioned"
rm -rf "$SCALAR_PATH" "$ARRAY_PATH" "$PART_PATH"

# Create each Delta write schema, then drop the table so only `_delta_log` remains. Re-reading it through
# the table function with a `String` structure makes the source column text while the write schema stays
# integer, so an INSERT casts String -> integer the way the reported bug does.
$CLICKHOUSE_CLIENT --query "
$WRITE
DROP TABLE IF EXISTS t_dl_scalar;
DROP TABLE IF EXISTS t_dl_array;
CREATE TABLE t_dl_scalar (id Int32, i Nullable(Int32)) ENGINE = DeltaLakeLocal('${SCALAR_PATH}', Parquet);
CREATE TABLE t_dl_array (id Int32, a Array(Nullable(Int32))) ENGINE = DeltaLakeLocal('${ARRAY_PATH}', Parquet);
DROP TABLE t_dl_scalar;
DROP TABLE t_dl_array;
"

insert() { # insert <path> <structure> <values> [extra settings]
    $CLICKHOUSE_CLIENT --query "
    $WRITE ${4:-}
    INSERT INTO TABLE FUNCTION deltaLakeLocal('$1', 'Parquet', '$2') VALUES $3;
    " 2>&1
}

expect_error() { # expect_error <label> <code> <path> <structure> <values> [extra settings]
    local out
    out=$(insert "$3" "$4" "$5" "${6:-}")
    if printf '%s' "$out" | grep -qF "($2)"; then echo "$1: $2"; else echo "$1: NOT REJECTED"; fi
}

read_back() { $CLICKHOUSE_CLIENT --query "$WRITE SELECT $2 FROM deltaLakeLocal('$1') $3"; }

expect_error 'a) unparsable String' CANNOT_PARSE_TEXT "$SCALAR_PATH" 'id Int32, i String' "(1, 'not a number')"
echo -n 'a) rows committed: '; read_back "$SCALAR_PATH" 'count()'

expect_error 'b) String out of range of the column' CANNOT_PARSE_TEXT "$SCALAR_PATH" 'id Int32, i String' "(2, '999999999999')"
echo -n 'b) rows committed: '; read_back "$SCALAR_PATH" 'count()'

echo 'c) a genuine NULL still round-trips:'
insert "$SCALAR_PATH" 'id Int32, i Nullable(String)' '(3, NULL)' > /dev/null
read_back "$SCALAR_PATH" 'id, i' 'WHERE id = 3'

expect_error 'd) mixed rows, one unparsable' CANNOT_PARSE_TEXT "$SCALAR_PATH" 'id Int32, i Nullable(String)' "(4, '7'), (5, NULL), (6, 'zzz')"
echo 'd) the same rows without it keep a source NULL apart from a cast NULL:'
insert "$SCALAR_PATH" 'id Int32, i Nullable(String)' "(4, '7'), (5, NULL)" > /dev/null
read_back "$SCALAR_PATH" 'id, i' 'WHERE id IN (4, 5) ORDER BY id'

echo 'h) a value the column can represent is committed:'
insert "$SCALAR_PATH" 'id Int32, i String' "(9, '42')" > /dev/null
read_back "$SCALAR_PATH" 'id, i' 'WHERE id = 9'

echo 'i) with delta_lake_accurate_write_cast = 0 the plain cast still commits NULL:'
insert "$SCALAR_PATH" 'id Int32, i String' "(10, 'not a number')" 'SET delta_lake_accurate_write_cast = 0;' > /dev/null
read_back "$SCALAR_PATH" 'id, i' 'WHERE id = 10'

expect_error 'j) numeric value out of range, as before' CANNOT_CONVERT_TYPE "$SCALAR_PATH" 'id Int32, i Int64' '(11, 999999999999)'

expect_error 'e) unparsable array element' CANNOT_PARSE_TEXT "$ARRAY_PATH" 'id Int32, a Array(String)' "(7, ['5', 'zz'])"
echo 'f) a NULL array element still round-trips:'
insert "$ARRAY_PATH" 'id Int32, a Array(Nullable(String))' "(8, ['5', NULL])" > /dev/null
read_back "$ARRAY_PATH" 'id, a' 'WHERE id = 8'

# The partition-key carrier. `CREATE TABLE ... ENGINE = DeltaLakeLocal` rejects PARTITION BY, so the log
# of a partitioned table is written directly.
mkdir -p "${PART_PATH}/_delta_log"
cat > "${PART_PATH}/_delta_log/00000000000000000000.json" <<'JSON'
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"p","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"p\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"x\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["p"],"configuration":{},"createdTime":1700000000000}}
JSON

expect_error 'g) unparsable partition key' CANNOT_PARSE_TEXT "$PART_PATH" 'p String, x String' "('zz', 'a')"
echo -n 'g) partition directories created: '; find "${PART_PATH}" -maxdepth 1 -name 'p=*' | wc -l
echo -n 'g) log versions: '; find "${PART_PATH}/_delta_log" -name '*.json' | wc -l
echo 'g) a partition key the column can represent, and a NULL one, are committed:'
insert "$PART_PATH" 'p String, x String' "('7', 'a')" > /dev/null
insert "$PART_PATH" 'p Nullable(String), x String' "(NULL, 'b')" > /dev/null
read_back "$PART_PATH" 'p, x' 'ORDER BY x'

rm -rf "$SCALAR_PATH" "$ARRAY_PATH" "$PART_PATH"
