#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: the XGBoost contrib is not built in the fast test.
# Tag no-replicated-database: the dump of a `Replicated` database names macros that the replaying `clickhouse-local` does not have.

# --dump-schema replays function aliases, executed constraint joins, dictionary source queries, TTL dictionaries and
# legacy aggregate states, and refuses a stored table-function argument that would load a dictionary.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# --dump-schema refuses input on stdin, whatever the test runner passes.
exec < /dev/null

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$work"
mkdir -p "$work"
trap 'rm -rf "$work"' EXIT
cd "$work" || exit 1

# Creates the objects in a fresh source, dumps database `d`, and replays the dump into a fresh target.
# A MergeTree TTL reads `enable_xgboost` from the server's own settings, so every instance gets it.
round_trip() {
    local name=$1 ddl=$2
    rm -rf "$work/src" "$work/dst"
    $CLICKHOUSE_LOCAL --path "$work/src" --enable_xgboost=1 -n -q "$ddl"
    $CLICKHOUSE_LOCAL --path "$work/src" --enable_xgboost=1 --dump-schema d > "$work/$name.sql" 2> "$work/$name.err"
    echo "$name: dump rc=$?"
    $CLICKHOUSE_LOCAL --path "$work/dst" --enable_xgboost=1 -n --queries-file "$work/$name.sql" > "$work/$name.out" 2>&1
    echo "$name: replay rc=$?"
    grep -o 'Code: [0-9]*' "$work/$name.out" | head -1
}

echo '--- a multiIf alias carries the lossy supertype gate ---'
round_trip alias "
CREATE DATABASE d;
SET allow_lossy_numeric_supertype = 1;
CREATE TABLE d.t (a UInt64, x Float64 DEFAULT arraySum(replicate(caseWithoutExpression(a > 0, toDecimal64(a, 2), 0.5), range(1))))
    ENGINE = Memory;"
grep -c '^SET allow_lossy_numeric_supertype = 1;' "$work/alias.sql"

echo '--- an eager dictionary comes after the table its source query reads through merge ---'
round_trip dictionary_source "
CREATE DATABASE d;
CREATE TABLE d.zzz_src (id UInt64, v UInt64) ENGINE = Memory;
CREATE DICTIONARY d.aaa_dict (id UInt64, v UInt64) PRIMARY KEY id
    SOURCE(CLICKHOUSE(QUERY 'SELECT id, v FROM merge(''d'', ''^zzz_src\$'')')) LAYOUT(FLAT()) LIFETIME(0)
    SETTINGS(dictionary_lazy_load = 0);"

echo '--- a table comes after the dictionary its TTL reads ---'
round_trip ttl "
SET enable_xgboost = 1;
SET allow_suspicious_ttl_expressions = 1;
CREATE DATABASE d;
CREATE TABLE d.train (x1 Float64, x2 Float64, y Float64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO d.train SELECT number, intDiv(number, 7), 2 * number + 3 * intDiv(number, 7) FROM numbers(100);
CREATE DICTIONARY d.zzz_model (x1 Float64, x2 Float64, y Float64) PRIMARY KEY (x1, x2)
    SOURCE(CLICKHOUSE(TABLE 'train' DB 'd')) LAYOUT(XGBOOST(num_iterations 10)) LIFETIME(0);
CREATE TABLE d.aaa_ttl (x1 Float64, x2 Float64) ENGINE = MergeTree ORDER BY x1
    TTL toDateTime(toUInt64(predictXGBoost('d.zzz_model', x1, x2)));"

echo '--- legacy aggregate states keep their version ---'
round_trip legacy_state "
CREATE DATABASE d;
CREATE TABLE d.t (s AggregateFunction(0, uniq, UInt64), n Array(AggregateFunction(0, uniq, UInt64)),
    c AggregateFunction(1, uniq, UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO d.t SELECT uniqState(number), [uniqState(number)], uniqState(toUInt64(number)) FROM numbers(10);"
query="SELECT name, type FROM system.columns WHERE database = 'd' AND table = 't' ORDER BY name"
[ "$($CLICKHOUSE_LOCAL --path "$work/src" -q "$query")" = "$($CLICKHOUSE_LOCAL --path "$work/dst" -q "$query")" ] \
    && echo 'column types match' || echo 'FAIL: column types differ'
$CLICKHOUSE_LOCAL --path "$work/src" -q "SELECT * FROM d.t FORMAT RowBinary" > "$work/states.bin"
$CLICKHOUSE_LOCAL --path "$work/dst" -q "INSERT INTO d.t FROM INFILE '$work/states.bin' FORMAT RowBinary;
    SELECT finalizeAggregation(s), finalizeAggregation(n[1]), finalizeAggregation(c) FROM d.t"

echo '--- a stored table-function argument that loads a dictionary is refused ---'
rm -rf "$work/src"
printf '1\thttp://127.0.0.1:1/x.tsv\n' > "$work/paths.tsv"
$CLICKHOUSE_LOCAL --path "$work/src" -n -q "
CREATE DATABASE d;
CREATE DICTIONARY d.paths (id UInt64, url String) PRIMARY KEY id SOURCE(FILE(path '$work/paths.tsv' format 'TSV'))
    LAYOUT(FLAT()) LIFETIME(0);
CREATE TABLE d.t (x UInt64) AS url(dictGetString('d.paths', 'url', toUInt64(1)), 'TSV', 'x UInt64');"
rm "$work/paths.tsv"
$CLICKHOUSE_LOCAL --path "$work/src" --dump-schema d > /dev/null 2> "$work/tf.err"
echo "dump rc=$?"
grep -o 'Code: 48' "$work/tf.err" | head -1
grep -c 'paths.tsv' "$work/tf.err"

echo '--- a Distributed sharding key loads its dictionary only when the call is constant ---'
cat > "$work/clusters.xml" <<'XML'
<clickhouse><remote_servers><c><shard><replica><host>localhost</host><port>9000</port></replica></shard></c></remote_servers></clickhouse>
XML
for key in "k" "toUInt64(1)"
do
    rm -rf "$work/src"
    printf '1\t5\n' > "$work/keys.tsv"
    $CLICKHOUSE_LOCAL --config-file "$work/clusters.xml" --path "$work/src" -n -q "
    CREATE DATABASE d;
    CREATE TABLE d.loc (k UInt64) ENGINE = Memory;
    CREATE DICTIONARY d.zzz_dict (id UInt64, v UInt64) PRIMARY KEY id SOURCE(FILE(path '$work/keys.tsv' format 'TSV'))
        LAYOUT(FLAT()) LIFETIME(0);
    CREATE TABLE d.aaa_dist (k UInt64) ENGINE = Distributed(c, d, loc, dictGetUInt64('d.zzz_dict', 'v', $key));"
    rm "$work/keys.tsv"
    $CLICKHOUSE_LOCAL --config-file "$work/clusters.xml" --path "$work/src" --dump-schema d > /dev/null 2> "$work/keys.err"
    echo "$key: dump rc=$?"
    grep -o 'Code: 48' "$work/keys.err" | head -1
    grep -c 'keys.tsv' "$work/keys.err"
done

echo '--- a JOIN in a constraint subquery carries the Dynamic key gate ---'
$CLICKHOUSE_CLIENT --allow_dynamic_type_in_join_keys=1 -n -q "
CREATE TABLE src (k Dynamic) ENGINE = Memory;
CREATE TABLE t (id UInt64, CONSTRAINT c CHECK id < (SELECT count() FROM src AS a INNER JOIN src AS b ON a.k = b.k)) ENGINE = Memory;"
$CLICKHOUSE_CLIENT --dump-schema "${CLICKHOUSE_DATABASE}" > "$work/join.sql" 2> "$work/join.err" || echo "dump failed: $(cat "$work/join.err")"
grep -c '^SET allow_dynamic_type_in_join_keys = 1;' "$work/join.sql"
$CLICKHOUSE_LOCAL --path "$work/join_dst" -n --queries-file "$work/join.sql" > "$work/join.out" 2>&1
echo "replay rc=$?"
grep -o 'Code: [0-9]*' "$work/join.out" | head -1
