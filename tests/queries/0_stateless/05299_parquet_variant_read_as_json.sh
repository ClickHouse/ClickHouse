#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format, which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A variant column is read as `JSON` when the requested type is `JSON`. Rows 3 and 4 are a variant
# null and a null of the whole group, both are read as an empty object.
DATA_FILE=$CUR_DIR/data_parquet/05299_variant_read_as_json.parquet

${CLICKHOUSE_LOCAL} --query "
    SELECT n, v, JSONAllPathsWithTypes(v)
    FROM file('${DATA_FILE}', Parquet, 'n Int64, v JSON')
    ORDER BY n
"

echo '--- typed path ---'
${CLICKHOUSE_LOCAL} --query "
    SELECT n, v.a, toTypeName(v.a), v.b.c, toTypeName(v.b.c)
    FROM file('${DATA_FILE}', Parquet, 'n Int64, v JSON(a Int64, b.c LowCardinality(String))')
    ORDER BY n
"

echo '--- skip ---'
${CLICKHOUSE_LOCAL} --query "
    SELECT n, v
    FROM file('${DATA_FILE}', Parquet, 'n Int64, v JSON(SKIP b, SKIP REGEXP \'^t\')')
    ORDER BY n
"

echo '--- shared data ---'
${CLICKHOUSE_LOCAL} --query "
    SELECT n, v, JSONDynamicPaths(v), JSONSharedDataPaths(v)
    FROM file('${DATA_FILE}', Parquet, 'n Int64, v JSON(max_dynamic_paths = 1)')
    ORDER BY n
"

echo '--- insert into a table ---'
${CLICKHOUSE_LOCAL} --query "
    CREATE TABLE t (n Int64, v JSON(a Int64)) ENGINE = Memory;
    INSERT INTO t SELECT * FROM file('${DATA_FILE}', Parquet);
    SELECT n, v.a, v.b.c FROM t ORDER BY n;
"

echo '--- Nullable(JSON): the variant null and the null group are NULL ---'
${CLICKHOUSE_LOCAL} --query "
    SELECT n, v, toTypeName(v), v.a, v.b.c
    FROM file('${DATA_FILE}', Parquet, 'n Int64, v Nullable(JSON(a Int64))')
    ORDER BY n
"
${CLICKHOUSE_LOCAL} --query "
    CREATE TABLE tn (n Int64, v Nullable(JSON(a Int64))) ENGINE = Memory;
    INSERT INTO tn SELECT * FROM file('${DATA_FILE}', Parquet);
    SELECT n, v, isNull(v) FROM tn ORDER BY n;
"
echo '--- schema inference still gives Dynamic ---'
${CLICKHOUSE_LOCAL} --query "DESCRIBE file('${DATA_FILE}', Parquet)"

echo '--- a non-object value ---'
${CLICKHOUSE_LOCAL} --query "
    SELECT * FROM file('${CUR_DIR}/data_parquet/04928_variant_spark.parquet', Parquet, 'id Int64, v JSON')
" 2>&1 | grep -o 'Cannot read Parquet variant column .v. as JSON: it contains a value of type Int32.*Read the column as Dynamic instead'
