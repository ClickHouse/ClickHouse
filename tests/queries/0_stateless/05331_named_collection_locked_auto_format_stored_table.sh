#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: S3, HDFS and AzureBlobStorage are not in the fast test build

# A table over a named collection with `format = 'auto' NOT OVERRIDABLE` stores the inferred format as an override.
# Loading the stored definition again accepts it; a definition the user writes keeps the lock.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

dir="${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$dir"

url="http://localhost:11111/test/data.csv"
columns="(id UInt64, v String)"

# Prints only the "Override not allowed" part of the error of a query that must fail.
function expect_locked()
{
    local path=$1 key=$2 query=$3
    if output=$(${CLICKHOUSE_LOCAL} --path="$path" --query "$query" 2>&1 < /dev/null); then
        echo "Expected a refusal: $query"
    fi
    echo "$output" | grep -o "Override not allowed for '$key'" | head -1
}

echo "-- locked 'auto': S3, HDFS and AzureBlobStorage"
${CLICKHOUSE_LOCAL} --path="$dir/main" --query "
    CREATE NAMED COLLECTION nl_s3 AS url = '$url', format = 'auto' NOT OVERRIDABLE;
    CREATE NAMED COLLECTION nl_hdfs AS url = 'hdfs://localhost:12222/data.csv', format = 'auto' NOT OVERRIDABLE;
    CREATE NAMED COLLECTION nl_azure AS connection_string = 'http://localhost:11111/devstoreaccount1?sig=X',
        container = 'cont', blob_path = 'data.csv', format = 'auto' NOT OVERRIDABLE;
    CREATE DATABASE d ENGINE = Atomic;
    CREATE TABLE d.s3 $columns ENGINE = S3(nl_s3);
    CREATE TABLE d.hdfs $columns ENGINE = HDFS(nl_hdfs);
    CREATE TABLE d.azure $columns ENGINE = AzureBlobStorage(nl_azure);
    SELECT name, engine_full LIKE '%format = \\'CSV\\'%' FROM system.tables WHERE database = 'd' ORDER BY name;
" < /dev/null

echo "-- the stored definitions load, also with a short ATTACH"
${CLICKHOUSE_LOCAL} --path="$dir/main" --query "
    SELECT name FROM system.tables WHERE database = 'd' ORDER BY name;
    DETACH TABLE d.s3;
    ATTACH TABLE d.s3;
    DETACH TABLE d.hdfs;
    ATTACH TABLE d.hdfs;
    DETACH TABLE d.azure;
    ATTACH TABLE d.azure;
    SELECT 'attached', name FROM system.tables WHERE database = 'd' ORDER BY name;
" < /dev/null

echo "-- a format written by the user is refused"
expect_locked "$dir/main" format "CREATE TABLE d.t1 $columns ENGINE = S3(nl_s3, format = 'TSV')"
expect_locked "$dir/main" format "CREATE TABLE d.t2 $columns ENGINE = S3(nl_s3, format = 'CSV')"
expect_locked "$dir/main" format "ATTACH TABLE d.t3 UUID '05331000-0000-0000-0000-000000000003' $columns ENGINE = S3(nl_s3, format = 'CSV')"

echo "-- a locked value that is not 'auto' stays locked on load"
${CLICKHOUSE_LOCAL} --path="$dir/locked_value" --query "
    CREATE NAMED COLLECTION nv AS url = '$url';
    CREATE DATABASE d ENGINE = Atomic;
    CREATE TABLE d.t $columns ENGINE = S3(nv, format = 'TSV');
    ALTER NAMED COLLECTION nv SET format = 'CSV' NOT OVERRIDABLE;
    SELECT 'created', count() FROM system.tables WHERE database = 'd';
" < /dev/null
expect_locked "$dir/locked_value" format "SELECT 'loaded'"

echo "-- a locked 'auto' of another key stays locked on load"
${CLICKHOUSE_LOCAL} --path="$dir/locked_key" --query "
    CREATE NAMED COLLECTION nk AS url = '$url';
    CREATE DATABASE d ENGINE = Atomic;
    CREATE TABLE d.t $columns ENGINE = S3(nk, compression_method = 'gzip');
    ALTER NAMED COLLECTION nk SET compression_method = 'auto' NOT OVERRIDABLE;
    SELECT 'created', count() FROM system.tables WHERE database = 'd';
" < /dev/null
expect_locked "$dir/locked_key" compression_method "SELECT 'loaded'"

echo "-- 'structure' locked after the CREATE"
${CLICKHOUSE_LOCAL} --path="$dir/structure" --query "
    CREATE NAMED COLLECTION ns AS url = '$url', format = 'CSV', structure = 'auto';
    CREATE DATABASE d ENGINE = Atomic;
    CREATE TABLE d.t $columns ENGINE = S3(ns, structure = 'id UInt64, v String');
    ALTER NAMED COLLECTION ns SET structure = 'auto' NOT OVERRIDABLE;
    SELECT 'created', count() FROM system.tables WHERE database = 'd';
" < /dev/null
${CLICKHOUSE_LOCAL} --path="$dir/structure" --query "
    DETACH TABLE d.t;
    ATTACH TABLE d.t;
    SELECT 'attached', name FROM system.tables WHERE database = 'd';
" < /dev/null
expect_locked "$dir/structure" structure "CREATE TABLE d.t2 $columns ENGINE = S3(ns, structure = 'id UInt64, v String')"

echo "-- 'auto' locked after the CREATE, url resolved through s3_base on load"
${CLICKHOUSE_LOCAL} --path="$dir/s3_base" --query "
    CREATE NAMED COLLECTION nb AS url = '$url', format = 'auto';
    CREATE DATABASE d ENGINE = Atomic;
    CREATE TABLE d.b $columns ENGINE = S3(nb);
    ALTER NAMED COLLECTION nb SET url = 'data.csv';
    ALTER NAMED COLLECTION nb SET format = 'auto' NOT OVERRIDABLE;
    SELECT 'created', count() FROM system.tables WHERE database = 'd';
" < /dev/null
${CLICKHOUSE_LOCAL} --path="$dir/s3_base" --s3_base='http://localhost:11111/test/' --query "
    SELECT 'loaded', name FROM system.tables WHERE database = 'd';
" < /dev/null

echo "-- 'auto' locked after the CREATE: URL and S3"
${CLICKHOUSE_LOCAL} --path="$dir/url" --query "
    CREATE NAMED COLLECTION nu_url AS url = '$url', format = 'auto';
    CREATE NAMED COLLECTION nu_s3 AS url = '$url', format = 'auto';
    CREATE DATABASE d ENGINE = Atomic;
    CREATE TABLE d.u $columns ENGINE = URL(nu_url);
    CREATE TABLE d.s $columns ENGINE = S3(nu_s3);
    SELECT name, engine_full LIKE '%format = \\'CSV\\'%' FROM system.tables WHERE database = 'd' ORDER BY name;
    ALTER NAMED COLLECTION nu_url SET format = 'auto' NOT OVERRIDABLE;
    ALTER NAMED COLLECTION nu_s3 SET format = 'auto' NOT OVERRIDABLE;
" < /dev/null
${CLICKHOUSE_LOCAL} --path="$dir/url" --query "
    SELECT name FROM system.tables WHERE database = 'd' ORDER BY name;
    DETACH TABLE d.u;
    ATTACH TABLE d.u;
    SELECT 'attached', name FROM system.tables WHERE database = 'd' AND name = 'u';
" < /dev/null
expect_locked "$dir/url" format "CREATE TABLE d.u2 $columns ENGINE = URL(nu_url)"

echo "-- 'auto' locked after the CREATE: URL dispatched to File"
mkdir -p "$dir/url_dispatch"
printf '1,a\n' > "$dir/url_dispatch/data.csv"
${CLICKHOUSE_LOCAL} --path="$dir/url_dispatch" --query "
    CREATE NAMED COLLECTION nu_file AS url = 'file://$dir/url_dispatch/data.csv', format = 'auto';
    CREATE DATABASE d ENGINE = Atomic;
    CREATE TABLE d.f $columns ENGINE = URL(nu_file);
    SELECT name, engine_full LIKE '%format = \\'CSV\\'%' FROM system.tables WHERE database = 'd' ORDER BY name;
    ALTER NAMED COLLECTION nu_file SET format = 'auto' NOT OVERRIDABLE;
" < /dev/null
${CLICKHOUSE_LOCAL} --path="$dir/url_dispatch" --query "
    SELECT * FROM d.f;
    DETACH TABLE d.f;
    ATTACH TABLE d.f;
    SELECT 'attached', * FROM d.f;
" < /dev/null

rm -rf "$dir"
