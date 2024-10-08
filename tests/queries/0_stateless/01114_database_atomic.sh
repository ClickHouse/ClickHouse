#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: 45 seconds running

# Creation of a database with Ordinary engine emits a warning.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATABASE_1="${CLICKHOUSE_DATABASE}_1"
DATABASE_2="${CLICKHOUSE_DATABASE}_2"
DATABASE_3="${CLICKHOUSE_DATABASE}_3"

$CLICKHOUSE_CLIENT --allow_deprecated_database_ordinary=0 -q "CREATE DATABASE ${DATABASE_1} ENGINE=Ordinary" 2>&1| grep -Fac "UNKNOWN_DATABASE_ENGINE"

$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${DATABASE_1} ENGINE=Atomic"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${DATABASE_2}"
$CLICKHOUSE_CLIENT --allow_deprecated_database_ordinary=1 -q "CREATE DATABASE ${DATABASE_3} ENGINE=Ordinary"

$CLICKHOUSE_CLIENT --show_table_uuid_in_table_create_query_if_not_nil=0 -q "SHOW CREATE DATABASE ${DATABASE_1}"
$CLICKHOUSE_CLIENT --show_table_uuid_in_table_create_query_if_not_nil=0 -q "SHOW CREATE DATABASE ${DATABASE_2}"
$CLICKHOUSE_CLIENT -q "SHOW CREATE DATABASE ${DATABASE_3}"

uuid_db_1=`$CLICKHOUSE_CLIENT -q "SELECT uuid FROM system.databases WHERE name='${DATABASE_1}'"`
uuid_db_2=`$CLICKHOUSE_CLIENT -q "SELECT uuid FROM system.databases WHERE name='${DATABASE_2}'"`
$CLICKHOUSE_CLIENT -q "SELECT name,
                              engine,
                              splitByChar('/', data_path)[-2],
                              splitByChar('/', metadata_path)[-2] as uuid_path, ((splitByChar('/', metadata_path)[-3] as metadata) = substr(uuid_path, 1, 3)) OR metadata='metadata'
                              FROM system.databases WHERE name LIKE '${CLICKHOUSE_DATABASE}_%'" | sed "s/$uuid_db_1/00001114-1000-4000-8000-000000000001/g" | sed "s/$uuid_db_2/00001114-1000-4000-8000-000000000002/g"

$CLICKHOUSE_CLIENT -m -q "
CREATE TABLE ${DATABASE_1}.mt_tmp (n UInt64) ENGINE=MergeTree() ORDER BY tuple();
INSERT INTO ${DATABASE_1}.mt_tmp SELECT * FROM numbers(100);
CREATE TABLE ${DATABASE_3}.mt (n UInt64) ENGINE=MergeTree() ORDER BY tuple() PARTITION BY (n % 5);
INSERT INTO ${DATABASE_3}.mt SELECT * FROM numbers(110);

RENAME TABLE ${DATABASE_1}.mt_tmp TO ${DATABASE_3}.mt_tmp; /* move from Atomic to Ordinary */
RENAME TABLE ${DATABASE_3}.mt TO ${DATABASE_1}.mt;         /* move from Ordinary to Atomic */
SELECT count() FROM ${DATABASE_1}.mt;
SELECT count() FROM ${DATABASE_3}.mt_tmp;

DROP DATABASE ${DATABASE_3};
"

explicit_uuid=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DATABASE_2}.mt UUID '$explicit_uuid' (n UInt64) ENGINE=MergeTree() ORDER BY tuple() PARTITION BY (n % 5)"
$CLICKHOUSE_CLIENT --show_table_uuid_in_table_create_query_if_not_nil=1 -q "SHOW CREATE TABLE ${DATABASE_2}.mt" | sed "s/$explicit_uuid/00001114-0000-4000-8000-000000000002/g"
$CLICKHOUSE_CLIENT -q "SELECT name, uuid, create_table_query FROM system.tables WHERE database='${DATABASE_2}'" | sed "s/$explicit_uuid/00001114-0000-4000-8000-000000000002/g"

RANDOM_COMMENT="$RANDOM"
$CLICKHOUSE_CLIENT --max-threads 5 --function_sleep_max_microseconds_per_block 120000000 -q "SELECT count(col), sum(col) FROM (SELECT n + sleepEachRow(3) AS col FROM ${DATABASE_1}.mt) -- ${RANDOM_COMMENT}" &     # 66s (3s * 22 rows per partition [Using 5 threads in parallel]), result: 110, 5995
$CLICKHOUSE_CLIENT --max-threads 5 --function_sleep_max_microseconds_per_block 120000000 -q "INSERT INTO ${DATABASE_2}.mt SELECT number + sleepEachRow(2.2) FROM numbers(30) -- ${RANDOM_COMMENT}" &                # 66s (2.2s * 30 rows)

it=0
while [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id != queryID() AND current_database = currentDatabase() AND query LIKE '%-- ${RANDOM_COMMENT}%'") -ne 2 ]]; do
    it=$((it+1))
    if [ $it -ge 50 ];
    then
        echo "Failed to wait for first batch of queries"
        $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id != queryID() AND current_database = currentDatabase() AND query LIKE '%-- ${RANDOM_COMMENT}%'"
    fi
    sleep 0.1
done

$CLICKHOUSE_CLIENT -m -q "
RENAME TABLE ${DATABASE_1}.mt TO ${DATABASE_1}.mt_tmp;
RENAME TABLE ${DATABASE_1}.mt_tmp TO ${DATABASE_2}.mt_tmp;
EXCHANGE TABLES ${DATABASE_2}.mt AND ${DATABASE_2}.mt_tmp;
RENAME TABLE ${DATABASE_2}.mt_tmp TO ${DATABASE_1}.mt;
EXCHANGE TABLES ${DATABASE_1}.mt AND ${DATABASE_2}.mt;
"

# Check that nothing changed
$CLICKHOUSE_CLIENT -q "SELECT count() FROM ${DATABASE_1}.mt"
uuid_mt1=$($CLICKHOUSE_CLIENT -q "SELECT uuid FROM system.tables WHERE database='${DATABASE_1}' AND name='mt'")
$CLICKHOUSE_CLIENT --show_table_uuid_in_table_create_query_if_not_nil=1 -q "SHOW CREATE TABLE ${DATABASE_1}.mt" | sed "s/$uuid_mt1/00001114-0000-4000-8000-000000000001/g"
$CLICKHOUSE_CLIENT --show_table_uuid_in_table_create_query_if_not_nil=1 -q "SHOW CREATE TABLE ${DATABASE_2}.mt" | sed "s/$explicit_uuid/00001114-0000-4000-8000-000000000002/g"

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE ${DATABASE_1}.mt SETTINGS database_atomic_wait_for_drop_and_detach_synchronously=0;
CREATE TABLE ${DATABASE_1}.mt (s String) ENGINE=Log();
INSERT INTO ${DATABASE_1}.mt SELECT 's' || toString(number) FROM numbers(5);
SELECT count() FROM ${DATABASE_1}.mt
" # result: 5

RANDOM_TUPLE="${RANDOM}_tuple"
$CLICKHOUSE_CLIENT --max-threads 5 --function_sleep_max_microseconds_per_block 60000000 -q "SELECT tuple(s, sleepEachRow(4)) FROM ${DATABASE_1}.mt -- ${RANDOM_TUPLE}" > /dev/null &    # 20s (4s * 5 rows)
it=0
while [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id != queryID() AND current_database = currentDatabase() AND query LIKE '%-- ${RANDOM_TUPLE}%'") -ne 1 ]]; do
    it=$((it+1))
    if [ $it -ge 50 ];
    then
        echo "Failed to wait for second batch of queries"
        $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id != queryID() AND current_database = currentDatabase() AND query LIKE '%-- ${RANDOM_TUPLE}%'"
    fi
    sleep 0.1
done
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${DATABASE_1}" --database_atomic_wait_for_drop_and_detach_synchronously=0 && echo "dropped"

wait # for INSERT and SELECT

$CLICKHOUSE_CLIENT -q "SELECT count(n), sum(n) FROM ${DATABASE_2}.mt"    # result: 30, 435
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${DATABASE_2}" --database_atomic_wait_for_drop_and_detach_synchronously=0
