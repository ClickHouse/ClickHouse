#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Use the process ID to keep temporary files unique, and remove them on failure.
TEMP_SQL_FILE_NAME="01523_client_local_queries_file_parameter_tmp_$$.sql"
SECOND_SQL_FILE_NAME="${TEMP_SQL_FILE_NAME}.second"
STDIN_DATA_FILE_NAME="${TEMP_SQL_FILE_NAME}.data"
SPACED_SQL_FILE_NAME="${TEMP_SQL_FILE_NAME}.space name.sql"
trap 'rm -f "$TEMP_SQL_FILE_NAME" "$SECOND_SQL_FILE_NAME" "$STDIN_DATA_FILE_NAME" "$SPACED_SQL_FILE_NAME"' EXIT
echo "SELECT 1;" > "$TEMP_SQL_FILE_NAME"
$CLICKHOUSE_CLIENT --queries-file="$TEMP_SQL_FILE_NAME" 2>&1

echo "CREATE TABLE 01523_test(value Int32) ENGINE=Log;
INSERT INTO 01523_test 
    VALUES (1), (2), (3);
SELECT * FROM 01523_test;
DROP TABLE 01523_test;" > "$TEMP_SQL_FILE_NAME"
$CLICKHOUSE_CLIENT --queries-file="$TEMP_SQL_FILE_NAME" 2>&1

echo "CREATE TABLE 01523_test (a Int64, b Int64) ENGINE = File(CSV, stdin);
SELECT a, b FROM 01523_test;
DROP TABLE 01523_test;" > "$TEMP_SQL_FILE_NAME"

printf '1,2\n3,4\n' | $CLICKHOUSE_LOCAL --queries-file="$TEMP_SQL_FILE_NAME" 2>&1

# clickhouse-local executes query files before --query. clickhouse-client still rejects the combination.
echo "SELECT 'from file';" > "$TEMP_SQL_FILE_NAME"
$CLICKHOUSE_LOCAL --queries-file="$TEMP_SQL_FILE_NAME" -q "SELECT 'from query';" < /dev/null
$CLICKHOUSE_CLIENT --queries-file="$TEMP_SQL_FILE_NAME" -q "SELECT 2;" 2>&1 | grep -o 'BAD_ARGUMENTS'
$CLICKHOUSE_LOCAL --queries-file="$TEMP_SQL_FILE_NAME" --query "SELECT 'from long query';" < /dev/null
$CLICKHOUSE_CLIENT --queries-file="$TEMP_SQL_FILE_NAME" --query "SELECT 4;" 2>&1 | grep -o 'BAD_ARGUMENTS'

# Multiple files run in order, and a SQL UDF created in a file is available to --query.
printf "%s\n" "CREATE FUNCTION f_01523 AS (x) -> x + 1;" "SELECT 'first file';" > "$TEMP_SQL_FILE_NAME"
printf "%s\n" "SELECT 'second file';" > "$SECOND_SQL_FILE_NAME"
$CLICKHOUSE_LOCAL --queries-file="$TEMP_SQL_FILE_NAME" --queries-file="$SECOND_SQL_FILE_NAME" -q "SELECT f_01523(41);" < /dev/null

# SQL from separate files shares a session, even if -q is specified first.
printf '%s\n' 'CREATE TABLE state_01523 (x UInt8) ENGINE=Memory;' 'INSERT INTO state_01523 VALUES (4);' > "$TEMP_SQL_FILE_NAME"
printf '%s\n' 'INSERT INTO state_01523 VALUES (5);' > "$SECOND_SQL_FILE_NAME"
$CLICKHOUSE_LOCAL --async_insert=0 -q 'SELECT sum(x) FROM state_01523' --queries-file="$TEMP_SQL_FILE_NAME" --queries-file="$SECOND_SQL_FILE_NAME" < /dev/null

# Query files and --query keep their own ordering, regardless of interleaving on the command line.
echo "SELECT 'first file';" > "$TEMP_SQL_FILE_NAME"
echo "SELECT 'second file';" > "$SECOND_SQL_FILE_NAME"
$CLICKHOUSE_LOCAL -q "SELECT 'first query';" --queries-file="$TEMP_SQL_FILE_NAME" -q "SELECT 'second query';" --queries-file="$SECOND_SQL_FILE_NAME" < /dev/null

# A single --queries-file flag accepts multiple paths; SET remains visible to -q.
printf '%s\n' 'SET max_threads = 2;' 'CREATE FUNCTION f_more_01523 AS (x) -> x * 3;' > "$TEMP_SQL_FILE_NAME"
echo 'SELECT f_more_01523(7) FROM system.one;' > "$SECOND_SQL_FILE_NAME"
$CLICKHOUSE_LOCAL -q "SELECT getSetting('max_threads'), f_more_01523(8) FROM system.one" \
    --queries-file "$TEMP_SQL_FILE_NAME" "$SECOND_SQL_FILE_NAME" < /dev/null

# Whitespace in a query-file path must not disturb command-line query execution.
echo "SELECT 'spaced path';" > "$SPACED_SQL_FILE_NAME"
$CLICKHOUSE_LOCAL -q "SELECT 'after spaced path'" --queries-file="$SPACED_SQL_FILE_NAME" < /dev/null

# A missing query file must fail instead of silently running the trailing query.
MISSING_SQL_FILE_NAME="${TEMP_SQL_FILE_NAME}.missing"
if output=$($CLICKHOUSE_LOCAL --queries-file="$MISSING_SQL_FILE_NAME" -q 'SELECT 126899 FROM system.one' < /dev/null 2>&1); then
    echo 'missing query file unexpectedly succeeded'
elif [[ "$output" == *"$MISSING_SQL_FILE_NAME"* ]] && ! grep -Fxq '126899' <<< "$output"; then
    echo 'missing query file stopped query'
else
    echo 'unexpected missing-file output'
fi

# Redirected stdin must not become an implicit table until after query files finish.
printf '1,2\n3,4\n5,6\n' > "$STDIN_DATA_FILE_NAME"
echo "SELECT count();" > "$TEMP_SQL_FILE_NAME"
$CLICKHOUSE_LOCAL --queries-file="$TEMP_SQL_FILE_NAME" < "$STDIN_DATA_FILE_NAME"
$CLICKHOUSE_LOCAL --queries-file="$TEMP_SQL_FILE_NAME" -q "SELECT count();" < "$STDIN_DATA_FILE_NAME"
$CLICKHOUSE_LOCAL -q "SELECT count();" --queries-file="$TEMP_SQL_FILE_NAME" < "$STDIN_DATA_FILE_NAME"

# Explicit input configuration makes the stdin table available to query files.
# The trailing query uses system.one so that the test does not depend on reading stdin twice.
$CLICKHOUSE_LOCAL --structure "a UInt8, b UInt8" --input-format CSV --queries-file="$TEMP_SQL_FILE_NAME" -q "SELECT 99 FROM system.one;" < "$STDIN_DATA_FILE_NAME"

# File-level settings must not affect the generated stdin-table CREATE.
printf '1\n2\n3\n' > "$STDIN_DATA_FILE_NAME"
echo 'SET readonly = 1;' > "$TEMP_SQL_FILE_NAME"
$CLICKHOUSE_LOCAL --queries-file="$TEMP_SQL_FILE_NAME" -q "SELECT count();" < "$STDIN_DATA_FILE_NAME"

printf '%s\n' 'SET allow_experimental_kusto_dialect = 1;' "SET dialect = 'kusto';" > "$TEMP_SQL_FILE_NAME"
$CLICKHOUSE_LOCAL --queries-file="$TEMP_SQL_FILE_NAME" -q "table | count" < "$STDIN_DATA_FILE_NAME"

# Inferred-table schema discovery must not consume stdin before query files read it.
# This matters with the read method, which otherwise advances the shared descriptor.
printf '1\n2\n3\n' > "$STDIN_DATA_FILE_NAME"
printf '%s\n' 'CREATE TABLE stdin_source (x UInt8) ENGINE=File(TSV, stdin);' 'SELECT sum(x) FROM stdin_source;' > "$TEMP_SQL_FILE_NAME"
$CLICKHOUSE_LOCAL --storage_file_read_method=read --queries-file="$TEMP_SQL_FILE_NAME" < "$STDIN_DATA_FILE_NAME"
$CLICKHOUSE_LOCAL --storage_file_read_method=read --queries-file="$TEMP_SQL_FILE_NAME" -q 'SELECT 42 FROM system.one' < "$STDIN_DATA_FILE_NAME"
$CLICKHOUSE_LOCAL --storage_file_read_method=read --queries-file="$TEMP_SQL_FILE_NAME" -q 'SELECT count()' < "$STDIN_DATA_FILE_NAME"

# The calling shell may have already consumed a prefix of regular-file stdin.
# Adding --queries-file must not reset the inferred table's input offset to zero.
echo "SELECT 'init' FROM system.one;" > "$TEMP_SQL_FILE_NAME"
{ IFS= read -r _; $CLICKHOUSE_LOCAL --storage_file_read_method=read -q 'SELECT count(), sum(c1)'; } < "$STDIN_DATA_FILE_NAME"
{ IFS= read -r _; $CLICKHOUSE_LOCAL --storage_file_read_method=read --queries-file="$TEMP_SQL_FILE_NAME" -q 'SELECT count(), sum(c1)'; } < "$STDIN_DATA_FILE_NAME"

# The inferred stdin table must not hide a user table called `table` in query files.
printf '1\n2\n3\n' > "$STDIN_DATA_FILE_NAME"
printf '%s\n' 'CREATE TABLE `table` (x UInt8) ENGINE=Memory;' \
    'SELECT count() FROM `table`;' \
    'TRUNCATE TABLE `table`;' \
    'SELECT count() FROM `table`;' > "$TEMP_SQL_FILE_NAME"
$CLICKHOUSE_LOCAL --queries-file="$TEMP_SQL_FILE_NAME" \
    -q 'SELECT count(); SELECT count() FROM `table`;' 0<>"$STDIN_DATA_FILE_NAME"
cat "$STDIN_DATA_FILE_NAME"

# Query-file temporary tables also retain their own namespace.
printf '%s\n' 'CREATE TEMPORARY TABLE `table` (x UInt8) ENGINE=Memory;' \
    'INSERT INTO `table` SELECT 7;' \
    'SELECT sum(x) FROM `table`;' > "$TEMP_SQL_FILE_NAME"
$CLICKHOUSE_LOCAL --async_insert=0 --queries-file="$TEMP_SQL_FILE_NAME" \
    -q 'SELECT count(); SELECT sum(x) FROM `table`;' < "$STDIN_DATA_FILE_NAME"

# --copy is a format-conversion shortcut, not a second query mode for --queries-file.
$CLICKHOUSE_LOCAL --copy --queries-file="$TEMP_SQL_FILE_NAME" --input-format CSV < "$STDIN_DATA_FILE_NAME" 2>&1 | grep -o 'BAD_ARGUMENTS'

# Errors in a query file must stop execution before --query.
echo "SELECT throwIf(1, 'file failed');" > "$TEMP_SQL_FILE_NAME"
if output=$($CLICKHOUSE_LOCAL --queries-file="$TEMP_SQL_FILE_NAME" -q "SELECT 'trailing query';" < /dev/null 2>&1); then
    echo "unexpected success"
elif [[ "$output" == *"file failed"* ]] && ! grep -Fxq 'trailing query' <<< "$output"; then
    echo "file error stopped query"
else
    echo "unexpected output"
fi


# A failure in the second query file also stops the trailing --query.
printf '%s\n' "SELECT 'first file';" > "$TEMP_SQL_FILE_NAME"
echo "SELECT throwIf(1, 'second file failed');" > "$SECOND_SQL_FILE_NAME"
if output=$($CLICKHOUSE_LOCAL --queries-file="$TEMP_SQL_FILE_NAME" --queries-file="$SECOND_SQL_FILE_NAME" -q "SELECT 'trailing query';" < /dev/null 2>&1); then
    echo "unexpected success"
elif [[ "$output" == *"second file failed"* ]] && ! grep -Fxq 'trailing query' <<< "$output"; then
    echo "second file stopped query"
else
    echo "unexpected output"
fi
# SQL supplied through redirected stdin is not implicit table data.
printf '%s\n' 'CREATE FUNCTION f_stdin_01523 AS (x) -> x + 1;' 'SELECT 99;' > "$TEMP_SQL_FILE_NAME"
$CLICKHOUSE_LOCAL --queries-file=/dev/stdin -q 'SELECT f_stdin_01523(41)' < "$TEMP_SQL_FILE_NAME"
$CLICKHOUSE_LOCAL --queries-file="$TEMP_SQL_FILE_NAME" -q 'SELECT f_stdin_01523(41)' < "$TEMP_SQL_FILE_NAME"

# Nested includes must also recognize stdin as SQL, not input data.
printf '%s\n' '\i /dev/stdin' > "$SECOND_SQL_FILE_NAME"
$CLICKHOUSE_LOCAL --queries-file="$SECOND_SQL_FILE_NAME" -q 'SELECT f_stdin_01523(41)' < "$TEMP_SQL_FILE_NAME"

# A prelude that does not read stdin must not make read() replay the same rows.
printf '1\n2\n3\n' > "$STDIN_DATA_FILE_NAME"
echo 'SELECT 99 FROM system.one;' > "$TEMP_SQL_FILE_NAME"
$CLICKHOUSE_LOCAL --storage_file_read_method=read --use_cache_for_count_from_files=0 --queries-file="$TEMP_SQL_FILE_NAME" \
    -q 'SELECT count(); SELECT count(); SELECT count()' < "$STDIN_DATA_FILE_NAME"

# Repeated pread queries must keep the input boundary inherited from the caller.
# Schema inference and later reads should both skip the already-consumed first row.
printf '1\n2\n3\n' > "$STDIN_DATA_FILE_NAME"
echo "SELECT 'init' FROM system.one;" > "$TEMP_SQL_FILE_NAME"
{ IFS= read -r _; $CLICKHOUSE_LOCAL --storage_file_read_method=pread --use_cache_for_count_from_files=0 \
    -q 'SELECT sum(c1); SELECT sum(c1)'; } < "$STDIN_DATA_FILE_NAME"
{ IFS= read -r _; $CLICKHOUSE_LOCAL --storage_file_read_method=pread --use_cache_for_count_from_files=0 \
    --queries-file="$TEMP_SQL_FILE_NAME" -q 'SELECT sum(c1); SELECT sum(c1)'; } < "$STDIN_DATA_FILE_NAME"

# A SET database change must not let inferred stdin shadow an explicit table.
printf '1\n2\n3\n' > "$STDIN_DATA_FILE_NAME"
printf '%s\n' 'CREATE DATABASE db_01523;' \
    'CREATE TABLE db_01523.`table` (x UInt8) ENGINE=Memory;' \
    "SET database = 'db_01523';" > "$TEMP_SQL_FILE_NAME"
$CLICKHOUSE_LOCAL --queries-file="$TEMP_SQL_FILE_NAME" \
    -q 'SELECT count() FROM `table`; TRUNCATE TABLE `table`; SELECT count() FROM `table`' 0<>"$STDIN_DATA_FILE_NAME"
cat "$STDIN_DATA_FILE_NAME"

# Per-query database settings must not expose the same collision either.
printf '%s\n' 'CREATE DATABASE db_01523;' \
    'CREATE TABLE db_01523.`table` (x UInt8) ENGINE=Memory;' > "$TEMP_SQL_FILE_NAME"
$CLICKHOUSE_LOCAL --queries-file="$TEMP_SQL_FILE_NAME" \
    -q "SELECT count() FROM \`table\` SETTINGS database = 'db_01523'" < "$STDIN_DATA_FILE_NAME"

# A query-file setting must override the inferred stdin default, including ''.
echo "SET implicit_table_at_top_level = '';" > "$TEMP_SQL_FILE_NAME"
$CLICKHOUSE_LOCAL --queries-file="$TEMP_SQL_FILE_NAME" -q 'SELECT 42' < "$STDIN_DATA_FILE_NAME"
printf '%s\n' 'CREATE TABLE selected_01523 (x UInt8) ENGINE=Memory;' \
    'INSERT INTO selected_01523 SELECT arrayJoin([7, 8]);' \
    "SET implicit_table_at_top_level = 'selected_01523';" > "$TEMP_SQL_FILE_NAME"
$CLICKHOUSE_LOCAL --async_insert=0 --queries-file="$TEMP_SQL_FILE_NAME" -q 'SELECT sum(x)' < "$STDIN_DATA_FILE_NAME"

# Consuming stdin in a query file must not move the start of later pread scans.
printf '1\n2\n3\n' > "$STDIN_DATA_FILE_NAME"
printf '%s\n' 'CREATE TABLE prelude_stdin_01523 (x UInt8) ENGINE=File(TSV, stdin);' \
    "SELECT sum(x) FROM prelude_stdin_01523 SETTINGS storage_file_read_method='read';" > "$TEMP_SQL_FILE_NAME"
$CLICKHOUSE_LOCAL --storage_file_read_method=pread --use_cache_for_count_from_files=0 \
    --queries-file="$TEMP_SQL_FILE_NAME" -q 'SELECT sum(c1); SELECT sum(c1)' < "$STDIN_DATA_FILE_NAME"

# mmap already preserves descriptor position. Its first and later reads must
# use the same range, even if the caller consumed a line before clickhouse-local.
echo 'SELECT 99 FROM system.one;' > "$TEMP_SQL_FILE_NAME"
{ IFS= read -r _; $CLICKHOUSE_LOCAL --storage_file_read_method=mmap --use_cache_for_count_from_files=0 \
    -q 'SELECT sum(c1); SELECT sum(c1)'; } < "$STDIN_DATA_FILE_NAME"
{ IFS= read -r _; $CLICKHOUSE_LOCAL --storage_file_read_method=mmap --use_cache_for_count_from_files=0 \
    --queries-file="$TEMP_SQL_FILE_NAME" -q 'SELECT sum(c1); SELECT sum(c1)'; } < "$STDIN_DATA_FILE_NAME"

# For descriptor input, io_uring and pread_threadpool consume the shared fd,
# just like read. The deferred inference buffer must not replay stdin.
for method in io_uring pread_threadpool; do
    $CLICKHOUSE_LOCAL --storage_file_read_method="$method" --use_cache_for_count_from_files=0 \
        --queries-file="$TEMP_SQL_FILE_NAME" -q 'SELECT count(); SELECT count(); SELECT count()' < "$STDIN_DATA_FILE_NAME"
done

# Switching to mmap inside a query file must replace the forced pread
# inference reader; otherwise the first result would start at a different offset.
printf '%s\n' "SET storage_file_read_method = 'mmap';" \
    'SELECT 99 FROM system.one;' > "$TEMP_SQL_FILE_NAME"
{ IFS= read -r _; $CLICKHOUSE_LOCAL --storage_file_read_method=read --use_cache_for_count_from_files=0 \
    --queries-file="$TEMP_SQL_FILE_NAME" -q 'SELECT sum(c1); SELECT sum(c1)'; } < "$STDIN_DATA_FILE_NAME"

# After query files finish, the inferred table is available by its public name.
# The prelude must not change the meaning of an explicit FROM table.
printf '1\n2\n3\n' > "$STDIN_DATA_FILE_NAME"
echo 'SELECT 99 FROM system.one;' > "$TEMP_SQL_FILE_NAME"
$CLICKHOUSE_LOCAL --queries-file="$TEMP_SQL_FILE_NAME" \
    -q 'SELECT sum(c1) FROM `table`' < "$STDIN_DATA_FILE_NAME"
$CLICKHOUSE_LOCAL --queries-file="$TEMP_SQL_FILE_NAME" --table data_01523 \
    -q 'SELECT sum(c1) FROM data_01523' < "$STDIN_DATA_FILE_NAME"

# Public input names must not be hidden by tables in unrelated databases.
echo 'SELECT 99 FROM system.one;' > "$TEMP_SQL_FILE_NAME"
$CLICKHOUSE_LOCAL --table numbers --queries-file="$TEMP_SQL_FILE_NAME" \
    -q 'SELECT sum(c1) FROM numbers' < "$STDIN_DATA_FILE_NAME"
printf '%s\n' 'CREATE DATABASE unrelated_01523;' \
    'CREATE TABLE unrelated_01523.`table` (x UInt8) ENGINE=Memory;' \
    'SELECT 99 FROM system.one;' > "$TEMP_SQL_FILE_NAME"
$CLICKHOUSE_LOCAL --queries-file="$TEMP_SQL_FILE_NAME" -q 'SELECT sum(c1) FROM `table`' < "$STDIN_DATA_FILE_NAME"

# Each statement must resolve public aliases against its effective database.
$CLICKHOUSE_LOCAL --queries-file="$TEMP_SQL_FILE_NAME" \
    -q "SELECT sum(c1) FROM \`table\`; SELECT count() FROM \`table\` SETTINGS database='unrelated_01523'; SELECT sum(c1) FROM \`table\`" \
    < "$STDIN_DATA_FILE_NAME"

# A USE switch must remove the alias from the active query context as well as the session.
$CLICKHOUSE_LOCAL --queries-file="$TEMP_SQL_FILE_NAME" \
    -q 'SELECT sum(c1) FROM `table`; USE unrelated_01523; SELECT count() FROM `table`; USE default; SELECT sum(c1) FROM `table`' \
    < "$STDIN_DATA_FILE_NAME"

# Dropping inferred input allows the user to replace its public temporary name.
echo 'SELECT 99 FROM system.one;' > "$TEMP_SQL_FILE_NAME"
$CLICKHOUSE_LOCAL --queries-file="$TEMP_SQL_FILE_NAME" \
    -q 'CREATE TEMPORARY TABLE copied AS SELECT * FROM `table`; DROP TABLE `table`; CREATE TEMPORARY TABLE `table` AS SELECT * FROM copied; SELECT sum(c1) FROM `table`' \
    < "$STDIN_DATA_FILE_NAME"

# A replacement with different contents must not resurrect the inferred input alias.
$CLICKHOUSE_LOCAL --async_insert=0 --queries-file="$TEMP_SQL_FILE_NAME" \
    -q 'DROP TABLE `table`; CREATE TEMPORARY TABLE `table` (c1 UInt8) ENGINE=Memory; INSERT INTO `table` SELECT 9 FROM system.one; SELECT sum(c1) FROM `table`' \
    < "$STDIN_DATA_FILE_NAME"

# A profile-supplied implicit-table setting must not be overwritten.
printf '%s\n' "CREATE SETTINGS PROFILE profile_01523 SETTINGS implicit_table_at_top_level = '' READONLY;" \
    "SET profile = 'profile_01523';" > "$TEMP_SQL_FILE_NAME"
$CLICKHOUSE_LOCAL --queries-file="$TEMP_SQL_FILE_NAME" -q 'SELECT 42' < "$STDIN_DATA_FILE_NAME"

# An error before the -q phase must not leave a dangling temporary table holder.
echo "SELECT throwIf(1, 'prelude failed');" > "$TEMP_SQL_FILE_NAME"
if output=$($CLICKHOUSE_LOCAL --queries-file="$TEMP_SQL_FILE_NAME" -q 'SELECT count()' < "$STDIN_DATA_FILE_NAME" 2>&1); then
    echo 'unexpected success with failed prelude'
elif [[ "$output" == *'prelude failed'* && "$output" != *'TemporaryTableHolder'* && "$output" != *'Aborted'* ]]; then
    echo 'failed prelude cleaned up'
else
    echo 'failed prelude did not clean up'
fi
