#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree
# no-shared-merge-tree: the database `max_rows` limit is supported only by `Atomic` and `Ordinary`.

# Retrying an `INSERT` that insert deduplication recognizes as already written must stay a no-op
# even once the database has reached `max_rows`: the limit rejects only parts that add rows.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_dedup"
CH="${CLICKHOUSE_CLIENT} --insert_deduplicate=1 --async_insert=0 --max_insert_block_size=1000000 --min_insert_block_size_rows=0 --min_insert_block_size_bytes=0"

$CH -q "DROP DATABASE IF EXISTS ${DB}"
$CH -q "CREATE DATABASE ${DB} ENGINE = Atomic SETTINGS max_rows = 10"
$CH -q "CREATE TABLE ${DB}.r (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/r', 'r1') ORDER BY x"
$CH -q "CREATE TABLE ${DB}.m (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS non_replicated_deduplication_window = 100"

for table in r m
do
    echo "-- ${table}: fill the database to the limit"
    $CH -q "TRUNCATE TABLE ${DB}.r"
    $CH -q "TRUNCATE TABLE ${DB}.m"
    $CH -q "INSERT INTO ${DB}.${table} SELECT number FROM numbers(10)"
    $CH -q "SELECT rows FROM system.databases WHERE name = '${DB}'"

    echo "-- ${table}: retrying the same INSERT is deduplicated, not rejected"
    $CH -q "INSERT INTO ${DB}.${table} SELECT number FROM numbers(10)"
    $CH -q "SELECT count() FROM ${DB}.${table}"

    echo "-- ${table}: an INSERT of new rows is rejected"
    $CH -q "INSERT INTO ${DB}.${table} SELECT number + 100 FROM numbers(10)" 2>&1 | grep -oF "TOO_MANY_ROWS" | head -n1

    echo "-- ${table}: so is a repeated INSERT with deduplication disabled"
    $CH -q "INSERT INTO ${DB}.${table} SETTINGS insert_deduplicate = 0 SELECT number FROM numbers(10)" 2>&1 | grep -oF "TOO_MANY_ROWS" | head -n1
    $CH -q "SELECT count() FROM ${DB}.${table}"
done

$CH -q "DROP DATABASE ${DB}"
