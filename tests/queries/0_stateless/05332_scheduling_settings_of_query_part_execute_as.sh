#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A statement wrapped in `EXECUTE AS` inside a `PARALLEL WITH` subquery is a part of the same query, so it
# cannot set scheduling settings that differ from the query's.

user="sched_part_target_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${user}"
$CLICKHOUSE_CLIENT --query "CREATE TABLE sched_probe (x UInt64) ENGINE = Memory"
$CLICKHOUSE_CLIENT --query "CREATE USER ${user} IDENTIFIED WITH no_password"
$CLICKHOUSE_CLIENT --query "GRANT INSERT, SELECT ON ${CLICKHOUSE_DATABASE}.sched_probe TO ${user}"

$CLICKHOUSE_CLIENT --query "INSERT INTO sched_probe SELECT 1 PARALLEL WITH EXECUTE AS ${user} INSERT INTO sched_probe SETTINGS weight = 2 SELECT 2" 2>&1 \
    | grep -o -m1 "BAD_ARGUMENTS"

# The other subquery of the rejected query may have inserted its row.
$CLICKHOUSE_CLIENT --query "TRUNCATE TABLE sched_probe"
$CLICKHOUSE_CLIENT --query "INSERT INTO sched_probe SELECT 1 PARALLEL WITH EXECUTE AS ${user} INSERT INTO sched_probe SELECT 2"
$CLICKHOUSE_CLIENT --query "SELECT x FROM sched_probe ORDER BY x"

$CLICKHOUSE_CLIENT --query "DROP USER ${user}"
$CLICKHOUSE_CLIENT --query "DROP TABLE sched_probe"
