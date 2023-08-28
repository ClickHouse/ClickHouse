#!/bin/bash

# This script sets up export of system log tables to a remote server.
# Remote tables are created if not exist, and augmented with extra columns,
# and their names will contain a hash of the table structure,
# which allows exporting tables from servers of different versions.

# Pre-configured destination cluster, where to export the data
CLUSTER=${CLUSTER:=system_logs_export}

EXTRA_COLUMNS=${EXTRA_COLUMNS:="pull_request_number UInt32, commit_sha String, check_start_time DateTime, check_name LowCardinality(String), instance_type LowCardinality(String), "}
EXTRA_COLUMNS_EXPRESSION=${EXTRA_COLUMNS_EXPRESSION:="0 AS pull_request_number, '' AS commit_sha, now() AS check_start_time, '' AS check_name, '' AS instance_type"}
EXTRA_ORDER_BY_COLUMNS=${EXTRA_ORDER_BY_COLUMNS:="check_name, "}

CONNECTION_PARAMETERS=${CONNECTION_PARAMETERS:=""}

# Create all configured system logs:
clickhouse-client --query "SYSTEM FLUSH LOGS"

# It's doesn't make sense to try creating tables if SYNC fails
echo "SYSTEM SYNC DATABASE REPLICA default" | clickhouse-client --receive_timeout 180 $CONNECTION_PARAMETERS || exit 0

# For each system log table:
clickhouse-client --query "SHOW TABLES FROM system LIKE '%\\_log'" | while read -r table
do
    # Calculate hash of its structure:
    hash=$(clickhouse-client --query "
        SELECT sipHash64(groupArray((name, type)))
        FROM (SELECT name, type FROM system.columns
            WHERE database = 'system' AND table = '$table'
            ORDER BY position)
        ")

    # Create the destination table with adapted name and structure:
    statement=$(clickhouse-client --format TSVRaw --query "SHOW CREATE TABLE system.${table}" | sed -r -e '
        s/^\($/('"$EXTRA_COLUMNS"'/;
        s/ORDER BY \(/ORDER BY ('"$EXTRA_ORDER_BY_COLUMNS"'/;
        s/^CREATE TABLE system\.\w+_log$/CREATE TABLE IF NOT EXISTS '"$table"'_'"$hash"'/;
        /^TTL /d
        ')

    echo "Creating destination table ${table}_${hash}" >&2

    echo "$statement" | clickhouse-client --distributed_ddl_task_timeout=10 --receive_timeout=10 --send_timeout=10 $CONNECTION_PARAMETERS || continue

    echo "Creating table system.${table}_sender" >&2

    # Create Distributed table and materialized view to watch on the original table:
    clickhouse-client --query "
        CREATE TABLE system.${table}_sender
        ENGINE = Distributed(${CLUSTER}, default, ${table}_${hash})
        SETTINGS flush_on_detach=0
        EMPTY AS
        SELECT ${EXTRA_COLUMNS_EXPRESSION}, *
        FROM system.${table}
    "

    echo "Creating materialized view system.${table}_watcher" >&2

    clickhouse-client --query "
        CREATE MATERIALIZED VIEW system.${table}_watcher TO system.${table}_sender AS
        SELECT ${EXTRA_COLUMNS_EXPRESSION}, *
        FROM system.${table}
    "
done
