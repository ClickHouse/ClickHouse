#!/usr/bin/env bash
# Tags: long, no-parallel
# Tag no-parallel: FileLog -> MV streaming latency depends on `BackgroundSchedulePool` scheduling,
# same precedent as `02024_storage_filelog_mv.sh`.

# A push source starts the background job that feeds its materialized views in its own `startup`, which
# the `lazy_load_tables` stand-in never calls: nothing reads such a table directly, so the ingestion
# used to stall silently after the database was loaded, forever.

set -eu

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DIR="${USER_FILES_PATH:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}"

rm -rf -- "${DIR}"
mkdir -p "${DIR}/with_mv" "${DIR}/without_mv"

DB="${CLICKHOUSE_DATABASE}_lazy"

${CLICKHOUSE_CLIENT} --query "drop database if exists ${DB}"
${CLICKHOUSE_CLIENT} --query "create database ${DB} engine = Atomic settings lazy_load_tables = 1"

echo 1 > "${DIR}/with_mv/a.csv"

# poll_directory_watch_events_backoff_max bounds the watcher idle backoff to ~1s (default 32s).
${CLICKHOUSE_CLIENT} --query "create table ${DB}.q (a UInt64) engine = FileLog('${DIR}/with_mv/', 'CSV') settings poll_directory_watch_events_backoff_max = 1000"
${CLICKHOUSE_CLIENT} --query "create table ${DB}.dst (a UInt64) engine = MergeTree order by a"
${CLICKHOUSE_CLIENT} --query "create materialized view ${DB}.mv to ${DB}.dst as select a from ${DB}.q"

# A push source that feeds nothing is still lazily loaded: it cannot stall an ingestion it does not have.
${CLICKHOUSE_CLIENT} --query "create table ${DB}.unused (a UInt64) engine = FileLog('${DIR}/without_mv/', 'CSV')"

function count()
{
	${CLICKHOUSE_CLIENT} --query "select count() from ${DB}.dst"
}

function wait_for_count()
{
	local target="$1"
	local timeout=120
	local start=$EPOCHSECONDS
	while [[ $(count) != "$target" ]]; do
		if ((EPOCHSECONDS - start > timeout)); then
			echo "Timeout (${timeout}s) waiting for count() == ${target}, got $(count)."
			exit 1
		fi
		sleep 1
	done
}

wait_for_count 1

# Reloading the database is what a server restart does to a lazily loaded table.
${CLICKHOUSE_CLIENT} --query "detach database ${DB}"
${CLICKHOUSE_CLIENT} --query "attach database ${DB}"

# The source of the materialized view must not be a stand-in: its consumer has to be running without
# anyone reading it. The one without a materialized view must still be a stand-in.
${CLICKHOUSE_CLIENT} --query "select name, engine from system.tables where database = '${DB}' and name in ('q', 'unused') order by name"

echo 2 > "${DIR}/with_mv/b.csv"
wait_for_count 2

${CLICKHOUSE_CLIENT} --query "select a from ${DB}.dst order by a"
${CLICKHOUSE_CLIENT} --query "drop database ${DB} sync"

rm -rf -- "${DIR}"
