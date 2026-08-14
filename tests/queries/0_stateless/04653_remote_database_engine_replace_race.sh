#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Resolving a table of a `Remote` database through a local shard reads the structure from the local
# table's in-memory metadata. The structure can come back empty because of a race with concurrent DDL
# (e.g. `REPLACE TABLE` or lazy storage initialization) — see `getStructureOfRemoteTable`, which treats
# it as a transient condition. The `Remote` database must do the same: an existing table must never be
# reported as missing (`UNKNOWN_TABLE`); a transient failed resolution (`NO_REMOTE_SHARD_AVAILABLE`,
# asking the caller to retry) is acceptable.

REMOTE_DB="${CLICKHOUSE_DATABASE}_remote"

${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${REMOTE_DB}"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.t (id UInt64) ENGINE = MergeTree ORDER BY id;
    CREATE DATABASE ${REMOTE_DB} ENGINE = Remote('127.0.0.1', '${CLICKHOUSE_DATABASE}', 'default', '');
"

# Both loops are sent as a single batch of statements each: starting a client per query dominated the
# runtime of this test and made it exceed the time limit of a sanitizer build.
REPLACE_QUERIES=""
DESCRIBE_QUERIES=""
for _ in {1..30}
do
    REPLACE_QUERIES+="CREATE OR REPLACE TABLE ${CLICKHOUSE_DATABASE}.t (id UInt64) ENGINE = MergeTree ORDER BY id; "
    DESCRIBE_QUERIES+="DESCRIBE TABLE ${REMOTE_DB}.t FORMAT Null; "
done

function replace_thread()
{
    ${CLICKHOUSE_CLIENT} --query "${REPLACE_QUERIES}"
}

function describe_thread()
{
    # A transient `NO_REMOTE_SHARD_AVAILABLE` aborts the rest of the batch, so send a few of them.
    for _ in {1..3}
    do
        ${CLICKHOUSE_CLIENT} --query "${DESCRIBE_QUERIES}" 2>&1
    done
}

replace_thread &
describe_thread > "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_describe.out" 2>&1 &
wait

# The table exists for the whole duration of the loop, so `UNKNOWN_TABLE` ("Table ... does not exist")
# must never appear (the transient retryable `NO_REMOTE_SHARD_AVAILABLE` is acceptable).
if grep -e 'UNKNOWN_TABLE' -e 'does not exist' "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_describe.out"
then
    echo 'FAIL: an existing table was reported as missing'
fi
rm -f "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_describe.out"

echo 'OK'

${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${REMOTE_DB}"
