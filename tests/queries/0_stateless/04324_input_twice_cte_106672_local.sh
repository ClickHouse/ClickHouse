#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Callback-backed regression for the multi-reference `input` reject. Issue #106672 originally
# manifested over HTTP (covered in 04323), but `clickhouse-local`, the TCP handler and the gRPC
# server take a different path inside `StorageInput::readImpl`: when `getInputBlocksReaderCallback`
# is set, each plan-step gets its own fresh `StorageInputSource` pipe and the singleton storage
# never goes through the HTTP `was_pipe_initialized` / `setPipe` branch. Without the one-shot
# guard at the top of `ReadFromInput::initializePipeline`, two CTE references silently attach two
# sources to the same client-driven block reader, one consumes the data while the other sees EOF,
# and the INSERT silently drops rows. This test pins the rejection on the callback path.

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/04324_local_XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

LOCAL=(${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}")

"${LOCAL[@]}" --query "CREATE TABLE dst (id UInt64, name String) ENGINE = MergeTree() ORDER BY id"

echo '--- two CTE refs of input() in clickhouse-local: rejected'
echo '{"id":1,"name":"a"}' | "${LOCAL[@]}" --query "
    INSERT INTO dst
    WITH data AS (SELECT * FROM input('id UInt64, name String')),
         first_ref AS (SELECT id FROM data),
         second_ref AS (SELECT name FROM data)
    SELECT f.id, s.name FROM first_ref f CROSS JOIN second_ref s
    FORMAT JSONEachRow
" 2>&1 | grep -oE 'INVALID_USAGE_OF_INPUT|LOGICAL_ERROR' | head -1

echo '--- single CTE ref of input() in clickhouse-local: allowed'
echo '{"id":2,"name":"single"}' | "${LOCAL[@]}" --query "
    INSERT INTO dst
    WITH data AS (SELECT * FROM input('id UInt64, name String'))
    SELECT * FROM data
    FORMAT JSONEachRow
"

echo '--- direct input() without CTE in clickhouse-local: allowed'
echo '{"id":3,"name":"direct"}' | "${LOCAL[@]}" --query "
    INSERT INTO dst SELECT * FROM input('id UInt64, name String') FORMAT JSONEachRow
"

echo '--- final rows'
"${LOCAL[@]}" --query "SELECT id, name FROM dst ORDER BY id"
