#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression coverage for issue #42610: a VIEW with a scalar subquery used as
# a dictionary source must re-evaluate the subquery on every background
# reload, so the dictionary picks up new data. The original test relied on a
# fixed `SELECT sleep(3) FROM numbers(4)` wait to give the periodic updater
# time to fire, which became flaky under CI load. Polling the dictionary
# until the expected value appears is deterministic while still exercising
# the background-reload path that the issue is about; `SYSTEM RELOAD
# DICTIONARY` deliberately is not used here because forced reload was the
# original workaround in #42610 and would mask a regression of the fix.

$CLICKHOUSE_CLIENT -q "
    DROP DICTIONARY IF EXISTS TestTblDict;
    DROP VIEW IF EXISTS TestTbl_view;
    DROP TABLE IF EXISTS TestTbl;

    CREATE TABLE TestTbl
    (
        \`id\` UInt16,
        \`dt\` Date,
        \`val\` String
    )
    ENGINE = MergeTree
    PARTITION BY dt
    ORDER BY (id);

    CREATE VIEW TestTbl_view
    AS
    SELECT *
    FROM TestTbl
    WHERE dt = (SELECT max(dt) FROM TestTbl);

    CREATE DICTIONARY IF NOT EXISTS TestTblDict
    (
        \`id\` UInt16,
        \`dt\` Date,
        \`val\` String
    )
    PRIMARY KEY id
    SOURCE(CLICKHOUSE(TABLE TestTbl_view DB currentDatabase()))
    LIFETIME(1)
    LAYOUT(COMPLEX_KEY_HASHED());
"

# Wait until the dictionary picks up id=1's `dt` via background reload.
wait_for_dict_dt() {
    local expected=$1
    local deadline=$((SECONDS + 60))
    while [ $SECONDS -lt $deadline ]; do
        local actual
        actual=$($CLICKHOUSE_CLIENT -q "SELECT dt FROM TestTblDict WHERE id = 1")
        if [ "$actual" = "$expected" ]; then
            return 0
        fi
        sleep 0.2
    done
    echo "Timed out waiting for dictionary dt=$expected" >&2
    return 1
}

$CLICKHOUSE_CLIENT -q "INSERT INTO TestTbl VALUES (1, '2022-10-20', 'first')"
wait_for_dict_dt '2022-10-20'
$CLICKHOUSE_CLIENT -q "SELECT 'view' AS src, * FROM TestTbl_view"
$CLICKHOUSE_CLIENT -q "SELECT 'dict' AS src, * FROM TestTblDict"

$CLICKHOUSE_CLIENT -q "INSERT INTO TestTbl VALUES (1, '2022-10-21', 'second')"
wait_for_dict_dt '2022-10-21'
$CLICKHOUSE_CLIENT -q "SELECT 'view' AS src, * FROM TestTbl_view"
$CLICKHOUSE_CLIENT -q "SELECT 'dict' AS src, * FROM TestTblDict"

$CLICKHOUSE_CLIENT -q "
    DROP DICTIONARY IF EXISTS TestTblDict;
    DROP VIEW IF EXISTS TestTbl_view;
    DROP TABLE IF EXISTS TestTbl;
"
