#!/usr/bin/env bash
# Tags: atomic-database
# Tag atomic-database: this test edits local Atomic table metadata directly.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE=tab
FULL_ATTACH_TABLE=tab_full_attach
FIXED_TABLE=tab_fixed

cleanup()
{
    if [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.detached_tables WHERE database = currentDatabase() AND table = '${TABLE}'" 2>/dev/null)" = 1 ]; then
        $CLICKHOUSE_CLIENT -q "ATTACH TABLE ${TABLE}" >/dev/null 2>&1
    fi
    if [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.detached_tables WHERE database = currentDatabase() AND table = '${FIXED_TABLE}'" 2>/dev/null)" = 1 ]; then
        $CLICKHOUSE_CLIENT -q "ATTACH TABLE ${FIXED_TABLE}" >/dev/null 2>&1
    fi
    $CLICKHOUSE_CLIENT -q "
        DROP TABLE IF EXISTS ${TABLE} SYNC;
        DROP TABLE IF EXISTS ${FULL_ATTACH_TABLE} SYNC;
        DROP TABLE IF EXISTS ${FIXED_TABLE} SYNC;" >/dev/null 2>&1
}

trap cleanup EXIT
cleanup

# A full-definition ATTACH is a newly submitted definition and must be validated like CREATE.
uuid=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
output=$($CLICKHOUSE_CLIENT --send_logs_level fatal -q "
    ATTACH TABLE ${FULL_ATTACH_TABLE} UUID '${uuid}'
    (
        t Array(Array(String)),
        INDEX idx t TYPE text(tokenizer = 'splitByNonAlpha')
    )
    ENGINE = MergeTree ORDER BY tuple();" 2>&1)

if grep -q -F 'BAD_ARGUMENTS' <<< "$output"; then
    echo 'full-definition ATTACH rejected'
else
    echo 'full-definition ATTACH was not rejected with BAD_ARGUMENTS'
fi
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${FULL_ATTACH_TABLE} SYNC"

uuid=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
output=$($CLICKHOUSE_CLIENT --send_logs_level fatal -q "
    ATTACH TABLE ${FULL_ATTACH_TABLE} UUID '${uuid}'
    (
        t Array(Array(FixedString(8))),
        INDEX idx t TYPE text(tokenizer = 'splitByNonAlpha')
    )
    ENGINE = MergeTree ORDER BY tuple();" 2>&1)

if grep -q -F 'BAD_ARGUMENTS' <<< "$output"; then
    echo 'full-definition FixedString ATTACH rejected'
else
    echo 'full-definition FixedString ATTACH was not rejected with BAD_ARGUMENTS'
fi
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${FULL_ATTACH_TABLE} SYNC"

# Simulate metadata accepted by an older server: the table already has data, then ALTER ADD INDEX
# added the unsupported definition without materializing it. Only the stored metadata is changed;
# no text-index data exists in the part.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${TABLE} (t Array(Array(String))) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO ${TABLE} VALUES ([['old']]);"

$CLICKHOUSE_CLIENT -q "DETACH TABLE ${TABLE} SYNC"

metadata_path=$($CLICKHOUSE_CLIENT -q "
    SELECT metadata_path
    FROM system.detached_tables
    WHERE database = currentDatabase() AND table = '${TABLE}'")
default_disk_path=$($CLICKHOUSE_CLIENT -q "SELECT path FROM system.disks WHERE name = 'default'")
metadata_file="${default_disk_path}${metadata_path}"
sed -i \
    's/`t` Array(Array(String))/`t` Array(Array(String)),\n    INDEX idx t TYPE text(tokenizer = '\''splitByNonAlpha'\'')/' \
    "$metadata_file"

# Short ATTACH replays the stored metadata and must remain loadable after an upgrade.
$CLICKHOUSE_CLIENT -q "ATTACH TABLE ${TABLE}"
$CLICKHOUSE_CLIENT -q "
    SELECT count()
    FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table = '${TABLE}' AND name = 'idx';"

# Compatibility is limited to loading metadata. The legacy index is not disabled, so writes which
# build it continue to fail until the user drops the index.
output=$($CLICKHOUSE_CLIENT --send_logs_level fatal -q "INSERT INTO ${TABLE} VALUES ([['new']])" 2>&1)
if grep -q -F 'NOT_IMPLEMENTED' <<< "$output"; then
    echo 'legacy index still rejects writes'
else
    echo 'legacy index write did not fail with NOT_IMPLEMENTED'
fi

$CLICKHOUSE_CLIENT -q "
    ALTER TABLE ${TABLE} DROP INDEX idx;
    INSERT INTO ${TABLE} VALUES ([['new']]);
    SELECT count() FROM ${TABLE};"

# A legacy multidimensional FixedString index may already have data. Metadata replay and subsequent
# writes retain the pre-upgrade behavior; this change only prevents new definitions.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${FIXED_TABLE} (t Array(Array(FixedString(8)))) ENGINE = MergeTree ORDER BY tuple();"
$CLICKHOUSE_CLIENT -q "DETACH TABLE ${FIXED_TABLE} SYNC"

metadata_path=$($CLICKHOUSE_CLIENT -q "
    SELECT metadata_path
    FROM system.detached_tables
    WHERE database = currentDatabase() AND table = '${FIXED_TABLE}'")
metadata_file="${default_disk_path}${metadata_path}"
sed -i \
    's/`t` Array(Array(FixedString(8)))/`t` Array(Array(FixedString(8))),\n    INDEX idx t TYPE text(tokenizer = '\''splitByNonAlpha'\'')/' \
    "$metadata_file"

$CLICKHOUSE_CLIENT -q "
    ATTACH TABLE ${FIXED_TABLE};
    SELECT count()
    FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table = '${FIXED_TABLE}' AND name = 'idx';
    INSERT INTO ${FIXED_TABLE} VALUES ([['abcdefgh', 'ijklmnop']]);
    DETACH TABLE ${FIXED_TABLE} SYNC;
    ATTACH TABLE ${FIXED_TABLE};
    INSERT INTO ${FIXED_TABLE} VALUES ([['qrstuvwx', 'yzabcdef']]);
    SELECT count() FROM ${FIXED_TABLE};"
