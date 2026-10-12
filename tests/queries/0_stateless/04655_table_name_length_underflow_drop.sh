#!/usr/bin/env bash
# A table must not be creatable unless its dropped-metadata name
# metadata_dropped/{db}.{table}.{uuid}.sql can exist. Past the point where the escaped
# database name alone fills that budget the limit is 0, so the CREATE is rejected up front
# instead of succeeding and leaving a table that cannot be dropped.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Pad the unique per-test database name out to an exact escaped length. CLICKHOUSE_DATABASE is
# test_<8 lowercase letters and digits>, so every byte is a word character and the escaped
# length equals the character count. The unique prefix is what keeps this parallel-safe.
pad() { printf '%*s' "$1" '' | tr ' ' "${2:-d}"; }
# 211 is the largest escaped database name that still leaves room for a 2-character table name.
db_ok="${CLICKHOUSE_DATABASE}$(pad $((211 - ${#CLICKHOUSE_DATABASE})))"
# 214 is the first escaped length whose prefix alone exceeds the limit.
db_too_long="${CLICKHOUSE_DATABASE}$(pad $((214 - ${#CLICKHOUSE_DATABASE})))"
# Same 214 escaped bytes, but from only 118 characters: escapeForFileName expands every non-word
# byte to three, so a limit computed from the character count would be 95 instead of 0.
db_esc="${CLICKHOUSE_DATABASE}$(pad $((70 - ${#CLICKHOUSE_DATABASE})))$(pad 48 -)"
# 211 escaped bytes again, so its own limit is 2: long enough to hold t0, short enough that any
# longer destination name is rejected while it is the receiver.
db_rescue="${CLICKHOUSE_DATABASE}_r$(pad $((209 - ${#CLICKHOUSE_DATABASE})))"
db_short="${CLICKHOUSE_DATABASE}_short"
db_shrunk="${CLICKHOUSE_DATABASE}_shrunk"
db_absent="${CLICKHOUSE_DATABASE}_absent"

# Report a rename outcome by name: no output means it was accepted, the length guard means it was
# rejected, an unknown target database is reported as such, and any other error reddens the
# reference instead of being mistaken for one of the first three.
rename_outcome() {
    local label="$1"
    local out
    out=$($CLICKHOUSE_CLIENT -q "$2" 2>&1)
    if [ -z "$out" ]; then echo "$label-accepted"
    elif echo "$out" | grep -q 'ARGUMENT_OUT_OF_BOUND'; then echo "$label-rejected"
    elif echo "$out" | grep -q 'UNKNOWN_DATABASE'; then echo "$label-reported"
    else echo "$label-unexpected-error"; fi
}

# When using s3_plain_rewriteable as a db disk, minio doesn't allow the path segment to have
# more than 255 characters, and these database names produce segments close to that limit.
# Refer: https://github.com/minio/minio/blob/ddd9a84cd769e6bed67f5fe860f8f3c7527a6971/cmd/xl-storage.go#L154-L167
use_s3_plain_rewriteable_as_db_disk=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.disks WHERE name='disk_db_remote' AND type = 'ObjectStorage' AND object_storage_type='S3' AND metadata_type='PlainRewritable'" | tr -d '[:space:]')
if [ "$use_s3_plain_rewriteable_as_db_disk" == "0" ]; then
    # Just under the boundary: still allowed, and still droppable.
    $CLICKHOUSE_CLIENT -q "CREATE DATABASE \`$db_ok\` ENGINE = Atomic"
    $CLICKHOUSE_CLIENT -q "CREATE TABLE \`$db_ok\`.t0 (c0 Int) ENGINE = MergeTree() ORDER BY tuple()"
    echo "created"
    $CLICKHOUSE_CLIENT -q "DROP TABLE \`$db_ok\`.t0"
    echo "dropped"
    $CLICKHOUSE_CLIENT -q "DROP DATABASE \`$db_ok\`"

    # Past the boundary: rejected at CREATE, so no undroppable table is left behind.
    $CLICKHOUSE_CLIENT -q "CREATE DATABASE \`$db_too_long\` ENGINE = Atomic"
    $CLICKHOUSE_CLIENT -q "CREATE TABLE \`$db_too_long\`.t0 (c0 Int) ENGINE = MergeTree() ORDER BY tuple()" 2>&1 | grep -o -m 1 'ARGUMENT_OUT_OF_BOUND'

    # RENAME TABLE must validate the destination name against the database the table lands in,
    # not against the one it leaves (issue #102463). Each rename below is independent, so no
    # assertion depends on an earlier one having been accepted.
    $CLICKHOUSE_CLIENT -q "CREATE DATABASE \`$db_esc\` ENGINE = Atomic"
    $CLICKHOUSE_CLIENT -q "CREATE DATABASE \`$db_rescue\` ENGINE = Atomic"
    $CLICKHOUSE_CLIENT -q "CREATE DATABASE \`$db_short\` ENGINE = Atomic"
    $CLICKHOUSE_CLIENT -q "CREATE TABLE \`$db_short\`.r0 (c0 Int) ENGINE = MergeTree() ORDER BY tuple()"
    $CLICKHOUSE_CLIENT -q "CREATE TABLE \`$db_short\`.r1 (c0 Int) ENGINE = MergeTree() ORDER BY tuple()"
    $CLICKHOUSE_CLIENT -q "CREATE TABLE \`$db_short\`.r2 (c0 Int) ENGINE = MergeTree() ORDER BY tuple()"
    $CLICKHOUSE_CLIENT -q "CREATE TABLE \`$db_rescue\`.t0 (c0 Int) ENGINE = MergeTree() ORDER BY tuple()"

    # Views, materialized views, refreshable materialized views and dictionaries are renamed by the
    # same statement, so the destination length is checked against the same database for all of
    # them. They read `vsrc`, which no assertion renames, so each case below stands alone.
    $CLICKHOUSE_CLIENT -q "CREATE TABLE \`$db_short\`.vsrc (c0 Int) ENGINE = MergeTree() ORDER BY tuple()"
    $CLICKHOUSE_CLIENT -q "CREATE TABLE \`$db_short\`.mv_target (c0 Int) ENGINE = MergeTree() ORDER BY tuple()"
    $CLICKHOUSE_CLIENT -q "CREATE TABLE \`$db_short\`.rmv_target (c0 Int) ENGINE = MergeTree() ORDER BY tuple()"
    $CLICKHOUSE_CLIENT -q "CREATE VIEW \`$db_short\`.v0 AS SELECT * FROM \`$db_short\`.vsrc"
    $CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW \`$db_short\`.mv0 TO \`$db_short\`.mv_target AS SELECT * FROM \`$db_short\`.vsrc"
    $CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW \`$db_short\`.rmv0 REFRESH EVERY 1 YEAR TO \`$db_short\`.rmv_target AS SELECT * FROM \`$db_short\`.vsrc"
    $CLICKHOUSE_CLIENT -q "CREATE DICTIONARY \`$db_short\`.d0 (c0 Int DEFAULT 0) PRIMARY KEY c0 SOURCE(CLICKHOUSE(TABLE 'vsrc' DB '$db_short')) LAYOUT(FLAT()) LIFETIME(0)"

    # Into an oversized database: must be rejected, or the table cannot be dropped afterwards.
    rename_outcome "into-long" "RENAME TABLE \`$db_short\`.r0 TO \`$db_too_long\`.r0"
    rename_outcome "into-long-escaped" "RENAME TABLE \`$db_short\`.r1 TO \`$db_esc\`.r1"

    # Out of an oversized database into a short one: must be accepted, and the table must then
    # drop. This is the per-table escape hatch out of an already-oversized database.
    rename_outcome "out-of-long" "RENAME TABLE \`$db_rescue\`.t0 TO \`$db_short\`.rescued_table"
    if $CLICKHOUSE_CLIENT -q "DROP TABLE \`$db_short\`.rescued_table" 2>/dev/null; then
        echo "rescued-dropped"
    else
        echo "rescued-drop-failed"
    fi

    # An unknown target database is reported as such, rather than as a length violation measured
    # against the source database.
    src_limit=$($CLICKHOUSE_CLIENT -q "SELECT getMaxTableNameLengthForDatabase('$db_short')")
    over_limit_name=$(pad $((src_limit + 1)) a)
    rename_outcome "unknown-db" "RENAME TABLE \`$db_short\`.r2 TO \`$db_absent\`.\`$over_limit_name\`"

    rename_outcome "view-into-long" "RENAME TABLE \`$db_short\`.v0 TO \`$db_too_long\`.v0"
    rename_outcome "matview-into-long" "RENAME TABLE \`$db_short\`.mv0 TO \`$db_too_long\`.mv0"
    rename_outcome "refreshable-matview-into-long" "RENAME TABLE \`$db_short\`.rmv0 TO \`$db_too_long\`.rmv0"
    rename_outcome "dictionary-into-long" "RENAME DICTIONARY \`$db_short\`.d0 TO \`$db_too_long\`.d0"

    # db_too_long and db_esc are empty, but their own escaped names already fill the dropped
    # metadata budget, so shorten one of them first. That is the workaround from the issue.
    $CLICKHOUSE_CLIENT -q "RENAME DATABASE \`$db_too_long\` TO \`$db_shrunk\`"
    $CLICKHOUSE_CLIENT -q "DROP DATABASE \`$db_shrunk\`"
    $CLICKHOUSE_CLIENT -q "DROP DATABASE \`$db_esc\`"
    $CLICKHOUSE_CLIENT -q "DROP DATABASE \`$db_rescue\`"
    $CLICKHOUSE_CLIENT -q "DROP DATABASE \`$db_short\`"
else
    echo "created"
    echo "dropped"
    echo "ARGUMENT_OUT_OF_BOUND"
    echo "into-long-rejected"
    echo "into-long-escaped-rejected"
    echo "out-of-long-accepted"
    echo "rescued-dropped"
    echo "unknown-db-reported"
    echo "view-into-long-rejected"
    echo "matview-into-long-rejected"
    echo "refreshable-matview-into-long-rejected"
    echo "dictionary-into-long-rejected"
fi
