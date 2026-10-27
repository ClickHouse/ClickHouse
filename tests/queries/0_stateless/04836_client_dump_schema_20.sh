#!/usr/bin/env bash

# --dump-schema emits the gates that CREATE reads outside the experimental-settings list when a stored definition
# carries them: a codec declared for a subcolumn, with the codec gates of its codecs, the `Elasticsearch` engine,
# gated functions in a column expression or type, `rank` with arguments and `STREAM` in the SELECT of a materialized
# view, and a dynamic disk configured `from_env`. Every dump is replayed into a fresh target with the gates at default.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# --dump-schema refuses input on stdin, whatever the test runner passes.
exec < /dev/null

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$work"
mkdir -p "$work"
trap 'rm -rf "$work"' EXIT

# Creates the objects of `$3` in database `d` of a fresh source and dumps it, printing the gates the dump sets. Then
# replays the dump into a fresh target, where clickhouse-local's `allow_introspection_functions = 1` is reset to the
# default, printing the first error code of the replay and whether `d.$2` has the definition of the source one.
# The remaining arguments go to every invocation.
round_trip() {
    local name=$1 object=$2 ddl=$3
    shift 3
    rm -rf "$work/src" "$work/dst"
    $CLICKHOUSE_LOCAL "$@" --path "$work/src" -n -q "CREATE DATABASE d; $ddl"
    $CLICKHOUSE_LOCAL "$@" --path "$work/src" --dump-schema d > "$work/$name.sql" 2>/dev/null
    echo "$name: dump rc=$?"
    grep '^SET ' "$work/$name.sql"
    $CLICKHOUSE_LOCAL "$@" --path "$work/dst" --allow_introspection_functions=0 -n --queries-file "$work/$name.sql" 2>&1 \
        | grep -o 'Code: [0-9]*' | head -1
    echo "$name: replay rc=${PIPESTATUS[0]}"
    local source_definition replayed_definition
    source_definition=$($CLICKHOUSE_LOCAL "$@" --path "$work/src" -q "SHOW CREATE TABLE d.$object")
    replayed_definition=$($CLICKHOUSE_LOCAL "$@" --path "$work/dst" -q "SHOW CREATE TABLE d.$object")
    echo "$name: same definition: $([ -n "$source_definition" ] && [ "$source_definition" = "$replayed_definition" ] && echo 1 || echo 0)"
}

# A codec declared for a subcolumn needs `enable_subcolumn_codecs`, and the gates its codecs need for the subcolumn type.
round_trip subcolumn_codec t "SET enable_subcolumn_codecs = 1;
    CREATE TABLE d.t (id UInt64, payload Tuple(ts DateTime64(3), value Float64), CODEC payload.ts (DoubleDelta, ZSTD(1)))
    ENGINE = MergeTree ORDER BY id;"
round_trip subcolumn_suspicious_codec t "SET enable_subcolumn_codecs = 1, allow_suspicious_codecs = 1;
    CREATE TABLE d.t (id UInt64, payload Tuple(ts UInt64, value Float64), CODEC payload.ts (Delta))
    ENGINE = MergeTree ORDER BY id;"
round_trip subcolumn_beta_codec t "SET enable_subcolumn_codecs = 1, enable_alp_codec = 1;
    CREATE TABLE d.t (id UInt64, payload Tuple(ts String, value Float64), CODEC payload.value (ALP, LZ4))
    ENGINE = MergeTree ORDER BY id;"

round_trip elasticsearch es "SET enable_elasticsearch = 1;
    CREATE TABLE d.es (doc JSON) ENGINE = Elasticsearch('http://127.0.0.1:1', 'idx');"

# CREATE builds the functions of a column expression and the aggregate function of a column type.
round_trip client_http_header t "SET allow_get_client_http_header = 1;
    CREATE TABLE d.t (s String, h String DEFAULT getClientHTTPHeader('X-Foo')) ENGINE = MergeTree ORDER BY s;"
round_trip introspection t "
    CREATE TABLE d.t (k UInt64, s String, n String DEFAULT demangle(s), f AggregateFunction(flameGraph, Array(UInt64)))
    ENGINE = MergeTree ORDER BY k;"
round_trip nonconst_timezone t "SET allow_nonconst_timezone_arguments = 1;
    CREATE TABLE d.t (t DateTime, tz String, l DateTime DEFAULT toTimeZone(t, tz)) ENGINE = MergeTree ORDER BY t;"
round_trip throw_if_error_code t "SET allow_custom_error_code_in_throwif = 1;
    CREATE TABLE d.t (x UInt64, y UInt8 MATERIALIZED throwIf(x > 100, 'big', toInt32(42))) ENGINE = MergeTree ORDER BY x;"

# CREATE analyzes the SELECT of a materialized view.
round_trip rank_arguments mv "SET allow_rank_dense_rank_arguments = 1;
    CREATE TABLE d.src (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE MATERIALIZED VIEW d.mv ENGINE = Memory AS SELECT x, rank(x) OVER (ORDER BY x) AS r FROM d.src;"
round_trip streaming_query mv "SET enable_streaming_queries = 1;
    CREATE TABLE d.src (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE MATERIALIZED VIEW d.mv ENGINE = Memory AS SELECT x FROM d.src STREAM;"

# A dynamic disk reads `from_env` when the table is created. The dump runs in a session that sees the path unmasked, and
# the source and the target put the disk in different directories under the same name.
users_config="$work/users.xml"
disk_config="$work/disk.xml"
cat > "$users_config" <<XML
<clickhouse>
    <profiles>
        <default><format_display_secrets_in_show_and_select>1</format_display_secrets_in_show_and_select></default>
    </profiles>
    <users>
        <default>
            <password></password>
            <profile>default</profile>
            <quota>default</quota>
        </default>
    </users>
    <quotas><default></default></quotas>
</clickhouse>
XML
cat > "$disk_config" <<XML
<clickhouse>
    <users_config>${users_config}</users_config>
    <display_secrets_in_show_and_select>1</display_secrets_in_show_and_select>
    <custom_local_disks_base_directory>${work}/disks/</custom_local_disks_base_directory>
</clickhouse>
XML
mkdir -p "$work/disks/src" "$work/disks/dst"
rm -rf "$work/src" "$work/dst"
CH_DUMP_SCHEMA_DISK_PATH="$work/disks/src/" $CLICKHOUSE_LOCAL --config-file "$disk_config" --path "$work/src" -n -q "
    CREATE DATABASE d;
    SET dynamic_disk_allow_from_env = 1;
    CREATE TABLE d.t (x UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS disk = disk(name = 'dump_schema_disk', type = local, path = 'from_env CH_DUMP_SCHEMA_DISK_PATH');"
CH_DUMP_SCHEMA_DISK_PATH="$work/disks/src/" $CLICKHOUSE_LOCAL --config-file "$disk_config" \
    --format_display_secrets_in_show_and_select=1 --path "$work/src" --dump-schema d > "$work/disk.sql" 2>/dev/null
echo "dynamic_disk: dump rc=$?"
grep '^SET ' "$work/disk.sql"
CH_DUMP_SCHEMA_DISK_PATH="$work/disks/dst/" $CLICKHOUSE_LOCAL --config-file "$disk_config" --path "$work/dst" \
    -n --queries-file "$work/disk.sql" 2>&1 | grep -o 'Code: [0-9]*' | head -1
echo "dynamic_disk: replay rc=${PIPESTATUS[0]}"
source_definition=$(CH_DUMP_SCHEMA_DISK_PATH="$work/disks/src/" $CLICKHOUSE_LOCAL --config-file "$disk_config" \
    --format_display_secrets_in_show_and_select=1 --path "$work/src" -q "SHOW CREATE TABLE d.t")
replayed_definition=$(CH_DUMP_SCHEMA_DISK_PATH="$work/disks/dst/" $CLICKHOUSE_LOCAL --config-file "$disk_config" \
    --format_display_secrets_in_show_and_select=1 --path "$work/dst" -q "SHOW CREATE TABLE d.t")
echo "dynamic_disk: same definition: $([ -n "$source_definition" ] && [ "$source_definition" = "$replayed_definition" ] && echo 1 || echo 0)"
