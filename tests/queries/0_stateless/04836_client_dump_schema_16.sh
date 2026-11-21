#!/usr/bin/env bash

# --dump-schema reads a `remote` cluster argument written as a server macro the way the server does: the macro is
# expanded before the cluster is looked up, so `{default_cluster_macro}` from the test configuration names the local
# cluster `test_shard_localhost`, and a reader through it is a local dependency. A macro the server cannot expand fails
# the server's own lookup, so the dump refuses it. A `SETTINGS` clause of a call is not one of its positional arguments:
# without a table argument the call reads `system.one`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# --dump-schema refuses input on stdin, whatever the test runner passes.
exec < /dev/null

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$work"
mkdir -p "$work"
DB="${CLICKHOUSE_DATABASE}_macro"
trap 'rm -rf "$work"; $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${DB}"' EXIT

# Prints whether the dump `$1` creates `$2` before `$3`.
dumped_before() {
    local first second
    first=$(grep -n "^CREATE [A-Z ]*$2 " "$1" | head -1 | cut -d: -f1)
    second=$(grep -n "^CREATE [A-Z ]*$3 " "$1" | head -1 | cut -d: -f1)
    [ -n "$first" ] && [ -n "$second" ] && [ "$first" -lt "$second" ] && echo yes || echo no
}

# Each reader checked for order sorts before the source it reads, so only the dependency edge through the macro dumps it
# after the source. The materialized view comes after the tables anyway, but its replay still needs the source.
$CLICKHOUSE_CLIENT -mq "
CREATE DATABASE ${DB} ENGINE = Atomic;
CREATE TABLE ${DB}.zzz_src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE ${DB}.zzz_dst (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE VIEW ${DB}.aaa_view AS SELECT * FROM remote(\`{default_cluster_macro}\`, '${DB}', 'zzz_src');
CREATE TABLE ${DB}.aab_engine ENGINE = Remote(\`{default_cluster_macro}\`, '${DB}', 'zzz_src');
CREATE TABLE ${DB}.aac_proxy AS remote(\`{default_cluster_macro}\`, ${DB}.zzz_src);
CREATE MATERIALIZED VIEW ${DB}.aad_mv TO ${DB}.zzz_dst AS
    SELECT dummy::UInt64 AS id FROM system.one WHERE id IN (SELECT id FROM remote(\`{default_cluster_macro}\`, ${DB}.zzz_src));
CREATE VIEW ${DB}.aae_cluster AS SELECT * FROM cluster(\`{default_cluster_macro}\`, '${DB}', 'zzz_src');
"
$CLICKHOUSE_CLIENT --dump-schema="${DB}" > "$work/dump.sql" 2> "$work/dump.err"
echo "dump rc=$?"
grep -o 'Code: [0-9]*' "$work/dump.err" | head -1
for reader in aaa_view aab_engine aac_proxy aae_cluster; do
    echo "${reader} after the source: $(dumped_before "$work/dump.sql" "${DB}.zzz_src" "${DB}.${reader}")"
done
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${DB}"
$CLICKHOUSE_CLIENT --queries-file "$work/dump.sql" 2>&1 | grep -o 'Code: [0-9]*' | head -1
echo "replay rc=${PIPESTATUS[0]}"
$CLICKHOUSE_CLIENT -mq "
INSERT INTO ${DB}.zzz_src VALUES (1);
SELECT count() FROM ${DB}.aaa_view;
SELECT count() FROM ${DB}.aab_engine;
"

# The same view in clickhouse-local, created while the configuration defines the macro and dumped with and without it.
cat > "$work/with_macro.xml" <<EOF
<clickhouse>
    <tcp_port>9999</tcp_port>
    <macros>
        <dump_schema_cluster_macro>dump_schema_macro_cluster</dump_schema_cluster_macro>
    </macros>
    <remote_servers>
        <dump_schema_macro_cluster>
            <shard><replica><host>127.0.0.1</host><port>9999</port></replica></shard>
        </dump_schema_macro_cluster>
    </remote_servers>
</clickhouse>
EOF
cat > "$work/without_macro.xml" <<EOF
<clickhouse>
    <tcp_port>9999</tcp_port>
    <remote_servers>
        <dump_schema_macro_cluster>
            <shard><replica><host>127.0.0.1</host><port>9999</port></replica></shard>
        </dump_schema_macro_cluster>
    </remote_servers>
</clickhouse>
EOF
$CLICKHOUSE_LOCAL --config-file "$work/with_macro.xml" --path "$work/local" -n -q "
    CREATE DATABASE d;
    CREATE TABLE d.zzz_src (id UInt64) ENGINE = MergeTree ORDER BY id;
    CREATE VIEW d.aaa_view AS SELECT * FROM remote(\`{dump_schema_cluster_macro}\`, 'd', 'zzz_src');"
$CLICKHOUSE_LOCAL --config-file "$work/with_macro.xml" --path "$work/local" --dump-schema d > "$work/local.sql" 2>/dev/null
echo "local with the macro: dump rc=$?, after the source: $(dumped_before "$work/local.sql" d.zzz_src d.aaa_view)"
$CLICKHOUSE_LOCAL --config-file "$work/without_macro.xml" --path "$work/local" --dump-schema d > /dev/null 2> "$work/local.err"
echo "local without the macro: dump rc=$?, refused as unexpandable: $(grep -c 'Cannot expand cluster name' "$work/local.err")"

# The `SETTINGS` clause stays in the stored calls. The readers through a table argument sort before their source as above.
$CLICKHOUSE_CLIENT -mq "
DROP DATABASE IF EXISTS ${DB};
CREATE DATABASE ${DB} ENGINE = Atomic;
CREATE TABLE ${DB}.zzz_src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE VIEW ${DB}.aaa_remote_settings
    AS SELECT * FROM remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}', ${DB}.zzz_src, SETTINGS skip_unavailable_shards = 1);
CREATE VIEW ${DB}.aab_cluster_settings
    AS SELECT * FROM cluster(\`{default_cluster_macro}\`, '${DB}', 'zzz_src', SETTINGS skip_unavailable_shards = 1);
CREATE VIEW ${DB}.default_remote
    AS SELECT dummy FROM remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}', SETTINGS skip_unavailable_shards = 1);
CREATE VIEW ${DB}.default_cluster AS SELECT dummy FROM cluster(\`{default_cluster_macro}\`, SETTINGS skip_unavailable_shards = 1);
"
$CLICKHOUSE_CLIENT --dump-schema="${DB}" > "$work/settings.sql" 2> "$work/settings.err"
echo "SETTINGS: dump rc=$?"
grep -o 'Code: [0-9]*' "$work/settings.err" | head -1
for reader in aaa_remote_settings aab_cluster_settings; do
    echo "${reader} after the source: $(dumped_before "$work/settings.sql" "${DB}.zzz_src" "${DB}.${reader}")"
done
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${DB}"
$CLICKHOUSE_CLIENT --queries-file "$work/settings.sql" 2>&1 | grep -o 'Code: [0-9]*' | head -1
echo "SETTINGS: replay rc=${PIPESTATUS[0]}"
$CLICKHOUSE_CLIENT -mq "
INSERT INTO ${DB}.zzz_src VALUES (1);
SELECT count() FROM ${DB}.aaa_remote_settings;
SELECT count() FROM ${DB}.aab_cluster_settings;
SELECT count() FROM ${DB}.default_remote;
SELECT count() FROM ${DB}.default_cluster;
"
