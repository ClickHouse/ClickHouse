#!/usr/bin/env bash
# Tags: no-darwin

# --dump-schema places a remote() address from the server's metadata only, so it never logs in with a stored login.
# A private server records every login in session_log, and 0.0.0.0 reaches it, so any attempt would show there.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_server"
rm -rf "$work"
mkdir -p "$work"

pid=""
cleanup() {
    if [ -n "$pid" ]; then
        kill "$pid" 2>/dev/null
        wait "$pid" 2>/dev/null
    fi
    rm -rf "$work"
}
trap cleanup EXIT

# The dump compares an address's port with tcpPort(), so the server needs a fixed one.
port=$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')

cat > "$work/config.xml" <<EOF
<clickhouse>
    <logger>
        <level>information</level>
        <log>$work/server.log</log>
        <errorlog>$work/server.err.log</errorlog>
        <console>0</console>
    </logger>
    <listen_host>0.0.0.0</listen_host>
    <tcp_port>$port</tcp_port>
    <path>$work/data/</path>
    <tmp_path>$work/data/tmp/</tmp_path>
    <user_files_path>$work/data/user_files/</user_files_path>
    <display_secrets_in_show_and_select>1</display_secrets_in_show_and_select>
    <session_log>
        <database>system</database>
        <table>session_log</table>
    </session_log>
    <users>
        <default>
            <password>default_secret</password>
            <profile>default</profile>
            <quota>default</quota>
            <networks><ip>::/0</ip></networks>
        </default>
        <dumper>
            <password></password>
            <profile>default</profile>
            <quota>default</quota>
            <networks><ip>::/0</ip></networks>
            <named_collection_control>1</named_collection_control>
            <show_named_collections_secrets>1</show_named_collections_secrets>
        </dumper>
        <reader>
            <password>reader_secret</password>
            <profile>default</profile>
            <quota>default</quota>
            <networks><ip>::/0</ip></networks>
        </reader>
    </users>
    <profiles><default/></profiles>
    <quotas><default/></quotas>
</clickhouse>
EOF

$CLICKHOUSE_BINARY server --config-file="$work/config.xml" > "$work/stdout.log" 2>&1 &
pid=$!

client() { $CLICKHOUSE_CLIENT_BINARY --host 127.0.0.1 --port "$port" --user dumper "$@"; }
for _ in {1..1200}; do
    client -q "SELECT 1" > /dev/null 2>&1 && break
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.1
done
if ! client -q "SELECT 1" > /dev/null 2>&1; then
    echo "The server did not start"
    exit 1
fi

# 0.0.0.0 is not one of the server's own addresses: a read through it goes over the network and logs in.
client --multiquery --query "
CREATE DATABASE creds;
CREATE TABLE creds.zzz_src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE VIEW creds.aaa_login_reader (id UInt64) AS SELECT * FROM remote('0.0.0.0:${port}', 'creds', 'zzz_src', 'reader', 'reader_secret');
CREATE NAMED COLLECTION nc_self AS host = '0.0.0.0', port = ${port}, user = 'reader', password = 'reader_secret', db = 'creds', table = 'zzz_src';
CREATE VIEW creds.aab_collection_reader (id UInt64) AS SELECT * FROM remote(nc_self);
CREATE DATABASE masked;
CREATE TABLE masked.zzz_src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE VIEW masked.aaa_login_reader (id UInt64) AS SELECT * FROM remote('0.0.0.0:${port}', 'masked', 'zzz_src', 'reader', 'reader_secret');
"
since=$(client -q "SELECT now64(6)")
logins() {
    client -q "SYSTEM FLUSH LOGS session_log"
    client -q "SELECT count() FROM system.session_log WHERE event_time_microseconds > toDateTime64('${since}', 6) AND user IN ($1)"
}

echo '--- the dump never logs in with a remote() call stored login ---'
dump_file="$work/dump.sql"
if client --format_display_secrets_in_show_and_select=1 --dump-schema=creds > "$dump_file" 2>"$work/err"; then
    src_line=$(grep -n "CREATE TABLE creds\.zzz_src" "$dump_file" | head -1 | cut -d: -f1)
    for reader in aaa_login_reader aab_collection_reader; do
        reader_line=$(grep -n "CREATE VIEW creds\.${reader} " "$dump_file" | head -1 | cut -d: -f1)
        if [ -z "$src_line" ] || [ -z "$reader_line" ]; then
            echo "FAIL: ${reader} or its source missing (src=$src_line reader=$reader_line)"
        elif [ "$src_line" -lt "$reader_line" ]; then
            echo "${reader}: local edge"
        else
            echo "${reader}: no local edge"
        fi
    done
    echo "readers warned: $(grep -c "^Warning: creds\.[a-z_]* reads creds\.zzz_src, which is in this dump" "$work/err")"
else
    echo "FAIL: dump rejected: $(cat "$work/err")"
fi
# Without the format setting the stored password reads back as [HIDDEN]; the dump needs no login anyway.
if client --dump-schema=masked > "$dump_file" 2>"$work/err"; then
    echo "masked reader warned: $(grep -cF "Warning: masked.aaa_login_reader reads masked.zzz_src, which is in this dump, through remote('0.0.0.0:${port}', ...)" "$work/err")"
else
    echo "FAIL: dump rejected: $(cat "$work/err")"
fi
echo "logins by the stored or default user during the dumps: $(logins "'reader', 'default'")"

echo '--- a remote() read through 0.0.0.0 does log in as the stored user ---'
client -q "SELECT count() FROM remote('0.0.0.0:${port}', 'system', 'one', 'reader', 'reader_secret')"
echo "logins by the stored user: $(logins "'reader'" | sed 's/^[1-9][0-9]*$/some/')"
