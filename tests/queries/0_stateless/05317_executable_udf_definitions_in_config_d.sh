#!/usr/bin/env bash
# Definition files of executable UDFs and UDF drivers that are placed in `config.d` are also merged into
# the server config, and the server must still start and load the functions.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_udf_config_d"
rm -rf "$work"
mkdir -p "$work/config.d"

cleanup() {
    if [ -s "$work/pid" ]; then
        local pid
        pid=$(cat "$work/pid")
        kill "$pid" 2>/dev/null
        for _ in {1..600}; do
            kill -0 "$pid" 2>/dev/null || break
            sleep 0.1
        done
    fi
    rm -rf "$work"
}
trap cleanup EXIT

cat > "$work/config.xml" <<EOF
<clickhouse>
    <logger>
        <level>information</level>
        <log>$work/server.log</log>
        <errorlog>$work/server.err.log</errorlog>
        <console>0</console>
    </logger>
    <listen_host>127.0.0.1</listen_host>
    <tcp_port>0</tcp_port>
    <path>$work/data/</path>
    <tmp_path>$work/data/tmp/</tmp_path>
    <user_files_path>$work/data/user_files/</user_files_path>
    <user_defined_executable_functions_config>config.d/*_function.*ml</user_defined_executable_functions_config>
    <user_defined_executable_function_drivers_config>config.d/*_driver.xml</user_defined_executable_function_drivers_config>
    <users>
        <default>
            <password></password>
            <profile>default</profile>
            <quota>default</quota>
            <networks><ip>127.0.0.1</ip></networks>
        </default>
    </users>
    <profiles><default/></profiles>
    <quotas><default/></quotas>
</clickhouse>
EOF

cat > "$work/config.d/xml_function.xml" <<'EOF'
<clickhouse>
    <function>
        <type>executable</type>
        <name>udf_config_d_first</name>
        <return_type>String</return_type>
        <argument><type>String</type></argument>
        <format>TabSeparated</format>
        <command>cat</command>
        <execute_direct>0</execute_direct>
    </function>
    <function>
        <type>executable</type>
        <name>udf_config_d_second</name>
        <return_type>String</return_type>
        <argument><type>String</type></argument>
        <format>TabSeparated</format>
        <command>cat</command>
        <execute_direct>0</execute_direct>
    </function>
</clickhouse>
EOF

cat > "$work/config.d/yaml_function.yaml" <<'EOF'
functions:
    type: executable
    name: udf_config_d_yaml
    return_type: String
    argument:
        type: String
    format: TabSeparated
    command: cat
    execute_direct: 0
EOF

# Drivers are experimental and disabled here, so this file only has to be merged into the server config.
cat > "$work/config.d/test_driver.xml" <<'EOF'
<clickhouse>
    <driver>
        <name>TestDriver</name>
        <create_command>/bin/true</create_command>
    </driver>
</clickhouse>
EOF

# The subshell keeps the server out of this shell's jobs, so stopping it prints nothing on stderr.
( $CLICKHOUSE_BINARY server --config-file="$work/config.xml" > "$work/stdout.log" 2>&1 & echo $! > "$work/pid" ) 2>/dev/null
pid=$(cat "$work/pid")

port=""
for _ in {1..1200}; do
    port=$(grep -oE 'Listening for native protocol \(tcp\): 127\.0\.0\.1:[0-9]+' "$work/server.log" 2>/dev/null | head -n1 | grep -oE '[0-9]+$')
    [ -n "$port" ] && break
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.1
done

if [ -z "$port" ]; then
    echo "The server did not start"
    grep -m1 -oE 'Code: [0-9]+\. DB::Exception: [^/]*' "$work/server.err.log" | sed 's/ *$//'
    exit 1
fi

for _ in {1..100}; do
    $CLICKHOUSE_CLIENT_BINARY --host 127.0.0.1 --port "$port" -q "SELECT 1" >/dev/null 2>&1 && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT_BINARY --host 127.0.0.1 --port "$port" -q "SELECT udf_config_d_first('a'), udf_config_d_second('b'), udf_config_d_yaml('c')"
