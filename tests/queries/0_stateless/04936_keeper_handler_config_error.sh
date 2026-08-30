#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs the keeper entrypoint of the binary under test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

if ! "$CLICKHOUSE_BINARY" help 2>&1 | grep -qF 'clickhouse keeper [args]'; then
    exit 0
fi

TMP_DIR="$(mktemp -d "${CLICKHOUSE_TMP}/keeper_prometheus_handler_config_error.XXXXXX")"
KEEPER_PID=""

# The storage and log directories must outlive the process that is using them, so a keeper left
# running is waited for before they are removed.
function cleanup()
{
    if [ -n "$KEEPER_PID" ] && kill -0 "$KEEPER_PID" 2>/dev/null; then
        kill "$KEEPER_PID" 2>/dev/null
        wait "$KEEPER_PID" 2>/dev/null
    fi

    rm -rf "$TMP_DIR"
}

trap cleanup EXIT

# Three distinct free ports are needed. `prometheus.port` has to be free for the listener to get as
# far as parsing its handlers, since a port already in use reports a genuine bind failure instead.
# Keeper also rejects a raft port equal to `keeper_server.tcp_port`, so neither can be left at 0.
#
# Ports are picked at random and retried on collision rather than probed for being free: nothing
# here is unique per concurrent copy of this test, so any derived port would be the same in every
# copy, and probe-then-bind is a race those copies lose against each other. Collisions are detected
# by binding, which is the only reliable test, so a retry always makes progress.
function pick_ports()
{
    PORT_BASE=$((32768 + RANDOM % 8000 * 4))
    PROM_PORT=$PORT_BASE
    KEEPER_PORT=$((PORT_BASE + 1))
    RAFT_PORT=$((PORT_BASE + 2))
}

# $1 = <handler> body, $2 = extra top-level config, $3 = config file, $4 = "no-port" to omit the port
function write_config()
{
    local port_element="<port>${PROM_PORT}</port>"
    [ "${4:-}" = no-port ] && port_element=""

    cat > "$3" <<EOF
<clickhouse>
    <listen_host>127.0.0.1</listen_host>
    $2
    <logger>
        <level>trace</level>
        <log>${TMP_DIR}/keeper.log</log>
        <errorlog>${TMP_DIR}/keeper.err.log</errorlog>
        <console>0</console>
    </logger>
    <prometheus>
        ${port_element}
        <handlers>
            <my_metrics>
                <url>/metrics</url>
                <handler>$1</handler>
            </my_metrics>
        </handlers>
    </prometheus>
    <keeper_server>
        <tcp_port>${KEEPER_PORT}</tcp_port>
        <server_id>1</server_id>
        <log_storage_path>${TMP_DIR}/coordination/log</log_storage_path>
        <snapshot_storage_path>${TMP_DIR}/coordination/snapshots</snapshot_storage_path>
        <coordination_settings>
            <operation_timeout_ms>10000</operation_timeout_ms>
            <session_timeout_ms>15000</session_timeout_ms>
            <raft_logs_level>warning</raft_logs_level>
        </coordination_settings>
        <raft_configuration>
            <server><id>1</id><hostname>127.0.0.1</hostname><port>${RAFT_PORT}</port></server>
        </raft_configuration>
    </keeper_server>
</clickhouse>
EOF
}

# Both exits of the listen error handler: plain, and `listen_try` which used to swallow it.
for listen_try in '' '<listen_try>1</listen_try>'; do
    if [ -z "$listen_try" ]; then echo '--- default'; else echo '--- listen_try'; fi

    # Any startup failure before the Prometheus listener would make the assertions below pass or
    # fail for an unrelated reason, so a port collision is retried rather than asserted on.
    for _ in $(seq 1 20); do
        pick_ports
        rm -rf "${TMP_DIR}/coordination" "${TMP_DIR}/keeper.err.log" "${TMP_DIR}/keeper.log"
        write_config '<type>no_such_prometheus_type</type>' "$listen_try" "${TMP_DIR}/keeper_config.xml"

        OUT="$(timeout 60 "$CLICKHOUSE_BINARY" keeper --config="${TMP_DIR}/keeper_config.xml" 2>&1)"
        RC=$?

        ALL="${OUT}$(cat "${TMP_DIR}/keeper.err.log" 2>/dev/null)"
        echo "$ALL" | grep -qE 'RAFT_ERROR|Address already in use' || break
    done

    echo "$ALL" | grep -qE 'RAFT_ERROR|Address already in use' \
        && echo 'FAIL: keeper stopped before the Prometheus listener'

    echo "$ALL" | grep -qF 'Unknown type no_such_prometheus_type' && echo 'reports the unknown type' \
        || echo 'FAIL: the unknown type is not reported'

    # Neither exit of the listen error handler may claim this is about the address: one wraps it in
    # NETWORK_ERROR, the other reduces it to advice about `<listen_host>`.
    echo "$ALL" | grep -qE 'NETWORK_ERROR|consider to' && echo 'FAIL: still blamed on the address' \
        || echo 'not blamed on the address'

    # An unusable Prometheus configuration must stop startup rather than run on without the
    # endpoint. 124 is the timeout above, meaning it was still running.
    [ "$RC" != 124 ] && echo 'refuses to start' || echo 'FAIL: started without the endpoint'
done

# Without `prometheus.port` there is no listener to configure, so a broken rule must not keep keeper
# down. This is the lock on the port check that keeps the parse out of that path.
echo '--- no port'
for _ in $(seq 1 20); do
    pick_ports
    rm -rf "${TMP_DIR}/coordination" "${TMP_DIR}/keeper.err.log" "${TMP_DIR}/keeper.log"
    write_config '<type>no_such_prometheus_type</type>' '' "${TMP_DIR}/keeper_config.xml" no-port
    "$CLICKHOUSE_BINARY" keeper --config="${TMP_DIR}/keeper_config.xml" >/dev/null 2>&1 &
    KEEPER_PID=$!

    READY=0
    for _ in $(seq 1 600); do
        grep -qF 'Ready for connections.' "${TMP_DIR}/keeper.log" 2>/dev/null && READY=1 && break
        kill -0 "$KEEPER_PID" 2>/dev/null || break
        sleep 0.1
    done

    [ "$READY" = 1 ] && break
    grep -qE 'RAFT_ERROR|Address already in use' "${TMP_DIR}/keeper.err.log" 2>/dev/null || break
    kill "$KEEPER_PID" 2>/dev/null; wait "$KEEPER_PID" 2>/dev/null; KEEPER_PID=""
done

[ "$READY" = 1 ] && echo 'starts anyway' || echo 'FAIL: kept down without a listener'
grep -qF 'Unknown type no_such_prometheus_type' "${TMP_DIR}/keeper.err.log" 2>/dev/null \
    && echo 'FAIL: the handler section was read' || echo 'handler section not read'
kill "$KEEPER_PID" 2>/dev/null; wait "$KEEPER_PID" 2>/dev/null; KEEPER_PID=""

# A valid section still serves metrics, so the hoist did not break the listener.
echo '--- valid'
STATUS=000
for _ in $(seq 1 20); do
    pick_ports
    rm -rf "${TMP_DIR}/coordination" "${TMP_DIR}/keeper.err.log" "${TMP_DIR}/keeper.log"
    write_config '<type>expose_metrics</type><metrics>true</metrics><events>true</events>' \
        '' "${TMP_DIR}/keeper_config.xml"
    "$CLICKHOUSE_BINARY" keeper --config="${TMP_DIR}/keeper_config.xml" >/dev/null 2>&1 &
    KEEPER_PID=$!

    for _ in $(seq 1 600); do
        STATUS="$(${CLICKHOUSE_CURL} -s -o /dev/null -w '%{http_code}' --max-time 2 \
                      "http://127.0.0.1:${PROM_PORT}/metrics" 2>/dev/null)"
        [ "$STATUS" = 200 ] && break
        kill -0 "$KEEPER_PID" 2>/dev/null || break
        sleep 0.1
    done

    [ "$STATUS" = 200 ] && break
    # Only a port collision is retried; anything else is reported as it is.
    grep -qE 'RAFT_ERROR|Address already in use' "${TMP_DIR}/keeper.err.log" 2>/dev/null || break
    kill "$KEEPER_PID" 2>/dev/null; wait "$KEEPER_PID" 2>/dev/null; KEEPER_PID=""
done

echo "metrics endpoint: ${STATUS}"
kill "$KEEPER_PID" 2>/dev/null; wait "$KEEPER_PID" 2>/dev/null; KEEPER_PID=""

# The HTTP control endpoints read a different section through a different factory, so they need
# their own cases. `prometheus.port` is left out here to keep this listener the only reader.
# $1 = <handler> body, $2 = extra top-level config, $3 = "no-port" to omit the port,
# "secure" to configure the separately built HTTPS listener instead
function write_control_config()
{
    local port_element="<port>${PROM_PORT}</port>"
    [ "${3:-}" = no-port ] && port_element=""
    [ "${3:-}" = secure ] && port_element="<secure_port>${PROM_PORT}</secure_port>"

    cat > "${TMP_DIR}/keeper_config.xml" <<EOF
<clickhouse>
    <listen_host>127.0.0.1</listen_host>
    $2
    <logger>
        <level>trace</level>
        <log>${TMP_DIR}/keeper.log</log>
        <errorlog>${TMP_DIR}/keeper.err.log</errorlog>
        <console>0</console>
    </logger>
    <keeper_server>
        <tcp_port>${KEEPER_PORT}</tcp_port>
        <server_id>1</server_id>
        <log_storage_path>${TMP_DIR}/coordination/log</log_storage_path>
        <snapshot_storage_path>${TMP_DIR}/coordination/snapshots</snapshot_storage_path>
        <http_control>
            ${port_element}
            <handlers>
                <rule>
                    <url>/probe</url>
                    <handler>$1</handler>
                </rule>
            </handlers>
        </http_control>
        <coordination_settings>
            <operation_timeout_ms>10000</operation_timeout_ms>
            <session_timeout_ms>15000</session_timeout_ms>
            <raft_logs_level>warning</raft_logs_level>
        </coordination_settings>
        <raft_configuration>
            <server><id>1</id><hostname>127.0.0.1</hostname><port>${RAFT_PORT}</port></server>
        </raft_configuration>
    </keeper_server>
</clickhouse>
EOF
}

for listen_try in '' '<listen_try>1</listen_try>'; do
    if [ -z "$listen_try" ]; then echo '--- control default'; else echo '--- control listen_try'; fi

    for _ in $(seq 1 20); do
        pick_ports
        rm -rf "${TMP_DIR}/coordination" "${TMP_DIR}/keeper.err.log" "${TMP_DIR}/keeper.log"
        write_control_config '<type>no_such_control_type</type>' "$listen_try"

        OUT="$(timeout 60 "$CLICKHOUSE_BINARY" keeper --config="${TMP_DIR}/keeper_config.xml" 2>&1)"
        RC=$?

        ALL="${OUT}$(cat "${TMP_DIR}/keeper.err.log" 2>/dev/null)"
        echo "$ALL" | grep -qE 'RAFT_ERROR|Address already in use' || break
    done

    echo "$ALL" | grep -qE 'RAFT_ERROR|Address already in use' \
        && echo 'FAIL: keeper stopped before the control listener'

    echo "$ALL" | grep -qF "Unknown handler type 'no_such_control_type'" \
        && echo 'reports the unknown type' || echo 'FAIL: the unknown type is not reported'

    echo "$ALL" | grep -qE 'NETWORK_ERROR|consider to' && echo 'FAIL: still blamed on the address' \
        || echo 'not blamed on the address'

    [ "$RC" != 124 ] && echo 'refuses to start' || echo 'FAIL: started without the endpoint'
done

# Without the port there is no listener to configure, so a broken rule must not keep keeper down.
echo '--- control no port'
for _ in $(seq 1 20); do
    pick_ports
    rm -rf "${TMP_DIR}/coordination" "${TMP_DIR}/keeper.err.log" "${TMP_DIR}/keeper.log"
    write_control_config '<type>no_such_control_type</type>' '' no-port
    "$CLICKHOUSE_BINARY" keeper --config="${TMP_DIR}/keeper_config.xml" >/dev/null 2>&1 &
    KEEPER_PID=$!

    READY=0
    for _ in $(seq 1 600); do
        grep -qF 'Ready for connections.' "${TMP_DIR}/keeper.log" 2>/dev/null && READY=1 && break
        kill -0 "$KEEPER_PID" 2>/dev/null || break
        sleep 0.1
    done

    [ "$READY" = 1 ] && break
    grep -qE 'RAFT_ERROR|Address already in use' "${TMP_DIR}/keeper.err.log" 2>/dev/null || break
    kill "$KEEPER_PID" 2>/dev/null; wait "$KEEPER_PID" 2>/dev/null; KEEPER_PID=""
done

[ "$READY" = 1 ] && echo 'starts anyway' || echo 'FAIL: kept down without a listener'
grep -qF "Unknown handler type 'no_such_control_type'" "${TMP_DIR}/keeper.err.log" 2>/dev/null \
    && echo 'FAIL: the handler section was read' || echo 'handler section not read'
kill "$KEEPER_PID" 2>/dev/null; wait "$KEEPER_PID" 2>/dev/null; KEEPER_PID=""

# A valid section still serves the endpoint, so the hoist did not break the listener.
echo '--- control valid'
STATUS=000
for _ in $(seq 1 20); do
    pick_ports
    rm -rf "${TMP_DIR}/coordination" "${TMP_DIR}/keeper.err.log" "${TMP_DIR}/keeper.log"
    write_control_config '<type>ready</type>' ''
    "$CLICKHOUSE_BINARY" keeper --config="${TMP_DIR}/keeper_config.xml" >/dev/null 2>&1 &
    KEEPER_PID=$!

    # The readiness handler binds its own configured path rather than the rule's <url>.
    for _ in $(seq 1 600); do
        STATUS="$(${CLICKHOUSE_CURL} -s -o /dev/null -w '%{http_code}' --max-time 2 \
                      "http://127.0.0.1:${PROM_PORT}/ready" 2>/dev/null)"
        [ "$STATUS" = 200 ] && break
        kill -0 "$KEEPER_PID" 2>/dev/null || break
        sleep 0.1
    done

    [ "$STATUS" = 200 ] && break
    grep -qE 'RAFT_ERROR|Address already in use' "${TMP_DIR}/keeper.err.log" 2>/dev/null || break
    kill "$KEEPER_PID" 2>/dev/null; wait "$KEEPER_PID" 2>/dev/null; KEEPER_PID=""
done

echo "control endpoint: ${STATUS}"
kill "$KEEPER_PID" 2>/dev/null; wait "$KEEPER_PID" 2>/dev/null; KEEPER_PID=""

# The HTTPS control listener is built by its own guarded copy of the construction, so moving
# only that one back inside the callback has to be caught here. No TLS material is configured:
# the handler section is read before any socket work, which is what this asserts.
echo '--- control secure'
for _ in $(seq 1 20); do
    pick_ports
    rm -rf "${TMP_DIR}/coordination" "${TMP_DIR}/keeper.err.log" "${TMP_DIR}/keeper.log"
    write_control_config '<type>no_such_control_type</type>' '' secure

    OUT="$(timeout 60 "$CLICKHOUSE_BINARY" keeper --config="${TMP_DIR}/keeper_config.xml" 2>&1)"
    RC=$?

    ALL="${OUT}$(cat "${TMP_DIR}/keeper.err.log" 2>/dev/null)"
    echo "$ALL" | grep -qE 'RAFT_ERROR|Address already in use' || break
done

echo "$ALL" | grep -qF "Unknown handler type 'no_such_control_type'" \
    && echo 'reports the unknown type' || echo 'FAIL: the unknown type is not reported'

echo "$ALL" | grep -qE 'NETWORK_ERROR|consider to' && echo 'FAIL: still blamed on the address' \
    || echo 'not blamed on the address'

[ "$RC" != 124 ] && echo 'refuses to start' || echo 'FAIL: started without the endpoint'

