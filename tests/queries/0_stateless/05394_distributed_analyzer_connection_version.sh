#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

user="${CLICKHOUSE_TEST_UNIQUE_NAME}_user"
table="distributed_analyzer_connection_version"
proxy_dir=$(mktemp -d "${CLICKHOUSE_TMP}/analyzer_connection_version.XXXXXX")
proxy_pid=
printf '<clickhouse/>\n' > "${proxy_dir}/client.xml"

cleanup()
{
    if [ -n "${proxy_pid}" ]; then
        kill "${proxy_pid}" 2>/dev/null || true
        wait "${proxy_pid}" 2>/dev/null || true
    fi
    rm -rf "${proxy_dir}"
    ${CLICKHOUSE_CLIENT} --multiquery --query "DROP TABLE IF EXISTS ${table}_dist; DROP TABLE IF EXISTS ${table}; DROP USER IF EXISTS ${user};"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --multiquery --query "
    DROP TABLE IF EXISTS ${table}_dist; DROP TABLE IF EXISTS ${table};
    DROP USER IF EXISTS ${user};
    CREATE USER ${user} SETTINGS allow_experimental_analyzer = 1, apply_settings_from_server = 1;
    CREATE TABLE ${table} (event_name String, event_count UInt64) ENGINE = Memory;
    INSERT INTO ${table} VALUES ('a', 2), ('a', 3), ('b', 7);
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.${table} TO ${user};
    GRANT REMOTE, CREATE TEMPORARY TABLE ON *.* TO ${user};
    CREATE TABLE ${table}_dist AS remote('${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_TCP},${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_TCP}', '${CLICKHOUSE_DATABASE}', '${table}', '${user}');
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.${table}_dist TO ${user};
"

mkfifo "${proxy_dir}/ports"
exec 3<> "${proxy_dir}/ports"
# A native client's application version is independent of its protocol revision.
# Negotiate revision 54460 (as in clickhouse-go 2.31.0) and replace only the
# application version in `Hello` and `ClientInfo`. All query/settings/data bytes are
# forwarded unchanged. One listener represents an application (`INITIAL_QUERY`),
# the other an old ClickHouse initiator (`SECONDARY_QUERY`).
python3 - "${CLICKHOUSE_HOST}" "${CLICKHOUSE_PORT_TCP}" "${proxy_dir}/ports" <<'PYTHON' &
import select
import socket
import socketserver
import sys
import threading


def read_exact(sock, size):
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise EOFError("Unexpected end of native packet")
        data.extend(chunk)
    return bytes(data)


def varuint(value):
    data = bytearray()
    while value > 127:
        data.append((value & 127) | 128)
        value >>= 7
    data.append(value)
    return bytes(data)


def read_varuint(sock):
    value = shift = 0
    while True:
        byte = read_exact(sock, 1)[0]
        value |= (byte & 127) << shift
        if byte < 128:
            return value
        shift += 7


def read_string(sock):
    size = read_varuint(sock)
    return varuint(size) + read_exact(sock, size)


def expect(value, expected):
    if value != expected:
        raise ValueError(f"Unexpected native field: {value!r}, expected {expected!r}")


class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        client = self.request
        with socket.create_connection((sys.argv[1], int(sys.argv[2])), timeout=10) as server:
            major_version, minor_version = self.server.application_version
            expect(read_varuint(client), 0)  # Client Hello.
            name = read_string(client)
            for _ in range(3):
                read_varuint(client)  # Original major, minor, revision.
            server.sendall(b"\x00" + name + varuint(major_version) + varuint(minor_version) + varuint(54460)
                           + b"".join(read_string(client) for _ in range(3)))

            expect(read_varuint(server), 0)  # Server Hello.
            name = read_string(server)
            major, minor = read_varuint(server), read_varuint(server)
            read_varuint(server)  # Advertise the negotiated revision to the client.
            client.sendall(b"\x00" + name + varuint(major) + varuint(minor) + varuint(54460)
                           + read_string(server) + read_string(server)
                           + varuint(read_varuint(server)))
            server.sendall(read_string(client))  # Quota-key addendum.

            packet = read_varuint(client)
            while packet == 4:  # Client Ping can precede the first query.
                server.sendall(varuint(packet))
                expect(read_varuint(server), 4)  # Server Pong.
                client.sendall(varuint(4))
                packet = read_varuint(client)
            expect(packet, 1)  # Query.
            header = b"\x01" + read_string(client)
            expect(read_exact(client, 1), b"\x01")  # Initial query from clickhouse-client.
            header += bytes([self.server.query_kind])
            header += b"".join(read_string(client) for _ in range(3))
            header += read_exact(client, 8)  # Initial-query start time.
            expect(read_exact(client, 1), b"\x01")  # TCP interface.
            header += b"\x01" + b"".join(read_string(client) for _ in range(3))
            for _ in range(3):
                read_varuint(client)
            header += varuint(major_version) + varuint(minor_version) + varuint(54460)
            header += read_string(client)  # Quota key.
            header += varuint(read_varuint(client))  # Distributed depth.
            read_varuint(client)  # Application patch version.
            header += varuint(0)
            server.sendall(header)

            while True:
                ready, _, _ = select.select([client, server], [], [], 30)
                if not ready:
                    raise TimeoutError("Native proxy timed out")
                for source in ready:
                    data = source.recv(65536)
                    if not data:
                        return
                    (server if source is client else client).sendall(data)


servers = []
for query_kind, version in ((1, (2, 31)), (2, (22, 8))):
    server = socketserver.ThreadingTCPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    server.query_kind = query_kind
    server.application_version = version
    threading.Thread(target=server.serve_forever, daemon=True).start()
    servers.append(server)
with open(sys.argv[3], "w") as ready:
    ready.write(" ".join(str(server.server_address[1]) for server in servers) + "\n")
threading.Event().wait()
PYTHON
proxy_pid=$!
if ! read -r -t 30 application_port old_server_port <&3
then
    echo 'Timed out waiting for the native-protocol proxy' >&2
    exit 1
fi
exec 3>&-

# Two shards require aggregate-state blocks. Repeating on pooled interserver
# connections also exercises the previous-query-kind state of `TCPHandler`.
for analyzer in 1 0
do
    for attempt in 1 2
    do
        ${CLICKHOUSE_CLIENT_BINARY} --config-file "${proxy_dir}/client.xml" \
            --database "${CLICKHOUSE_DATABASE}" --host 127.0.0.1 --port "${application_port}" --user "${user}" \
            --allow_experimental_analyzer "${analyzer}" --prefer_localhost_replica 0 \
            --max_distributed_connections 2 --max_threads 2 --use_query_cache 0 \
            --max_memory_usage 1073741824 --max_execution_time 10 \
            --timeout_before_checking_execution_speed 0 --max_rows_to_read 1000000 \
            --max_bytes_to_read 1000000000 --receive_timeout 10 --query "
                SELECT events.event_name AS name, sum(events.event_count) AS total
                FROM ${table}_dist AS events
                GROUP BY 1 WITH TOTALS ORDER BY name FORMAT TSV
            "
    done
done

# Use a clean client config above so runner-injected analyzer settings cannot
# turn the following implicit-setting check into an explicit-setting check.
# An actual old server, unlike a low-version application, still needs the
# compatibility fallback when it does not send an explicit analyzer setting.
for attempt in 1 2
do
    ${CLICKHOUSE_CLIENT_BINARY} --config-file "${proxy_dir}/client.xml" \
        --database "${CLICKHOUSE_DATABASE}" --host 127.0.0.1 --port "${old_server_port}" --user "${user}" \
        --receive_timeout 10 --query "SELECT 'old_server', toUInt8(getSetting('allow_experimental_analyzer'))"
done

# Explicit settings from an older initiator must still take precedence.
${CLICKHOUSE_CLIENT_BINARY} --config-file "${proxy_dir}/client.xml" \
    --database "${CLICKHOUSE_DATABASE}" --host 127.0.0.1 --port "${old_server_port}" --user "${user}" \
    --allow_experimental_analyzer 1 --receive_timeout 10 \
    --query "SELECT 'explicit_analyzer', toUInt8(getSetting('allow_experimental_analyzer'))"
