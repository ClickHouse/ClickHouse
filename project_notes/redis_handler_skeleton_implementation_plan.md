# Redis-Compatible TCP Skeleton Implementation Plan

## Scope

Implement a minimal Redis-compatible TCP endpoint for ClickHouse, gated by a new `redis_port` config key. This stage should only provide a small RESP command skeleton and server registration, with no data access or SQL execution.

## 1. Server Registration Points

Use `programs/server/Server.cpp` as the registration reference.

- Add `#include <Server/RedisHandlerFactory.h>` near the existing protocol factory includes, especially `TCPHandlerFactory.h`, `MySQLHandlerFactory.h`, and `PostgreSQLHandlerFactory.h`.
- Add a `redis_port` block inside `Server::createServers`, next to the compatibility protocol listeners:
  - `tcp_port` block around the native `TCPHandlerFactory` setup.
  - `mysql_port` block using `MySQLHandlerFactory`.
  - `postgresql_port` block using `PostgreSQLHandlerFactory`.
- Follow the normal `createServer` pattern:
  - `port_name = "redis_port";`
  - `socketBindListen(server_settings, socket, listen_host, port)`
  - `socket.setReceiveTimeout(settings[Setting::receive_timeout])`
  - `socket.setSendTimeout(settings[Setting::send_timeout])`
  - `ProtocolServerAdapter(listen_host, port_name, "Redis compatibility protocol: " + address.toString(), std::make_unique<TCPServer>(new RedisHandlerFactory(...), server_pool, socket, makeServerParams(server_settings), connection_filter))`
- `createServer` already handles an omitted config key by returning early, starts the listener, logs `Listening for ...`, and registers the port through `global_context->registerServerPort`.
- Runtime config reload support should be considered because `Server::reloadConfiguration` stops/starts listeners by port name. If `redis_port` is a first-class listener, `ServerType` should also recognize it.
- Update `src/Server/ServerType.h` and `src/Server/ServerType.cpp` during implementation:
  - Add `REDIS` to `ServerType::Type`.
  - Include `REDIS` in `is_type_default`.
  - Map `redis_port` to `Type::REDIS` in `ServerType::shouldStop`.

## 2. Relevant Handler And Factory Examples

Most relevant examples under `src/Server`:

- `TCPServerConnectionFactory.h`: base factory interface used by `TCPServer`.
- `TCPServer.h` and `TCPServer.cpp`: wrapper around `Poco::Net::TCPServer` that passes `TCPServer` into handler factories and handles listener shutdown.
- `TCPHandlerFactory.h`: simple factory pattern, peer-address logging, `DummyTCPHandler` fallback for `Poco::Net::NetException`.
- `KeeperTCPHandlerFactory.h`: compact factory and handler construction example for a custom TCP protocol that does not use the native query protocol.
- `MySQLHandlerFactory.h` and `MySQLHandlerFactory.cpp`: best compatibility-protocol factory reference, including connection id allocation and protocol-specific log naming.
- `PostgreSQLHandlerFactory.h` and `PostgreSQLHandlerFactory.cpp`: another compatibility-protocol factory reference with per-connection ids and read/write profile events.
- `MySQLHandler.h`/`MySQLHandler.cpp` and `PostgreSQLHandler.h`/`PostgreSQLHandler.cpp`: examples for handlers deriving from `Poco::Net::TCPServerConnection`, owning socket read/write buffers, and implementing `run`.

For the Redis skeleton, keep the handler much smaller than `MySQLHandler` and `PostgreSQLHandler`: parse one RESP array command at a time, write a simple RESP response, and loop until `QUIT`, EOF, timeout, or protocol error.

## 3. Build File Changes

`src/Server/CMakeLists.txt` probably does not need changes for `RedisHandler.cpp`, `RedisHandlerFactory.cpp`, or `RedisProtocol.cpp`.

Reason: `src/CMakeLists.txt` uses `add_object_library(clickhouse_server Server)`, and `cmake/dbms_glob_sources.cmake` defines source/header globbing with `CONFIGURE_DEPENDS`. New `*.cpp` and `*.h` files directly under `src/Server` should be collected automatically for the `clickhouse_server` object library.

Build-related files that may need changes during implementation:

- `programs/server/Server.cpp` for the listener registration and include.
- `src/Server/ServerType.h` and `src/Server/ServerType.cpp` for runtime start/stop classification of `redis_port`.
- Config examples may eventually need updates, but they are not required for a minimal skeleton if `redis_port` is optional and can be supplied on the command line as `--redis_port=9006`.

## 4. Proposed New Files

- `src/Server/RedisHandler.h`
  - Declare `RedisHandler : public Poco::Net::TCPServerConnection`.
  - Store `IServer &`, `TCPServer &`, logger, socket buffers, and optional connection id.
  - Implement `run` as the connection loop.

- `src/Server/RedisHandler.cpp`
  - Construct `ReadBufferFromPocoSocket` and `WriteBufferFromPocoSocket`.
  - Call `RedisProtocol` parser for each request.
  - Dispatch only `PING`, `QUIT`, unsupported command, and malformed request responses.
  - Flush after every response.

- `src/Server/RedisHandlerFactory.h`
  - Declare `RedisHandlerFactory : public TCPServerConnectionFactory`.
  - Store `IServer &`, logger, and an atomic connection id counter.

- `src/Server/RedisHandlerFactory.cpp`
  - Log peer address and connection id.
  - Return `new RedisHandler(server, tcp_server, socket, connection_id)`.
  - Optionally mirror `TCPHandlerFactory`/`KeeperTCPHandlerFactory` with a `DummyTCPHandler` for `Poco::Net::NetException`.

- `src/Server/RedisProtocol.h`
  - Define a tiny parser result type, for example `Command`, `Malformed`, and `EndOfStream`.
  - Expose parsing for RESP array bulk-string commands.
  - Expose response constants/helpers for simple strings and errors.

- `src/Server/RedisProtocol.cpp`
  - Parse the minimal RESP form used by Redis clients: `*<argc>\r\n$<len>\r\n<bytes>\r\n...`.
  - Normalize command names case-insensitively.
  - Reject malformed arrays, lengths, missing `\r\n`, unsupported inline protocol, and incomplete frames as protocol errors or EOF as appropriate.

## 5. Minimal Command Behavior

- `PING` returns `+PONG\r\n`.
- `QUIT` returns `+OK\r\n` and closes the connection after flushing.
- Any well-formed but unsupported command returns `-ERR unknown command\r\n`.
- Any malformed request returns `-ERR Protocol error\r\n`.

Notes:

- `PING` with arguments can be treated as unsupported or protocol-valid unknown behavior in this skeleton. The simplest stage-2 behavior is to accept only exactly one bulk string command, so `PING message` is not required.
- Request parsing should handle command names case-insensitively because Redis commands are conventionally case-insensitive.
- Do not route commands into ClickHouse sessions or query execution in this stage.

## 6. Proposed Config Key

Use top-level `redis_port`.

Initial manual usage can rely on a command-line override:

```bash
./clickhouse server -- --redis_port=9006
```

Default config updates can be deferred unless the implementation stage explicitly requires `programs/server/config.xml`, `programs/server/config.yaml.example`, or generated docs.

## 7. Build Target

After implementation, build the server binary target:

```bash
ninja clickhouse
```

Per repository instructions, run it from the build directory and redirect output to a build log file in that build directory.

## 8. Manual Verification Commands

Start ClickHouse with `redis_port` enabled, for example on `9006`, then verify:

```bash
redis-cli -p 9006 PING
```

Expected output:

```text
PONG
```

Raw RESP verification:

```bash
printf '*1\r\n$4\r\nPING\r\n' | nc 127.0.0.1 9006
```

Expected output bytes:

```text
+PONG
```

The actual response must end with `\r\n`.
