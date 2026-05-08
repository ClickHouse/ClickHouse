# Redis `GET` Implementation Status

Date: 2026-05-08

Current short SHA: `5ba08275d18`

## Implemented Scope

- `GET` command.
- `GET` uses the selected Redis DB mapping from `SELECT`.
- ClickHouse table is resolved from the configured `database` and `table`.
- Target table is checked for `IKeyValueEntity`.
- Single `String` primary key is supported.
- Single `String` `default_column` is supported.
- Existing key returns Redis bulk string.
- Missing key returns Redis null bulk string.
- Wrong number of arguments returns Redis error.
- No `MGET` yet.

## Runtime Config Used For Verification

- `redis_port=9006`
- `redis.db._0.database=default`
- `redis.db._0.table=kv_baseline`
- `redis.db._0.default_column=value`

## Manual Verification

Command:

```bash
printf 'SELECT 0\nGET key_000000001\n' | redis-cli -h 127.0.0.1 -p 9006
```

Observed:

```text
OK
value_000000001
```

Command:

```bash
printf 'SELECT 0\nGET missing_key\n' | redis-cli -h 127.0.0.1 -p 9006
```

Observed:

```text
OK

```

The blank line after `OK` is the Redis null bulk string as shown by `redis-cli`.

Command:

```bash
redis-cli -h 127.0.0.1 -p 9006 GET
```

Observed:

```text
ERR wrong number of arguments for 'get' command
```

Command:

```bash
redis-cli -h 127.0.0.1 -p 9006 GET a b
```

Observed:

```text
ERR wrong number of arguments for 'get' command
```

The server stayed alive after the checks. A final `SELECT 1` through the ClickHouse native client returned `1`, and `pgrep -a clickhouse` showed the fresh `build-new` server still running.

## Runtime Issue Diagnosed

Redis `GET` verification was inconsistent because two ClickHouse processes were running:

- one stale process served IPv4 `127.0.0.1:9006`;
- one newer process served IPv6 `::1:9006`;
- `redis-cli` reached different binaries depending on IPv4 or IPv6 resolution;
- the newer process also lacked the Redis DB mapping, so `SELECT 0` reported that Redis DB `0` was not configured.

Clean verification required stopping both processes and starting exactly one fresh `build-new` server with `/root/work/benchmarks/ch-runtime/config.xml`.

## Issues Recorded

The current `project_notes/redis_get_implementation_issues.md` records:

- `Build Target Name`: an earlier accidental `build` invocation used the unavailable `clickhouse-server` target; verification moved to available targets.
- `Sandbox ccache Write Failure`: the sandbox blocked `ccache` writes while building.
- `build-new Sandbox ccache Write Failure`: the same `ccache` sandbox restriction appeared in the correct `build-new` directory and was resolved by rerunning with escalated permissions.

## Next Planned Stage

- `MGET` over `IKeyValueEntity`.
- Tests.
- Final benchmark before and after.
