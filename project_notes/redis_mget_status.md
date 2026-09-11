# Redis `MGET` Implementation Status

Date: 2026-05-08

Current short SHA: `5ba08275d18`

## Implemented Scope

- `MGET` command.
- `MGET` uses the selected Redis DB mapping from `SELECT`.
- ClickHouse table is resolved from the configured `database` and `table`.
- Target table is checked for `IKeyValueEntity`.
- Single `String` primary key is supported.
- Single `String` `default_column` is supported.
- Existing keys return Redis bulk strings.
- Missing keys return Redis null bulk strings.
- Result order follows input key order.
- Wrong number of arguments returns Redis error.
- `GET` remains working.

## Runtime Config Used For Verification

- `redis_port=9006`
- `redis.db._0.database=default`
- `redis.db._0.table=kv_baseline`
- `redis.db._0.default_column=value`

## Manual Verification

Command:

```bash
printf 'SELECT 0\nMGET key_000000001 key_000000002 key_000000003\n' | redis-cli -h 127.0.0.1 -p 9006
```

Observed:

```text
OK
value_000000001
value_000000002
value_000000003
```

Command:

```bash
printf 'SELECT 0\nMGET key_000000001 missing_key key_000000003\n' | redis-cli -h 127.0.0.1 -p 9006
```

Observed:

```text
OK
value_000000001

value_000000003
```

The blank line is the Redis null bulk string as shown by `redis-cli`.

Command:

```bash
redis-cli -h 127.0.0.1 -p 9006 MGET
```

Observed:

```text
ERR wrong number of arguments for 'mget' command
```

Command:

```bash
redis-cli -h 127.0.0.1 -p 9006 MGET key_without_select
```

Observed:

```text
ERR no Redis DB selected
```

Command:

```bash
printf 'SELECT 0\nGET key_000000001\n' | redis-cli -h 127.0.0.1 -p 9006
```

Observed:

```text
OK
value_000000001
```

The server stayed alive after the checks. A final `SELECT 1` through the ClickHouse native client returned `1`, and `pgrep -a clickhouse` showed the fresh `build-new` server still running.

## Issues Recorded

`project_notes/redis_mget_implementation_issues.md` currently contains no stage-specific compile or runtime issues beyond the placeholder heading.

## Next Planned Stage

- Integration tests.
- Final before/after benchmark.
- Optional limits, pipelining, `HGET`, and `HMGET`.
