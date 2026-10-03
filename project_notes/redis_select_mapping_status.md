# Redis `SELECT` Mapping Status

Date: 2026-05-07

Current short SHA: `1240e42d4d4`

## Implemented Scope

- `SELECT` command.
- Redis DB index parsing.
- Config mapping through `redis.db._<n>.database`, `redis.db._<n>.table`, and `redis.db._<n>.default_column`.
- Selected target stored in connection state.
- No `GET` or `MGET` support yet.

## Runtime Config Used For Verification

- `redis_port=9006`
- `redis.db._0.database=default`
- `redis.db._0.table=kv_baseline`
- `redis.db._0.default_column=value`

## Manual Verification

```bash
redis-cli -p 9006 PING
```

Observed output:

```text
PONG
```

```bash
redis-cli -p 9006 SELECT 0
```

Observed output:

```text
OK
```

```bash
redis-cli -p 9006 SELECT 999
```

Observed output:

```text
ERR Redis DB 999 is not configured
```

```bash
redis-cli -p 9006 SELECT abc
```

Observed output:

```text
ERR invalid DB index
```

```bash
redis-cli -p 9006 GET some_key
```

Observed output:

```text
ERR unknown command 'GET'
```

The ClickHouse server stayed alive after all checks.

## Next Planned Stage

- `GET` over `IKeyValueEntity`.
- `MGET` over `IKeyValueEntity`.
