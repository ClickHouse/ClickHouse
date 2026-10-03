# Redis `SELECT` Mapping Implementation Plan

## Goal

Stage 3 adds connection-level Redis database selection without implementing key lookup yet:

- Support `SELECT <db>`.
- Map a Redis logical DB number to a ClickHouse `database`, `table`, and `default_column` from config.
- Store enough connection state for future `GET` and `MGET` implementation.

`GET`, `MGET`, and `IKeyValueEntity` integration are explicitly out of scope for this stage.

## Proposed Config Format

```xml
<redis>
    <db>
        <_0>
            <database>default</database>
            <table>kv_baseline</table>
            <default_column>value</default_column>
        </_0>
    </db>
</redis>
```

Expected Poco configuration paths:

- `redis.db._0.database`
- `redis.db._0.table`
- `redis.db._0.default_column`

The XML node uses `_0` because Poco XML configuration paths cannot naturally address a child named `0` as a property segment.

## Redis Command Behavior

- `SELECT 0` with valid config returns `+OK\r\n`.
- `SELECT <db>` where the mapped config path is missing returns a Redis-compatible `-ERR ...\r\n`.
- `SELECT` with the wrong number of arguments returns `-ERR wrong number of arguments ...\r\n`.
- `SELECT` with a non-integer DB index returns `-ERR invalid DB index\r\n`.
- `PING` and `QUIT` continue to work as they do in the skeleton.
- `GET` remains unsupported and continues returning an unknown-command style error in this stage.

## Implementation Notes

- `RedisHandler` needs access to `IServer` or its config again, instead of ignoring the constructor argument.
- `RedisHandler` should keep per-connection state:
  - `selected_db`, probably an integer or string representation.
  - selected target config containing `database`, `table`, and `default_column`.
- `SELECT` should validate the DB argument and load the corresponding config path, for example `redis.db._0`.
- The implementation should not resolve the ClickHouse table object yet unless required by existing helper APIs.
- Do not use `IKeyValueEntity` in this stage.
- Keep error handling consistent with the existing Redis skeleton: protocol parsing errors remain protocol errors; command validation errors are Redis error responses.

## Files Likely Changed Later

- `src/Server/RedisHandler.h`
- `src/Server/RedisHandler.cpp`
- `programs/server/config.xml`
- `programs/server/config.yaml.example`
- `project_notes/redis_select_mapping_status.md`

## Verification Commands

```bash
redis-cli -p 9006 PING
```

```bash
redis-cli -p 9006 SELECT 0
```

```bash
redis-cli -p 9006 SELECT 999
```

```bash
redis-cli -p 9006 GET some_key
```
