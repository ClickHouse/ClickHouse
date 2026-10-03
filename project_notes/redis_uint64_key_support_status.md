# Redis `UInt64` Key Support Status

## Metadata

- Date: 2026-05-09
- Short SHA: `f3509cd3fdd`

## Implemented Scope

- `UInt64` primary key support for `GET`.
- `UInt64` primary key support for `MGET`.
- `String` primary key path remains supported.
- `String` `default_column` remains the only supported value type.
- Composite keys are still unsupported.
- Non-`String` values are still unsupported.

## Test Target

```sql
CREATE TABLE bench.kv_test
(
    `key` UInt64,
    `value` String,
    `extra1` UInt32,
    `extra2` Float64
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY key
```

- Table: `bench.kv_test`
- Rows: 10000000
- Key column: `key UInt64`
- Value column: `value String`
- Extra columns: `extra1 UInt32`, `extra2 Float64`
- Engine: `EmbeddedRocksDB`

## Runtime Mapping Used

```text
redis.db._0.database=bench
redis.db._0.table=kv_test
redis.db._0.default_column=value
```

The runtime config kept `redis_port=9006`.

## Server State

Server liveness after checks:

```text
$ ./build-new/programs/clickhouse client --query "SELECT 1"
1
```

Running ClickHouse process:

```text
1232888 ./build-new/programs/clickhouse server --config-file=/root/work/benchmarks/ch-runtime/config.xml --pid-file=/root/work/benchmarks/ch-runtime/clickhouse-server.pid --daemon
```

Relevant listeners:

```text
LISTEN 0      4096            127.0.0.1:8123       0.0.0.0:*    users:(("clickhouse",pid=1232888,fd=65))
LISTEN 0      4096            127.0.0.1:9006       0.0.0.0:*    users:(("clickhouse",pid=1232888,fd=54))
LISTEN 0      4096            127.0.0.1:9000       0.0.0.0:*    users:(("clickhouse",pid=1232888,fd=71))
```

The server stayed alive after successful, missing-key, and invalid-key Redis checks.

## Manual Verification

### SQL Count

```text
$ ./build-new/programs/clickhouse client --query "SELECT count() FROM bench.kv_test"
10000000
```

### SQL Sample Keys

```text
$ ./build-new/programs/clickhouse client --query "SELECT key, value FROM bench.kv_test LIMIT 3"
0       broUQ4fAbnFbmOInLYaYRvj8vfg1MYTH9xI0M2KScosfogrOwxnq7OMkTkx OJR1Woctqn73tPy5CqpJqK4zn GfcgNXAey H92aTQHqOhBC83qFl52sOMjWHXHaub630td37fEeVWiDIq2AnHTmt OBGhovLoneNo52eoQni6JDXYlgBADTQ gzv2plCArp6B2Id 0961kEnzdx qXCA8 JTjs9KIVdKF1j8flLQoh4pLNAKG6mTpyQstVeC  en
65536   dIu4UgVdo1z4LIUkWRO7xkIJz5plC27gq0u68NzrMg9WhOy Yx2Cq7eTwTLzB1ALjnPYfCWXOBJwdofb HJp4iPxBufKH4vsfUWd7A44pM57Bs3zrPJnxPS0JIBZamkZ83gXU7mhYqW3nKnBsyauUnt7ooVUJOGwUC2agDBmI5kXJIv3Yse f4r2Oev2eK90I7M KQlkrQaZs0QeNG3GVCov6TwDjvAjXd6jpOJbRUgZACQkxhDkefhrk2rCGGdrrjpw4f0MS0VpyqBn1VkhnGw6h7gJI9N2UdANp6tEQU1ualIPGGh1YVoZ3cmu5Iv5VbeUKSPIbYAfYGpXnyeCqSAokzWXP1jnNTHyLVycVGum
131072  kJg39461Zi83cRgwx5gj6hr4qrou MDec7ETeDrJjScUfHAmoKlBIFtEaiglbqBfi3A2mSE5YUwPBzNtP6bXON3N7lKMG36e4ExWHn6oeEgz0AWtebSy8OmS2Ro4w6xqtFMCK4RaMQ8iVU peoj6nVzSrAG29qjM6xYCYHPNwX9ktD b8iAL0HOennMzQhD7 PzPxaFp1 9iWROZzv4tFQQ9kgSv5DyBaEzKnM6yhTe4ld26Oyt27WhP6rspIpdLLyEmlaxxS2ZWISR9yp pM7SdcAcvWbCM1ibJXvFLhomZN COr03oiC0fowP0xrmyLtpAlIkQAC3krvfEiWbRYHVPMVPIoo2gzgJqmRtZ0XF6DhjpwKro72UxcV4pvMgVNNMPm
```

### SQL Lookup For Existing Key

```text
$ ./build-new/programs/clickhouse client --query "SELECT value FROM bench.kv_test WHERE key = 0 LIMIT 1"
broUQ4fAbnFbmOInLYaYRvj8vfg1MYTH9xI0M2KScosfogrOwxnq7OMkTkx OJR1Woctqn73tPy5CqpJqK4zn GfcgNXAey H92aTQHqOhBC83qFl52sOMjWHXHaub630td37fEeVWiDIq2AnHTmt OBGhovLoneNo52eoQni6JDXYlgBADTQ gzv2plCArp6B2Id 0961kEnzdx qXCA8 JTjs9KIVdKF1j8flLQoh4pLNAKG6mTpyQstVeC  en
```

### Redis `GET` Existing `UInt64` Key

```text
$ printf 'SELECT 0\nGET 0\n' | redis-cli -h 127.0.0.1 -p 9006
OK
broUQ4fAbnFbmOInLYaYRvj8vfg1MYTH9xI0M2KScosfogrOwxnq7OMkTkx OJR1Woctqn73tPy5CqpJqK4zn GfcgNXAey H92aTQHqOhBC83qFl52sOMjWHXHaub630td37fEeVWiDIq2AnHTmt OBGhovLoneNo52eoQni6JDXYlgBADTQ gzv2plCArp6B2Id 0961kEnzdx qXCA8 JTjs9KIVdKF1j8flLQoh4pLNAKG6mTpyQstVeC  en
```

The Redis value matched the SQL lookup for `key = 0`.

### Redis `MGET` Existing `UInt64` Keys

```text
$ printf 'SELECT 0\nMGET 0 65536 131072\n' | redis-cli -h 127.0.0.1 -p 9006
OK
broUQ4fAbnFbmOInLYaYRvj8vfg1MYTH9xI0M2KScosfogrOwxnq7OMkTkx OJR1Woctqn73tPy5CqpJqK4zn GfcgNXAey H92aTQHqOhBC83qFl52sOMjWHXHaub630td37fEeVWiDIq2AnHTmt OBGhovLoneNo52eoQni6JDXYlgBADTQ gzv2plCArp6B2Id 0961kEnzdx qXCA8 JTjs9KIVdKF1j8flLQoh4pLNAKG6mTpyQstVeC  en
dIu4UgVdo1z4LIUkWRO7xkIJz5plC27gq0u68NzrMg9WhOy Yx2Cq7eTwTLzB1ALjnPYfCWXOBJwdofb HJp4iPxBufKH4vsfUWd7A44pM57Bs3zrPJnxPS0JIBZamkZ83gXU7mhYqW3nKnBsyauUnt7ooVUJOGwUC2agDBmI5kXJIv3Yse f4r2Oev2eK90I7M KQlkrQaZs0QeNG3GVCov6TwDjvAjXd6jpOJbRUgZACQkxhDkefhrk2rCGGdrrjpw4f0MS0VpyqBn1VkhnGw6h7gJI9N2UdANp6tEQU1ualIPGGh1YVoZ3cmu5Iv5VbeUKSPIbYAfYGpXnyeCqSAokzWXP1jnNTHyLVycVGum
kJg39461Zi83cRgwx5gj6hr4qrou MDec7ETeDrJjScUfHAmoKlBIFtEaiglbqBfi3A2mSE5YUwPBzNtP6bXON3N7lKMG36e4ExWHn6oeEgz0AWtebSy8OmS2Ro4w6xqtFMCK4RaMQ8iVU peoj6nVzSrAG29qjM6xYCYHPNwX9ktD b8iAL0HOennMzQhD7 PzPxaFp1 9iWROZzv4tFQQ9kgSv5DyBaEzKnM6yhTe4ld26Oyt27WhP6rspIpdLLyEmlaxxS2ZWISR9yp pM7SdcAcvWbCM1ibJXvFLhomZN COr03oiC0fowP0xrmyLtpAlIkQAC3krvfEiWbRYHVPMVPIoo2gzgJqmRtZ0XF6DhjpwKro72UxcV4pvMgVNNMPm
```

### Redis `GET` Missing Numeric Key

```text
$ printf 'SELECT 0\nGET 999999999999999999\n' | redis-cli -h 127.0.0.1 -p 9006
OK

```

The blank line after `OK` is `redis-cli` displaying a null bulk string for the missing key.

### Redis `GET` Invalid Non-Numeric Key

```text
$ printf 'SELECT 0\nGET abc\n' | redis-cli -h 127.0.0.1 -p 9006
OK
ERR invalid UInt64 key
```

### Redis `GET` Negative Key

```text
$ printf 'SELECT 0\nGET -1\n' | redis-cli -h 127.0.0.1 -p 9006
OK
ERR invalid UInt64 key
```

### Redis `GET` Overflow Key

```text
$ printf 'SELECT 0\nGET 18446744073709551616\n' | redis-cli -h 127.0.0.1 -p 9006
OK
ERR invalid UInt64 key
```

## Implementation Issues Encountered

See `project_notes/redis_uint64_key_implementation_issues.md`.

Important points:

- `ninja -C build-new clickhouse-server` was not a valid target in this build tree.
- `ninja -C build-new clickhouse-server-lib` compiled `src/Server/RedisHandler.cpp` and linked `src/libdbms.a`.
- Runtime verification initially still used a stale `./build-new/programs/clickhouse` executable, so the binary had to be relinked with `ninja -C build-new clickhouse`.
- After relinking and restarting the server, `UInt64` `GET` and `MGET` checks passed.

## Next Planned Stage

- Run benchmarks on `bench.kv_test`.
- Compare ClickHouse HTTP SQL against the ClickHouse Redis-compatible endpoint on the existing 10M-row table.
- Update the final benchmark summary with the 10M `UInt64` key results.
