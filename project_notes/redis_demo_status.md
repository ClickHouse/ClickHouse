# Redis Demo Status

## Metadata

- Date: 2026-05-10
- Branch: `redis-handler`
- Current short SHA: `acbf950d019`

## Demo Files

- `demo/redis_endpoint_demo.py`
- `demo/README.md`

## Related Project Notes

- `project_notes/redis_demo_proof_ui_plan.md`
- `project_notes/redis_demo_current_state.md`
- `project_notes/redis_demo_implementation_issues.md`

`project_notes/redis_demo_ui_plan.md` is a stale duplicate of the earlier simpler demo plan and should not be included in the demo commit.

## Implemented UI Tabs

- Overview
- Compare `GET`/`MGET`
- Request Path
- Manual Console
- Proof / Live Verification
- Benchmark Context

## What The Demo Shows

- Regular ClickHouse HTTP SQL path.
- Redis-compatible read-only endpoint path.
- Actual SQL request.
- Actual Redis command sequence.
- Result equality.
- Illustrative client-side latency.
- Request path diagram.
- `system.query_log` proof for the SQL path.
- Static benchmark context.

## What Was Verified

- `python3 -m py_compile demo/redis_endpoint_demo.py` passed.
- Streamlit started on `0.0.0.0:8501` during smoke check.
- Dependencies are Streamlit plus Python standard library.
- Redis endpoint is visible on port `9006` via `ss`.
- `system.query_log` terminal check shows SQL path queries.
- Opening port `9006` in a browser returns a protocol error because it is RESP/TCP, not HTTP.

## How To Run

```bash
python3 -m streamlit run demo/redis_endpoint_demo.py --server.address 0.0.0.0 --server.port 8501
```

## Suggested Defense Flow

1. Open Overview.
2. Show port `9006` with `ss`.
3. Run SQL path.
4. Show SQL in `system.query_log`.
5. Run Redis endpoint path.
6. Show matching result.
7. Explain Request Path.
8. Show Benchmark Context with caveats.

## Remaining Limitations

- SQL console is documented as read-only but does not enforce `SELECT`-only.
- `system.query_log` can lag despite `SYSTEM FLUSH LOGS`.
- `RedisHandler` logs are environment-specific.
- UI latency is illustrative, not a benchmark replacement.
- Live browser check depends on running ClickHouse with correct config.

## Careful Wording

- Do not claim broad Redis compatibility.
- Do not claim ClickHouse is faster than Redis.
- Do not claim deployment-wide performance.
- Say that the endpoint removes much of SQL/HTTP overhead for supported `GET`/`MGET` point lookups over prepared key-value tables.
