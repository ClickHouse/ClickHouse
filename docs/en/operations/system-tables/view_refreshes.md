---
slug: /en/operations/system-tables/view_refreshes
---
# view_refreshes

Information about [Refreshable Materialized Views](../../sql-reference/statements/create/view.md#refreshable-materialized-view). Contains all refreshable materialized views, regardless of whether there's a refresh in progress or not.


Columns:

- `database` ([String](../../sql-reference/data-types/string.md)) — The name of the database the table is in.
- `view` ([String](../../sql-reference/data-types/string.md)) — Table name.
- `uuid` ([UUID](../../sql-reference/data-types/uuid.md)) — Table uuid (Atomic database).
- `status` ([String](../../sql-reference/data-types/string.md)) — Current state of the refresh.
- `last_success_time` ([Nullable](../../sql-reference/data-types/nullable.md)([DateTime](../../sql-reference/data-types/datetime.md))) — Time when the latest successful refresh started. NULL if no successful refreshes happened since server startup or table creation.
- `last_success_duration_ms` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — How long the latest refresh took.
- `last_refresh_time` ([Nullable](../../sql-reference/data-types/nullable.md)([DateTime](../../sql-reference/data-types/datetime.md))) — Time when the latest refresh attempt finished (if known) or started (if unknown or still running). NULL if no refresh attempts happened since server startup or table creation.
- `last_refresh_replica` ([String](../../sql-reference/data-types/string.md)) — If coordination is enabled, name of the replica that made the current (if running) or previous (if not running) refresh attempt.
- `next_refresh_time` ([Nullable](../../sql-reference/data-types/nullable.md)([DateTime](../../sql-reference/data-types/datetime.md))) — Time at which the next refresh is scheduled to start, if status = Scheduled.
- `exception` ([String](../../sql-reference/data-types/string.md)) — Error message from previous attempt if it failed.
- `retry` ([UInt64](../../sql-reference/data-types/int-uint.md)) — How many failed attempts there were so far, for the current refresh.
- `progress` ([Float64](../../sql-reference/data-types/float.md)) — Progress of the current refresh, between 0 and 1. Not available if status is `RunningOnAnotherReplica`.
- `read_rows` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Number of rows read by the current refresh so far. Not available if status is `RunningOnAnotherReplica`.
- `read_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Number of bytes read during the current refresh. Not available if status is `RunningOnAnotherReplica`.
- `total_rows` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Estimated total number of rows that need to be read by the current refresh. Not available if status is `RunningOnAnotherReplica`.
- `written_rows` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Number of rows written during the current refresh. Not available if status is `RunningOnAnotherReplica`.
- `written_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Number rof bytes written during the current refresh. Not available if status is `RunningOnAnotherReplica`.

**Example**

```sql
SELECT
    database,
    view,
    status,
    last_refresh_result,
    last_refresh_time,
    next_refresh_time
FROM system.view_refreshes

┌─database─┬─view───────────────────────┬─status────┬─last_refresh_result─┬───last_refresh_time─┬───next_refresh_time─┐
│ default  │ hello_documentation_reader │ Scheduled │ Finished            │ 2023-12-01 01:24:00 │ 2023-12-01 01:25:00 │
└──────────┴────────────────────────────┴───────────┴─────────────────────┴─────────────────────┴─────────────────────┘
```
