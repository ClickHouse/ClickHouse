---
slug: /en/operations/system-tables/view_refreshes
---
# view_refreshes

Information about [Refreshable Materialized Views](../../sql-reference/statements/create/view.md#refreshable-materialized-view). Contains all refreshable materialized views, regardless of whether there's a refresh in progress or not.


Columns:

- `database` ([String](../../sql-reference/data-types/string.md)) — The name of the database the table is in.
- `view` ([String](../../sql-reference/data-types/string.md)) — Table name.
- `status` ([String](../../sql-reference/data-types/string.md)) — Current state of the refresh.
- `last_refresh_result` ([String](../../sql-reference/data-types/string.md)) — Outcome of the latest refresh attempt.
- `last_refresh_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Time of the last successful refresh. `NULL` if no refresh succeeded since server startup or table creation.
- `next_refresh_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Time at which the next refresh is scheduled to start.
- `remaining_dependencies` ([Array(String)](../../sql-reference/data-types/array.md)) — If the view has [refresh dependencies](../../sql-reference/statements/create/view.md#refresh-dependencies), this array contains the subset of those dependencies that are not satisfied for the current refresh yet. If `status = 'WaitingForDependencies'`, a refresh is ready to start as soon as these dependencies are fulfilled.
- `exception` ([String](../../sql-reference/data-types/string.md)) — if `last_refresh_result = 'Exception'`, i.e. the last refresh attempt failed, this column contains the corresponding error message and stack trace.
- `progress` ([Float64](../../sql-reference/data-types/float.md)) — Progress of the current refresh, between 0 and 1.
- `read_rows` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Number of rows read by the current refresh so far.
- `total_rows` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Estimated total number of rows that need to be read by the current refresh.

(There are additional columns related to current refresh progress, but they are currently unreliable.)

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
