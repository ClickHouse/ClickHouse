---
toc_priority: 53
toc_title: WATCH
---

# WATCH Statement (Experimental) {#watch}

!!! important "Important"
    This is an experimental feature that may change in backwards-incompatible ways in the future releases.
    Enable live views and `WATCH` query using `set allow_experimental_live_view = 1`.


``` sql
WATCH [db.]live_view
[EVENTS]
[LIMIT n]
[FORMAT format]
```

The `WATCH` query performs continuous data retrieval from a [live view](./create/view.md#live-view) table.
Unless the `LIMIT` clause is specified it provides an infinite stream of query results
from a live view.

```sql
WATCH [db.]live_view
```

The virtual `_version` column in the query result indicates the current result version.

By default, the requested data is returned to the client, while in conjunction with [INSERT INTO](../../sql-reference/statements/insert-into.md)
it can be forwarded to a different table.

```sql
INSERT INTO [db.]table WATCH [db.]live_view ...
```

## EVENTS Clause {#events-clause}

The `EVENTS` clause can be used to obtain a short form of the `WATCH` query
where instead of the query result you will just get the latest query
result version.

```sql
WATCH [db.]live_view EVENTS LIMIT 1
```

## LIMIT Clause {#limit-clause}

The `LIMIT n` clause species the number of updates the `WATCH` query should wait
for before terminating. By default there is no limit on the number of updates and therefore
the query will not terminate. The value of `0`
indicates that the `WATCH` query should not wait for any new query results
and therefore will return immediately once query is evaluated.

```sql
WATCH [db.]live_view LIMIT 1
```

## FORMAT Clause {#format-clause}

The `FORMAT` clause works the same way as for the [SELECT](./select/index.md#format-clause).

!!! info "Note"
    The [JSONEachRowWithProgress](../../interfaces/formats/#jsoneachrowwithprogress) format should be used when watching [live view](./create/view.md#live-view) 
    tables over the HTTP interface. The progress messages will be added to the output
    to keep the long-lived HTTP connection alive until the query result changes. 
    The interval between progress messages is controlled using the [live_view_heartbeat_interval](./create/view.md#live-view-settings) setting.

