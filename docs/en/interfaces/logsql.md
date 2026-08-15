---
slug: /interfaces/logsql
sidebar_position: 10
---

# LogsQL dialect

ClickHouse provides an experimental `logsql` dialect for querying log tables with
[VictoriaLogs LogsQL](https://docs.victoriametrics.com/victorialogs/logsql/) syntax.
It translates a LogsQL query to a ClickHouse `SELECT` query.

Enable it for a session and configure the source table:

```sql
SET allow_experimental_logsql_dialect = 1;
SET logsql_database = 'default';
SET logsql_table = 'logs';
SET logsql_time_column = '_time';
SET logsql_message_column = '_msg';
SET dialect = 'logsql';
```

The configured table must contain the time and message columns. LogsQL filters,
stream filters, text and numeric comparisons, time ranges, and pipe operations
such as `fields`, `stats`, `sort`, `limit`, `format`, `extract`, and `unpack_json`
are supported. Field values used by text filters are converted to their LogsQL
string representation, so typed log columns may be searched too.

This dialect is experimental and is not a byte-for-byte implementation of
VictoriaLogs. Unsupported syntax is rejected with `NOT_IMPLEMENTED`; query
semantics can also differ where ClickHouse types, regular expressions, time
zones, or SQL aggregation rules differ. Use ordinary ClickHouse SQL when a
LogsQL construct is not supported or exact VictoriaLogs compatibility is needed.
