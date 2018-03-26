# system.settings

Contains information about settings that are currently in use.
I.e. used for executing the query you are using to read from the system.settings table).

Columns:

```text
name String  – Setting name.
value String  – Setting value.
changed UInt8 -–Whether the setting was explicitly defined in the config or explicitly changed.
```

Example:

```sql
SELECT *
FROM system.settings
WHERE changed
```

```text
┌─name───────────────────┬─value───────┬─changed─┐
│ max_threads            │ 8           │       1 │
│ use_uncompressed_cache │ 0           │       1 │
│ load_balancing         │ random      │       1 │
│ max_memory_usage       │ 10000000000 │       1 │
└────────────────────────┴─────────────┴─────────┘
```

