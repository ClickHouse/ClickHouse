---
slug: /en/operations/system-tables/settings_changes
---
# settings_changes

Contains information about setting changes in previous ClickHouse versions.

Columns:

- `type` ([Enum](../../sql-reference/data-types/enum.md)) - The settings type: `Core` (general / query settings), `MergeTree`.
- `version` ([String](../../sql-reference/data-types/string.md)) — The ClickHouse version in which settings were changed
- `changes` ([Array](../../sql-reference/data-types/array.md) of [Tuple](../../sql-reference/data-types/tuple.md)) — A description of the setting changes: (setting name, previous value, new value, reason for the change)

**Example**

``` sql
SELECT *
FROM system.settings_changes
WHERE version = '23.5'
FORMAT Vertical
```

``` text
Row 1:
──────
type:    Core
version: 23.5
changes: [('input_format_parquet_preserve_order','1','0','Allow Parquet reader to reorder rows for better parallelism.'),('parallelize_output_from_storages','0','1','Allow parallelism when executing queries that read from file/url/s3/etc. This may reorder rows.'),('use_with_fill_by_sorting_prefix','0','1','Columns preceding WITH FILL columns in ORDER BY clause form sorting prefix. Rows with different values in sorting prefix are filled independently'),('output_format_parquet_compliant_nested_types','0','1','Change an internal field name in output Parquet file schema.')]
```

**See also**

- [Settings](../../operations/settings/index.md#session-settings-intro)
- [system.settings](settings.md)
