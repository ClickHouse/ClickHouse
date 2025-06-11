---
description: 'System table containing information about all cached file schemas.'
keywords: ['system table', 'schema_inference_cache']
slug: /operations/system-tables/schema_inference_cache
title: 'system.schema_inference_cache'
---

import SystemTableCloud from '@site/docs/_snippets/_system_table_cloud.md';

# system.schema_inference_cache

<SystemTableCloud/>

Contains information about all cached file schemas.

Columns:
- `storage` ([String](/sql-reference/data-types/string.md)) — Storage name: File, URL, S3 or HDFS.
- `source` ([String](/sql-reference/data-types/string.md)) — File source.
- `format` ([String](/sql-reference/data-types/string.md)) — Format name.
- `additional_format_info` ([String](/sql-reference/data-types/string.md)) - Additional information required to identify the schema. For example, format specific settings.
- `registration_time` ([DateTime](/sql-reference/data-types/datetime.md)) — Timestamp when schema was added in cache.
- `schema` ([String](/sql-reference/data-types/string.md)) - Cached schema.

**Example**

Let's say we have a file `data.jsonl` with this content:
```json
{"id" :  1, "age" :  25, "name" :  "Josh", "hobbies" :  ["football", "cooking", "music"]}
{"id" :  2, "age" :  19, "name" :  "Alan", "hobbies" :  ["tennis", "art"]}
{"id" :  3, "age" :  32, "name" :  "Lana", "hobbies" :  ["fitness", "reading", "shopping"]}
{"id" :  4, "age" :  47, "name" :  "Brayan", "hobbies" :  ["movies", "skydiving"]}
```

:::tip
Place `data.jsonl` in the `user_files_path` directory.  You can find this by looking
in your ClickHouse configuration files. The default is:
```sql
<user_files_path>/var/lib/clickhouse/user_files/</user_files_path>
```
:::

Open `clickhouse-client` and run the `DESCRIBE` query:

```sql
DESCRIBE file('data.jsonl') SETTINGS input_format_try_infer_integers=0;
```

```response
┌─name────┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ id      │ Nullable(Float64)       │              │                    │         │                  │                │
│ age     │ Nullable(Float64)       │              │                    │         │                  │                │
│ name    │ Nullable(String)        │              │                    │         │                  │                │
│ hobbies │ Array(Nullable(String)) │              │                    │         │                  │                │
└─────────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Let's see the content of the `system.schema_inference_cache` table:

```sql
SELECT *
FROM system.schema_inference_cache
FORMAT Vertical
```
```response
Row 1:
──────
storage:                File
source:                 /home/droscigno/user_files/data.jsonl
format:                 JSONEachRow
additional_format_info: schema_inference_hints=, max_rows_to_read_for_schema_inference=25000, schema_inference_make_columns_nullable=true, try_infer_integers=false, try_infer_dates=true, try_infer_datetimes=true, try_infer_numbers_from_strings=true, read_bools_as_numbers=true, try_infer_objects=false
registration_time:      2022-12-29 17:49:52
schema:                 id Nullable(Float64), age Nullable(Float64), name Nullable(String), hobbies Array(Nullable(String))
```


**See also**
- [Automatic schema inference from input data](/interfaces/schema-inference.md)
