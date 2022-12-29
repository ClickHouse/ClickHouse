---
slug: /en/operations/system-tables/schema_inference_cache
---
# Schema inference cache

Contains information about all cached file schemas.

Columns:
- `storage` ([String](../../sql-reference/data-types/string.md)) — Storage name: File, URL, S3 or URL.
- `source` ([String](../../sql-reference/data-types/string.md)) — File source.
- `format` ([String](../../sql-reference/data-types/string.md)) — Format name.
- `additional_format_info` (([String](../../sql-reference/data-types/string.md))) - Additional information required to identify the schema. For example, format specific settings.
- `registration_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Timestamp when schema was added in cache.
- `schema` ([String](../../sql-reference/data-types/string.md)) - Cached schema.

**Example**

Let's say we have a file `data.jsonl` with the next content:
```json
{"id" :  1, "age" :  25, "name" :  "Josh", "hobbies" :  ["football", "cooking", "music"]}
{"id" :  2, "age" :  19, "name" :  "Alan", "hobbies" :  ["tennis", "art"]}
{"id" :  3, "age" :  32, "name" :  "Lana", "hobbies" :  ["fintess", "reading", "shopping"]}
{"id" :  4, "age" :  47, "name" :  "Brayan", "hobbies" :  ["movies", "skydiving"]}
```

Let's run `DESCRIBE` query:

```sql
DESCRIBE file('data.jsonl') SETTINGS input_format_try_infer_integers=0;
```

```text
┌─name────┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ id      │ Nullable(Float64)       │              │                    │         │                  │                │
│ age     │ Nullable(Float64)       │              │                    │         │                  │                │
│ name    │ Nullable(String)        │              │                    │         │                  │                │
│ hobbies │ Array(Nullable(String)) │              │                    │         │                  │                │
└─────────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Let's see the content of `system.schema_inference_cache` table:

```sql
SELECT * FROM system.schema_inference_cache;
```

```text
┌─storage─┬─source───────────────────────────────────────────────────────────┬─format──────┬─additional_format_info────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬───registration_time─┬─schema──────────────────────────────────────────────────────────────────────────────────────────────┐
│ File    │ /etc/clickhouse-server/user_files/hobbies.jsonl                  │ JSONEachRow │ schema_inference_hints=, max_rows_to_read_for_schema_inference=25000, schema_inference_make_columns_nullable=true, try_infer_integers=false, try_infer_dates=true, try_infer_datetimes=true, try_infer_numbers_from_strings=true, read_bools_as_numbers=true, try_infer_objects=false │ 2022-12-23 16:17:24 │ id Nullable(Float64), age Nullable(Float64), name Nullable(String), hobbies Array(Nullable(String)) │
└─────────┴──────────────────────────────────────────────────────────────────┴─────────────┴───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴─────────────────────┴─────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

**See also**
-   [Automatic schema inference from input data](../../interfaces/schema-inference.md)

[Original article](https://clickhouse.com/docs/en/operations/system-tables/schema_inference_cache) <!--hide-->
