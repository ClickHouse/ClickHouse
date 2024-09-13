---
slug: /en/sql-reference/data-types/json
sidebar_position: 26
sidebar_label: JSON
---

# JSON

:::note
This feature is experimental and is not production-ready. If you need to work with JSON documents, consider using [this guide](/docs/en/integrations/data-ingestion/data-formats/json.md) instead.
:::

Stores JavaScript Object Notation (JSON) documents in a single column.

`JSON` is an alias for `Object('json')`.

:::note
The JSON data type is an obsolete feature. Do not use it.
If you want to use it, set `allow_experimental_object_type = 1`.
:::

## Example

**Example 1**

Creating a table with a `JSON` column and inserting data into it:

```sql
CREATE TABLE json
(
    o JSON
)
ENGINE = Memory
```

```sql
INSERT INTO json VALUES ('{"a": 1, "b": { "c": 2, "d": [1, 2, 3] }}')
```

```sql
SELECT o.a, o.b.c, o.b.d[3] FROM json
```

```text
┌─o.a─┬─o.b.c─┬─arrayElement(o.b.d, 3)─┐
│   1 │     2 │                      3 │
└─────┴───────┴────────────────────────┘
```

**Example 2**

To be able to create an ordered `MergeTree` family table the sorting key has to be extracted into its column. For example, to insert a file of compressed HTTP access logs in JSON format:

```sql
CREATE TABLE logs
(
	timestamp DateTime,
	message JSON
)
ENGINE = MergeTree
ORDER BY timestamp
```

```sql
INSERT INTO logs
SELECT parseDateTimeBestEffort(JSONExtractString(json, 'timestamp')), json
FROM file('access.json.gz', JSONAsString)
```

## Displaying JSON columns

When displaying a `JSON` column ClickHouse only shows the field values by default (because internally, it is represented as a tuple). You can display the field names as well by setting `output_format_json_named_tuples_as_objects = 1`:

```sql
SET output_format_json_named_tuples_as_objects = 1

SELECT * FROM json FORMAT JSONEachRow
```

```text
{"o":{"a":1,"b":{"c":2,"d":[1,2,3]}}}
```

## Related Content

- [Getting Data Into ClickHouse - Part 2 - A JSON detour](https://clickhouse.com/blog/getting-data-into-clickhouse-part-2-json)
