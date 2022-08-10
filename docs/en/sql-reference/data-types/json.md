---
sidebar_position: 54
sidebar_label: JSON
---

# JSON {#json-data-type}

Stores JavaScript Object Notation (JSON) documents in a single column.

`JSON` is an alias for `Object('json')`.

:::warning
The JSON data type is an experimental feature. To use it, set `allow_experimental_object_type = 1`.
:::

## Example {#usage-example}

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
