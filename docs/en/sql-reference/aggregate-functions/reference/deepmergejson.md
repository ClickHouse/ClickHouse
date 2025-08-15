---
description: 'Merges JSON objects from multiple rows into a single JSON object.'
sidebar_position: 217
slug: /sql-reference/aggregate-functions/reference/deepmergejson
title: 'deepMergeJSON'
---

# deepMergeJSON

Merges JSON objects from multiple rows into a single JSON object. When the same path appears in multiple rows, the value from the last row (based on processing order) is kept. The function preserves the nested structure of JSON objects.

**Syntax**

```sql
deepMergeJSON(json)
```

**Arguments**

- `json` — [JSON](../../data-types/newjson.md) column containing objects to merge.

**Returned Value**

- A merged JSON object containing all unique paths from the input rows [JSON](../../data-types/newjson.md).

**Example**

Query:

```sql
DROP TABLE IF EXISTS test_json;
CREATE TABLE test_json(json JSON) ENGINE = Memory;
INSERT INTO test_json VALUES 
    ('{"a": 1, "b": {"c": 2}}'),
    ('{"a": 3, "b": {"d": 4}}'),
    ('{"b": {"c": 5}, "e": 6}');

SELECT deepMergeJSON(json) FROM test_json;
```

Result:

```reference
┌─deepMergeJSON(json)───────────────┐
│ {"a":3,"b":{"c":5,"d":4},"e":6}  │
└───────────────────────────────────┘
```

In this example:
- Path `a` appears in rows 1 and 2, so the value from row 2 (`3`) is kept
- Path `b.c` appears in rows 1 and 3, so the value from row 3 (`5`) is kept
- Path `b.d` only appears in row 2, so its value (`4`) is preserved
- Path `e` only appears in row 3, so its value (`6`) is preserved

**Use Cases**

This function is particularly useful for:

1. **Merging partial updates**: When you have JSON documents representing partial updates to an object
2. **Configuration aggregation**: Combining configuration settings from multiple sources
3. **User preference merging**: Aggregating user preferences where later values override earlier ones

**Example with Partial Updates**

```sql
DROP TABLE IF EXISTS user_updates;
CREATE TABLE user_updates(user_id UInt32, update JSON) ENGINE = Memory;

INSERT INTO user_updates VALUES 
    (1, '{"name": "Alice", "email": "alice@example.com"}'),
    (1, '{"email": "newalice@example.com", "phone": "+1234567890"}'),
    (1, '{"preferences": {"theme": "dark", "language": "en"}}');

SELECT user_id, deepMergeJSON(update) AS merged_profile 
FROM user_updates 
GROUP BY user_id;
```

Result:

```reference
┌─user_id─┬─merged_profile──────────────────────────────────────────────────────────────────────────┐
│       1 │ {"name":"Alice","email":"newalice@example.com","phone":"+1234567890",                  │
│         │  "preferences":{"theme":"dark","language":"en"}}                                       │
└─────────┴─────────────────────────────────────────────────────────────────────────────────────────┘
```

**Complex Nested Structures**

The function correctly handles deeply nested structures:

```sql
DROP TABLE IF EXISTS nested_json;
CREATE TABLE nested_json(data JSON) ENGINE = Memory;

INSERT INTO nested_json VALUES 
    ('{"level1": {"level2": {"level3": {"a": 1}}}}'),
    ('{"level1": {"level2": {"level3": {"b": 2}}, "other": 3}}'),
    ('{"level1": {"level2": {"level3": {"a": 4}}}}');

SELECT deepMergeJSON(data) FROM nested_json;
```

Result:

```reference
┌─deepMergeJSON(data)───────────────────────────────────────┐
│ {"level1":{"level2":{"level3":{"a":4,"b":2},"other":3}}} │
└───────────────────────────────────────────────────────────┘
```

**Deleting Paths with `$unset`**

The function supports explicit deletion of paths using the special `$unset` marker. This is useful when you need to remove a field from the merged result:

1. **Using path suffix**: Append `.$unset` to any path with value `true` to delete that path:

```sql
DROP TABLE IF EXISTS json_with_unset;
CREATE TABLE json_with_unset(data JSON) ENGINE = Memory;

INSERT INTO json_with_unset VALUES 
    ('{"user": {"name": "Alice", "email": "alice@example.com", "phone": "123"}}'),
    ('{"user": {"email.$unset": true}}'),  -- This deletes user.email
    ('{"user": {"city": "NYC"}}');

SELECT deepMergeJSON(data) FROM json_with_unset;
```

Result:

```reference
┌─deepMergeJSON(data)────────────────────────────────┐
│ {"user":{"name":"Alice","phone":"123","city":"NYC"}} │
└─────────────────────────────────────────────────────┘
```

2. **Using object marker**: Set a path's value to `{"$unset": true}` to delete it:

```sql
DROP TABLE IF EXISTS json_unset_object;
CREATE TABLE json_unset_object(data JSON) ENGINE = Memory;

INSERT INTO json_unset_object VALUES 
    ('{"config": {"debug": true, "timeout": 30, "retries": 3}}'),
    ('{"config": {"timeout": {"$unset": true}}}'),  -- Delete config.timeout
    ('{"config": {"debug": false}}');

SELECT deepMergeJSON(data) FROM json_unset_object;
```

Result:

```reference
┌─deepMergeJSON(data)─────────────────────┐
│ {"config":{"debug":false,"retries":3}} │
└─────────────────────────────────────────┘
```

**Important aspects of `$unset`:**
- Deleting a path also removes all its child paths
- Deletion operations respect row order - only newer deletions affect the result
- A path can be re-added after deletion if a newer row contains that path

**Notes**

- The function preserves the data types of values in the JSON
- When merging arrays, the entire array from the last occurrence is kept (arrays are not merged element-wise)
- Empty objects `{}` are treated as valid values and will override previous values at the same path
- The order of rows matters - later rows override values from earlier rows for the same paths
- The function has built-in limits to prevent memory abuse:
  - Maximum 10,000 distinct paths
  - Maximum path length of 1,000 characters
  - Maximum total state size of 100 MiB
