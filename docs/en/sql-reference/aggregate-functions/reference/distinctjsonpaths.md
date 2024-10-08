---
slug: /en/sql-reference/aggregate-functions/reference/distinctjsonpaths
sidebar_position: 216
---

# distinctJSONPaths

Calculates the list of distinct paths stored in [JSON](../../data-types/newjson.md) column.

**Syntax**

```sql
distinctJSONPaths(json)
```

**Arguments**

- `json` — [JSON](../../data-types/newjson.md) column.

**Returned Value**

- The sorted list of paths [Array(String)](../../data-types/array.md).

**Example**

Query:

```sql
DROP TABLE IF EXISTS test_json;
CREATE TABLE test_json(json JSON) ENGINE = Memory;
INSERT INTO test_json VALUES ('{"a" : 42, "b" : "Hello"}'), ('{"b" : [1, 2, 3], "c" : {"d" : {"e" : "2020-01-01"}}}'), ('{"a" : 43, "c" : {"d" : {"f" : [{"g" : 42}]}}}')
```

```sql
SELECT distinctJSONPaths(json) FROM test_json;
```

Result:

```reference
┌─distinctJSONPaths(json)───┐
│ ['a','b','c.d.e','c.d.f'] │
└───────────────────────────┘
```

# distinctJSONPathsAndTypes

Calculates the list of distinct paths and their types stored in [JSON](../../data-types/newjson.md) column.

**Syntax**

```sql
distinctJSONPathsAndTypes(json)
```

**Arguments**

- `json` — [JSON](../../data-types/newjson.md) column.

**Returned Value**

- The sorted map of paths and types [Map(String, Array(String))](../../data-types/map.md).

**Example**

Query:

```sql
DROP TABLE IF EXISTS test_json;
CREATE TABLE test_json(json JSON) ENGINE = Memory;
INSERT INTO test_json VALUES ('{"a" : 42, "b" : "Hello"}'), ('{"b" : [1, 2, 3], "c" : {"d" : {"e" : "2020-01-01"}}}'), ('{"a" : 43, "c" : {"d" : {"f" : [{"g" : 42}]}}}')
```

```sql
SELECT distinctJSONPathsAndTypes(json) FROM test_json;
```

Result:

```reference
┌─distinctJSONPathsAndTypes(json)───────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ {'a':['Int64'],'b':['Array(Nullable(Int64))','String'],'c.d.e':['Date'],'c.d.f':['Array(JSON(max_dynamic_types=16, max_dynamic_paths=256))']} │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

**Note**

If JSON declaration contains paths with specified types, these paths will be always included in the result of `distinctJSONPaths/distinctJSONPathsAndTypes` functions even if input data didn't have values for these paths.

```sql
DROP TABLE IF EXISTS test_json;
CREATE TABLE test_json(json JSON(a UInt32)) ENGINE = Memory;
INSERT INTO test_json VALUES ('{"b" : "Hello"}'), ('{"b" : "World", "c" : [1, 2, 3]}');
```

```sql
SELECT json FROM test_json;
```

```text
┌─json──────────────────────────────────┐
│ {"a":0,"b":"Hello"}                   │
│ {"a":0,"b":"World","c":["1","2","3"]} │
└───────────────────────────────────────┘
```

```sql
SELECT distinctJSONPaths(json) FROM test_json;
```

```text
┌─distinctJSONPaths(json)─┐
│ ['a','b','c']           │
└─────────────────────────┘
```

```sql
SELECT distinctJSONPathsAndTypes(json) FROM test_json;
```

```text
┌─distinctJSONPathsAndTypes(json)────────────────────────────────┐
│ {'a':['UInt32'],'b':['String'],'c':['Array(Nullable(Int64))']} │
└────────────────────────────────────────────────────────────────┘
```