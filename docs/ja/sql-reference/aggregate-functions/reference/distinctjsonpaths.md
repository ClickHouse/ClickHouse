---
slug: /ja/sql-reference/aggregate-functions/reference/distinctjsonpaths
sidebar_position: 216
---

# distinctJSONPaths

[JSON](../../data-types/newjson.md) カラムに格納されている異なるパスのリストを計算します。

**構文**

```sql
distinctJSONPaths(json)
```

**引数**

- `json` — [JSON](../../data-types/newjson.md) カラム。

**返される値**

- ソートされたパスのリスト [Array(String)](../../data-types/array.md)。

**例**

クエリ:

```sql
DROP TABLE IF EXISTS test_json;
CREATE TABLE test_json(json JSON) ENGINE = Memory;
INSERT INTO test_json VALUES ('{"a" : 42, "b" : "Hello"}'), ('{"b" : [1, 2, 3], "c" : {"d" : {"e" : "2020-01-01"}}}'), ('{"a" : 43, "c" : {"d" : {"f" : [{"g" : 42}]}}}')
```

```sql
SELECT distinctJSONPaths(json) FROM test_json;
```

結果:

```reference
┌─distinctJSONPaths(json)───┐
│ ['a','b','c.d.e','c.d.f'] │
└───────────────────────────┘
```

# distinctJSONPathsAndTypes

[JSON](../../data-types/newjson.md) カラムに格納されている異なるパスとその型のリストを計算します。

**構文**

```sql
distinctJSONPathsAndTypes(json)
```

**引数**

- `json` — [JSON](../../data-types/newjson.md) カラム。

**返される値**

- ソートされたパスと型のマップ [Map(String, Array(String))](../../data-types/map.md)。

**例**

クエリ:

```sql
DROP TABLE IF EXISTS test_json;
CREATE TABLE test_json(json JSON) ENGINE = Memory;
INSERT INTO test_json VALUES ('{"a" : 42, "b" : "Hello"}'), ('{"b" : [1, 2, 3], "c" : {"d" : {"e" : "2020-01-01"}}}'), ('{"a" : 43, "c" : {"d" : {"f" : [{"g" : 42}]}}}')
```

```sql
SELECT distinctJSONPathsAndTypes(json) FROM test_json;
```

結果:

```reference
┌─distinctJSONPathsAndTypes(json)───────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ {'a':['Int64'],'b':['Array(Nullable(Int64))','String'],'c.d.e':['Date'],'c.d.f':['Array(JSON(max_dynamic_types=16, max_dynamic_paths=256))']} │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

**注意**

JSON 宣言に指定された型のパスが含まれている場合、入力データにこれらのパスの値が含まれていなくても、`distinctJSONPaths/distinctJSONPathsAndTypes` 関数の結果に常に含まれます。

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
