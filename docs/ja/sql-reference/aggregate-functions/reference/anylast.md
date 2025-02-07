---
slug: /ja/sql-reference/aggregate-functions/reference/anylast
sidebar_position: 105
---

# anyLast

最後に遭遇した値を選択し、デフォルトではいかなる `NULL` 値も無視します。結果は [any](../../../sql-reference/aggregate-functions/reference/any.md) 関数の場合と同様に不確定です。

**構文**

```sql
anyLast(column) [RESPECT NULLS]
```

**パラメータ**
- `column`: カラム名。

:::note
関数名の後に `RESPECT NULLS` 修飾子をサポートしています。この修飾子を使用すると、`NULL` であるかどうかに関わらず、最後に渡された値を確実に選択します。
:::

**返される値**

- 最後に遭遇した値。

**例**

クエリ:

```sql
CREATE TABLE any_last_nulls (city Nullable(String)) ENGINE=Log;

INSERT INTO any_last_nulls (city) VALUES ('Amsterdam'),(NULL),('New York'),('Tokyo'),('Valencia'),(NULL);

SELECT anyLast(city) FROM any_last_nulls;
```

```response
┌─anyLast(city)─┐
│ Valencia      │
└───────────────┘
```
