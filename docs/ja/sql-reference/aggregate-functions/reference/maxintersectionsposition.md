---
slug: /ja/sql-reference/aggregate-functions/reference/maxintersectionsposition
sidebar_position: 164
title: maxIntersectionsPosition
---

# maxIntersectionsPosition

[`maxIntersections` 関数](./maxintersections.md) の出現位置を計算する集約関数です。

この構文は以下の通りです:

```sql
maxIntersectionsPosition(start_column, end_column)
```

**引数**

- `start_column` – 各区間の開始を表す数値のカラム。`start_column` が `NULL` または 0 の場合、その区間はスキップされます。

- `end_column` - 各区間の終了を表す数値のカラム。`end_column` が `NULL` または 0 の場合、その区間はスキップされます。

**返される値**

最大数の区間が重なる開始位置を返します。

**例**

```sql
CREATE TABLE my_events (
    start UInt32,
    end UInt32
)
Engine = MergeTree
ORDER BY tuple();

INSERT INTO my_events VALUES
   (1, 3),
   (1, 6),
   (2, 5),
   (3, 7);
```

区間は次のようになります:

```response
1 - 3
1 - - - - 6
  2 - - 5
    3 - - - 7
```

これらの区間のうち3つが値4を共有しており、それは2番目の区間から始まっています:

```sql
SELECT maxIntersectionsPosition(start, end) FROM my_events;
```

出力:
```response
2
```

言い換えれば、`(1,6)` の行が3つの交差する区間の開始であり、3が交差する最大数の区間です。
