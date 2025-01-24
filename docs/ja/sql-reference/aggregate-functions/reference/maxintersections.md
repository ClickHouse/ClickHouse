---
slug: /ja/sql-reference/aggregate-functions/reference/maxintersections
sidebar_position: 163
title: maxIntersections
---

# maxIntersections

グループ内のインターバルがどれだけ多く交差するか（すべてのインターバルが少なくとも1回交差する場合）の最大数を計算する集約関数です。

構文は以下の通りです:

```sql
maxIntersections(start_column, end_column)
```

**引数**

- `start_column` – 各インターバルの開始を表す数値カラム。この`start_column`が`NULL`または0の場合、そのインターバルはスキップされます。

- `end_column` - 各インターバルの終了を表す数値カラム。この`end_column`が`NULL`または0の場合、そのインターバルはスキップされます。

**戻り値**

交差しているインターバルの最大数を返します。

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

それぞれのインターバルは以下のようになります:

```response
1 - 3
1 - - - - 6
  2 - - 5
    3 - - - 7
```

これらのインターバルのうち3つが共通の値を持っています（値は`4`ですが、その共通の値自体は重要ではありません。交差の回数を測定しています）。インターバル`(1,3)`と`(3,7)`は終点を共有していますが、`maxIntersections`関数では交差しているとは見なされません。

```sql
SELECT maxIntersections(start, end) FROM my_events;
```

結果:
```response
3
```

最大インターバルが複数回出現する場合は、[`maxIntersectionsPosition`関数](./maxintersectionsposition.md)を使用してそれらの出現回数と場所を特定できます。
