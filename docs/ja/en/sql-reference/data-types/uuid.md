---
description: 'ClickHouse の UUID データ型 に関するドキュメント'
sidebar_label: 'UUID'
sidebar_position: 24
slug: /sql-reference/data-types/uuid
title: 'UUID'
doc_type: 'reference'
---

Universally Unique Identifier (UUID) は、レコードを識別するために使用される 16 バイトの値です。UUID の詳細については、[Wikipedia](https://en.wikipedia.org/wiki/Universally_unique_identifier) を参照してください。

UUIDv4 や UUIDv7 など、複数の UUID variant が存在しますが ([こちら](https://datatracker.ietf.org/doc/html/draft-ietf-uuidrev-rfc4122bis)を参照) 、ClickHouse は、挿入された UUID が特定の variant に準拠しているかどうかを検証しません。
UUID は内部的には 16 バイトのランダムな数列として扱われ、SQL レベルでは [8-4-4-4-12 representation](https://en.wikipedia.org/wiki/Universally_unique_identifier#Textual_representation) で表現されます。

UUID 値の例:

```text
61f0c404-5cb3-11e7-907b-a6006ad3dba0
```

デフォルトの UUID はすべて 0 です。これは、たとえば新しいレコードが挿入される際に UUID カラムの値が指定されていない場合に使用されます。

```text
00000000-0000-0000-0000-000000000000
```

:::warning
歴史的な理由により、UUID は後半部分でソートされます。

これは UUIDv4 の値では問題ありませんが、プライマリインデックスの定義に UUIDv7 カラムを使用すると、パフォーマンスが低下する可能性があります (ordering key や partition key での使用は問題ありません) 。
より具体的には、UUIDv7 の値は前半に timestamp、後半にカウンターを持ちます。
そのため、スパースなプライマリキー索引における UUIDv7 のソート (つまり各インデックスグラニュールの先頭の値) は、カウンターフィールドに基づいて行われます。
UUID が前半部分 (timestamp) でソートされると仮定すると、クエリの開始時に行われるプライマリキー索引解析の段階で、1 つを除くすべてのパーツ内のすべてのマークを刈り込めると期待されます。
しかし、後半部分 (カウンター) でソートされる場合は、すべてのパーツについて少なくとも 1 つのマークが返されることになり、不要なディスクアクセスが発生します。
:::

例:

```sql title="Query"
CREATE TABLE tab (uuid UUID) ENGINE = MergeTree PRIMARY KEY (uuid);

INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
SELECT * FROM tab;
```

```text title="Response"
┌─uuid─────────────────────────────────┐
│ 019d2555-7874-7e9d-a284-9b45a0b2f165 │
│ 019d2555-7874-7e9d-a284-9b46c3353be7 │
│ 019d2555-7878-77fc-a36f-4081aa58ec2b │
│ 019d2555-7878-77fc-a36f-40826555fb9b │
│ 019d2555-7870-7432-ba62-5250ac595328 │
│ 019d2555-7870-7432-ba62-5251da22bd19 │
│ 019d2555-786c-73e9-a031-4a7936df7d56 │
│ 019d2555-786c-73e9-a031-4a7a35a9544f │
│ 019d2555-7868-7333-89d1-2bd1639899c3 │
│ 019d2555-7868-7333-89d1-2bd297eb7d42 │
└──────────────────────────────────────┘

```

回避策として、UUID は後半部分から抽出したタイムスタンプに変換できます。

```sql title="Query"
CREATE TABLE tab (uuid UUID) ENGINE = MergeTree PRIMARY KEY (UUIDv7ToDateTime(uuid));
-- Or alternatively:                      [...] PRIMARY KEY (toStartOfHour(UUIDv7ToDateTime(uuid)));

INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
SELECT * FROM tab;
```

結果 (同じデータが挿入されていると仮定すると) :

```text title="Response"
┌─uuid─────────────────────────────────┐
│ 019d2555-7868-7333-89d1-2bd1639899c3 │
│ 019d2555-7868-7333-89d1-2bd297eb7d42 │
│ 019d2555-786c-73e9-a031-4a7936df7d56 │
│ 019d2555-786c-73e9-a031-4a7a35a9544f │
│ 019d2555-7870-7432-ba62-5250ac595328 │
│ 019d2555-7870-7432-ba62-5251da22bd19 │
│ 019d2555-7874-7e9d-a284-9b45a0b2f165 │
│ 019d2555-7874-7e9d-a284-9b46c3353be7 │
│ 019d2555-7878-77fc-a36f-4081aa58ec2b │
│ 019d2555-7878-77fc-a36f-40826555fb9b │
└──────────────────────────────────────┘

```

ORDER BY (UUIDv7ToDateTime(uuid), uuid)

<div id="generating-uuids">
  ## UUID の生成
</div>

ClickHouse には、ランダムなバージョン 4 の UUID 値を生成する [generateUUIDv4](../../sql-reference/functions/uuid-functions.md) 関数があります。

<div id="usage-example">
  ## 使用例
</div>

**例 1**

この例では、UUID カラムを持つテーブルを作成し、そのテーブルに値を挿入する方法を示します。

```sql title="Query"
CREATE TABLE t_uuid (x UUID, y String) ENGINE=TinyLog

INSERT INTO t_uuid SELECT generateUUIDv4(), 'Example 1'

SELECT * FROM t_uuid
```

```text title="Response"
┌────────────────────────────────────x─┬─y─────────┐
│ 417ddc5d-e556-4d27-95dd-a34d84e46a50 │ Example 1 │
└──────────────────────────────────────┴───────────┘
```

**例 2**

この例では、レコードの挿入時に UUID カラムの値を指定していないため、デフォルトの UUID 値が挿入されます。

```sql
INSERT INTO t_uuid (y) VALUES ('Example 2')

SELECT * FROM t_uuid
```

```text
┌────────────────────────────────────x─┬─y─────────┐
│ 417ddc5d-e556-4d27-95dd-a34d84e46a50 │ Example 1 │
│ 00000000-0000-0000-0000-000000000000 │ Example 2 │
└──────────────────────────────────────┴───────────┘
```

<div id="restrictions">
  ## 制約
</div>

UUIDデータ型でサポートされるのは、[String](../../sql-reference/data-types/string.md) データ型でもサポートされる関数のみです (たとえば、[min](/ja/sql-reference/aggregate-functions/reference/min)、[max](/ja/sql-reference/aggregate-functions/reference/max)、[count](/ja/sql-reference/aggregate-functions/reference/count) など) 。

UUIDデータ型は、算術演算 (たとえば [abs](/ja/sql-reference/functions/arithmetic-functions#abs)) や、[sum](/ja/sql-reference/aggregate-functions/reference/sum) や [avg](/ja/sql-reference/aggregate-functions/reference/avg) などの集約関数には対応していません。