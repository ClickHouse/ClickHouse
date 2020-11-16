---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 46
toc_title: UUID
---

# UUID {#uuid-data-type}

普遍一意識別子(UUID)は、レコードを識別するために使用される16バイトの番号です。 UUIDの詳細については、以下を参照してください [Wikipedia](https://en.wikipedia.org/wiki/Universally_unique_identifier).

UUID型の値の例を以下に示します:

``` text
61f0c404-5cb3-11e7-907b-a6006ad3dba0
```

新しいレコードを挿入するときにUUID列の値を指定しない場合、UUID値はゼロで埋められます:

``` text
00000000-0000-0000-0000-000000000000
```

## 生成する方法 {#how-to-generate}

Uuid値を生成するために、ClickHouseは [generateuidv4](../../sql-reference/functions/uuid-functions.md) 機能。

## 使用例 {#usage-example}

**例1**

この例では、UUID型の列を持つテーブルを作成し、テーブルに値を挿入する方法を示します。

``` sql
CREATE TABLE t_uuid (x UUID, y String) ENGINE=TinyLog
```

``` sql
INSERT INTO t_uuid SELECT generateUUIDv4(), 'Example 1'
```

``` sql
SELECT * FROM t_uuid
```

``` text
┌────────────────────────────────────x─┬─y─────────┐
│ 417ddc5d-e556-4d27-95dd-a34d84e46a50 │ Example 1 │
└──────────────────────────────────────┴───────────┘
```

**例2**

この例では、新しいレコードを挿入するときにUUID列の値は指定されません。

``` sql
INSERT INTO t_uuid (y) VALUES ('Example 2')
```

``` sql
SELECT * FROM t_uuid
```

``` text
┌────────────────────────────────────x─┬─y─────────┐
│ 417ddc5d-e556-4d27-95dd-a34d84e46a50 │ Example 1 │
│ 00000000-0000-0000-0000-000000000000 │ Example 2 │
└──────────────────────────────────────┴───────────┘
```

## 制限 {#restrictions}

UUIDデータ型は、以下の関数のみをサポートします [文字列](string.md) データ型もサポートしています(例, [分](../../sql-reference/aggregate-functions/reference.md#agg_function-min), [最大](../../sql-reference/aggregate-functions/reference.md#agg_function-max),and [カウント](../../sql-reference/aggregate-functions/reference.md#agg_function-count)).

Uuidデータ型は、算術演算ではサポートされません(たとえば, [abs](../../sql-reference/functions/arithmetic-functions.md#arithm_func-abs)）または、以下のような集計関数 [和](../../sql-reference/aggregate-functions/reference.md#agg_function-sum) と [avg](../../sql-reference/aggregate-functions/reference.md#agg_function-avg).

[元の記事](https://clickhouse.tech/docs/en/data_types/uuid/) <!--hide-->
