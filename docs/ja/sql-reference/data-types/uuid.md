---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 46
toc_title: UUID
---

# UUID {#uuid-data-type}

Universally unique identifier(UUID)は、レコードを識別するために使用される16バイトの数値です。 UUIDの詳細については、以下を参照してください [Wikipedia](https://en.wikipedia.org/wiki/Universally_unique_identifier).

UUID型の値の例を以下に示します:

``` text
61f0c404-5cb3-11e7-907b-a6006ad3dba0
```

新しいレコードを挿入するときにuuid列の値を指定しない場合、uuidの値はゼロで埋められます:

``` text
00000000-0000-0000-0000-000000000000
```

## 生成する方法 {#how-to-generate}

UUID値を生成するには、ClickHouseに次の値を指定します [generateUUIDv4](../../sql-reference/functions/uuid-functions.md) 機能。

## 使用例 {#usage-example}

**例1**

この例では、uuid型の列を使用してテーブルを作成し、テーブルに値を挿入する方法を示します。

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

この例では、新しいレコードを挿入するときにuuid列の値が指定されていません。

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

UUIDデータ型は、以下の関数のみをサポートします [文字列](string.md) データ型もサポートします(たとえば, [分](../../sql-reference/aggregate-functions/reference.md#agg_function-min), [最大](../../sql-reference/aggregate-functions/reference.md#agg_function-max)、と [カウント](../../sql-reference/aggregate-functions/reference.md#agg_function-count)).

UUIDデータ型は、算術演算ではサポートされません(たとえば, [abs](../../sql-reference/functions/arithmetic-functions.md#arithm_func-abs) 以下のような)または集約関数 [合計](../../sql-reference/aggregate-functions/reference.md#agg_function-sum) と [平均](../../sql-reference/aggregate-functions/reference.md#agg_function-avg).

[元の記事](https://clickhouse.tech/docs/en/data_types/uuid/) <!--hide-->
