---
slug: /ja/sql-reference/statements/select/union
sidebar_label: UNION
---

# UNION句

`UNION`は`UNION ALL`または`UNION DISTINCT`を明示的に指定して使用できます。

`ALL`または`DISTINCT`を指定しない場合、`union_default_mode`設定に依存します。`UNION ALL`と`UNION DISTINCT`の違いは、`UNION DISTINCT`ではUNIONの結果に対して重複排除変換が行われることです。これは`UNION ALL`を含むサブクエリから`SELECT DISTINCT`を行うことと同等です。

任意の数の`SELECT`クエリの結果を拡張して組み合わせるために`UNION`を使用できます。例:

``` sql
SELECT CounterID, 1 AS table, toInt64(count()) AS c
    FROM test.hits
    GROUP BY CounterID

UNION ALL

SELECT CounterID, 2 AS table, sum(Sign) AS c
    FROM test.visits
    GROUP BY CounterID
    HAVING c > 0
```

結果のカラムはそのインデックス（`SELECT`内の順序）に基づいて一致します。カラム名が一致しない場合、最終結果のカラム名は最初のクエリから取得されます。

Unionのために型キャストが行われます。たとえば、同じフィールドを持つ二つのクエリを結合する際に、そのフィールドが非`Nullable`型と互換性のある`Nullable`型の場合、結果の`UNION`フィールドは`Nullable`型になります。

`UNION`の一部であるクエリを丸括弧で囲むことができます。[ORDER BY](../../../sql-reference/statements/select/order-by.md)や[LIMIT](../../../sql-reference/statements/select/limit.md)は、個別のクエリに適用され、最終結果には適用されません。最終結果に変換を適用する必要がある場合、すべてのクエリを`UNION`で`FROM`句内のサブクエリに配置できます。

`UNION`を`UNION ALL`または`UNION DISTINCT`を明示的に指定せずに使用する場合は、[union_default_mode](../../../operations/settings/settings.md#union-default-mode)設定を使用してUnionモードを指定できます。設定値は`ALL`、`DISTINCT`、または空文字列のいずれかです。しかし、`UNION`を`union_default_mode`設定で空文字列にすると、例外がスローされます。以下の例は、異なる設定値を持つクエリの結果を示しています。

クエリ:

```sql
SET union_default_mode = 'DISTINCT';
SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 2;
```

結果:

```text
┌─1─┐
│ 1 │
└───┘
┌─1─┐
│ 2 │
└───┘
┌─1─┐
│ 3 │
└───┘
```

クエリ:

```sql
SET union_default_mode = 'ALL';
SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 2;
```

結果:

```text
┌─1─┐
│ 1 │
└───┘
┌─1─┐
│ 2 │
└───┘
┌─1─┐
│ 2 │
└───┘
┌─1─┐
│ 3 │
└───┘
```

`UNION/UNION ALL/UNION DISTINCT`の一部であるクエリは同時に実行され、その結果を組み合わせることができます。

**参照**

- [insert_null_as_default](../../../operations/settings/settings.md#insert_null_as_default) 設定。
- [union_default_mode](../../../operations/settings/settings.md#union-default-mode) 設定。
