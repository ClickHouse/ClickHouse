---
description: 'UNION 句のドキュメント'
sidebar_label: 'UNION'
slug: /sql-reference/statements/select/union
title: 'UNION 句'
doc_type: 'reference'
---

`UNION` は、`UNION ALL` または `UNION DISTINCT` を明示的に指定して使用できます。

`ALL` または `DISTINCT` を指定しない場合は、`union_default_mode` 設定によって決まります。`UNION ALL` と `UNION DISTINCT` の違いは、`UNION DISTINCT` ではユニオン結果に対して重複排除が行われることです。これは、`UNION ALL` を含むサブクエリに対して `SELECT DISTINCT` を実行するのと同等です。

`UNION` を使用すると、任意の数の `SELECT` クエリの結果を連結して結合できます。例:

```sql title="Query"
SELECT CounterID, 1 AS table, toInt64(count()) AS c
    FROM test.hits
    GROUP BY CounterID

UNION ALL

SELECT CounterID, 2 AS table, sum(Sign) AS c
    FROM test.visits
    GROUP BY CounterID
    HAVING c > 0
```

結果カラムは、そのインデックス (`SELECT` 内での順序) によって対応付けられます。カラム名が一致しない場合、最終結果のカラム名は最初のクエリから取得されます。

`UNION` では型変換が行われます。たとえば、結合する 2 つのクエリに同じフィールドがあり、その型が互換性のある型で一方は non-`Nullable`、もう一方は `Nullable` である場合、結果の `UNION` ではそのフィールドは `Nullable` 型になります。

`UNION` の一部であるクエリは `()` で囲むことができます。[ORDER BY](../../../sql-reference/statements/select/order-by.md) と [LIMIT](../../../sql-reference/statements/select/limit.md) は最終結果ではなく、個々のクエリに適用されます。最終結果に変換を適用する必要がある場合は、`UNION` を含むすべてのクエリを [FROM](../../../sql-reference/statements/select/from.md) 句内のサブクエリに入れることができます。

`UNION ALL` または `UNION DISTINCT` を明示的に指定せずに `UNION` を使用する場合は、[union&#95;default&#95;mode](/ja/operations/settings/settings#union_default_mode) 設定を使って UNION モードを指定できます。設定値には `ALL`、`DISTINCT`、または空文字列を指定できます。ただし、`union_default_mode` 設定を空文字列にした状態で `UNION` を使用すると、例外が発生します。以下の例は、設定値が異なる場合のクエリ結果を示しています。

```sql title="Query"
SET union_default_mode = 'DISTINCT';
SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 2;
```

```text title="Response"
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

```sql title="Query"
SET union_default_mode = 'ALL';
SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 2;
```

```text title="Response"
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

`UNION/UNION ALL/UNION DISTINCT` を構成するクエリは同時に実行されることがあり、その結果が混在する場合があります。

**関連項目**

* [insert&#95;null&#95;as&#95;default](../../../operations/settings/settings.md#insert_null_as_default) 設定。
* [union&#95;default&#95;mode](/ja/operations/settings/settings#union_default_mode) 設定。