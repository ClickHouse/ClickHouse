---
description: 'OFFSET のドキュメント'
sidebar_label: 'OFFSET'
slug: /sql-reference/statements/select/offset
title: 'OFFSET FETCH 句'
doc_type: 'reference'
---

`OFFSET` と `FETCH` を使うと、データを分割して取得できます。これらは、1 回のクエリで取得する行ブロックを指定します。

```sql
-- SQL Standard style:
[OFFSET offset_row_count {ROW | ROWS}] [FETCH {FIRST | NEXT} fetch_row_count {ROW | ROWS} {ONLY | WITH TIES}]

-- MySQL/PostgreSQL style:
[LIMIT [n, ]m] [OFFSET offset_row_count]
```

`offset_row_count` または `fetch_row_count` の値には、数値またはリテラル定数を指定できます。`fetch_row_count` は省略可能で、デフォルト値は 1 です。

`OFFSET` は、クエリ結果セットから行を返し始める前にスキップする行数を指定します。`OFFSET n` は、結果の先頭から `n` 行をスキップします。

負の OFFSET もサポートされています。`OFFSET -n` は、結果の末尾から `n` 行をスキップします。

小数の OFFSET もサポートされています。`OFFSET n` で 0 &lt; n &lt; 1 の場合、結果の先頭から n * 100% がスキップされます。

例:
• `OFFSET 0.1` - 結果の先頭 10% をスキップします。

> **注意**
> • 小数は、0 より大きく 1 未満の [Float64](../../data-types/float.md) である必要があります。
> • 計算の結果、行数が端数になった場合は、次の整数に切り上げられます。

`FETCH` は、クエリ結果に含めることができる最大行数を指定します。

`ONLY` オプションは、`OFFSET` によって省略された行の直後の行を返すために使用します。この場合、`FETCH` は [LIMIT](../../../sql-reference/statements/select/limit.md) 句の代わりとして使用できます。たとえば、次のクエリ

```sql
SELECT * FROM test_fetch ORDER BY a OFFSET 1 ROW FETCH FIRST 3 ROWS ONLY;
```

そのクエリと同じです

```sql
SELECT * FROM test_fetch ORDER BY a LIMIT 3 OFFSET 1;
```

`WITH TIES` オプションは、`ORDER BY` 句に従って結果セットの最後の行と同順位になる追加の行も返すために使用されます。たとえば、`fetch_row_count` が 5 に設定されていて、さらに 2 行が 5 行目の `ORDER BY` カラムの値と一致する場合、結果セットには 7 行が含まれます。

:::note
標準では、`OFFSET` 句と `FETCH` 句の両方がある場合、`OFFSET` 句は `FETCH` 句の前になければなりません。
:::

:::note
実際のオフセットは、[offset](../../../operations/settings/settings.md#offset) 設定に依存する場合もあります。
:::

<div id="examples">
  ## 例
</div>

入力テーブル：

```text
┌─a─┬─b─┐
│ 1 │ 1 │
│ 2 │ 1 │
│ 3 │ 4 │
│ 1 │ 3 │
│ 5 │ 4 │
│ 0 │ 6 │
│ 5 │ 7 │
└───┴───┘
```

`ONLY` オプションの使用例:

```sql title="Query"
SELECT * FROM test_fetch ORDER BY a OFFSET 3 ROW FETCH FIRST 3 ROWS ONLY;
```

```text title="Response"
┌─a─┬─b─┐
│ 2 │ 1 │
│ 3 │ 4 │
│ 5 │ 4 │
└───┴───┘
```

`WITH TIES` オプションの使用例:

```sql title="Query"
SELECT * FROM test_fetch ORDER BY a OFFSET 3 ROW FETCH FIRST 3 ROWS WITH TIES;
```

```text title="Response"
┌─a─┬─b─┐
│ 2 │ 1 │
│ 3 │ 4 │
│ 5 │ 4 │
│ 5 │ 7 │
└───┴───┘
```