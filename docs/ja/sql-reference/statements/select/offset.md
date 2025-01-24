---
slug: /ja/sql-reference/statements/select/offset
sidebar_label: OFFSET
title: "OFFSET FETCH 句"
---

`OFFSET` と `FETCH` はデータを部分ごとに取得することを可能にします。これは、単一のクエリによって取得したい行のブロックを指定します。

``` sql
OFFSET offset_row_count {ROW | ROWS}] [FETCH {FIRST | NEXT} fetch_row_count {ROW | ROWS} {ONLY | WITH TIES}]
```

`offset_row_count` や `fetch_row_count` の値は、数値またはリテラル定数にすることができます。`fetch_row_count` を省略することもでき、その場合、デフォルトは1です。

`OFFSET` は、クエリ結果セットから行を返し始める前にスキップする行数を指定します。

`FETCH` は、クエリ結果の行数の最大値を指定します。

`ONLY` オプションは、`OFFSET` によって省略された行に続く行を返すために使用されます。この場合、`FETCH` は [LIMIT](../../../sql-reference/statements/select/limit.md) 句の代替となります。例えば、次のクエリは

``` sql
SELECT * FROM test_fetch ORDER BY a OFFSET 1 ROW FETCH FIRST 3 ROWS ONLY;
```

次のクエリと同一です:

``` sql
SELECT * FROM test_fetch ORDER BY a LIMIT 3 OFFSET 1;
```

`WITH TIES` オプションは、`ORDER BY` 句に従って結果セットの最終位置と同着となる追加の行を返すために使用されます。例えば、`fetch_row_count` が5の場合で、2つの追加の行が5行目の `ORDER BY` カラムの値と一致するなら、結果セットには7行が含まれることになります。

:::note    
標準に従えば、`OFFSET` 句は `FETCH` 句がある場合、その前に記述する必要があります。
:::

:::note    
実際のオフセットは、[offset](../../../operations/settings/settings.md#offset) 設定にも依存することがあります。
:::

## 例

入力テーブル:

``` text
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

``` sql
SELECT * FROM test_fetch ORDER BY a OFFSET 3 ROW FETCH FIRST 3 ROWS ONLY;
```

結果:

``` text
┌─a─┬─b─┐
│ 2 │ 1 │
│ 3 │ 4 │
│ 5 │ 4 │
└───┴───┘
```

`WITH TIES` オプションの使用例:

``` sql
SELECT * FROM test_fetch ORDER BY a OFFSET 3 ROW FETCH FIRST 3 ROWS WITH TIES;
```

結果:

``` text
┌─a─┬─b─┐
│ 2 │ 1 │
│ 3 │ 4 │
│ 5 │ 4 │
│ 5 │ 7 │
└───┴───┘
```
