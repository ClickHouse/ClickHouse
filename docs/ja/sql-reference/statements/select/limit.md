---
slug: /ja/sql-reference/statements/select/limit
sidebar_label: LIMIT
---

# LIMIT句

`LIMIT m`を使用すると、結果から最初の`m`行を選択できます。

`LIMIT n, m`を使用すると、最初の`n`行をスキップした後の結果から`m`行を選択できます。`LIMIT m OFFSET n`の構文はこれと同等です。

`n`と`m`は非負整数でなければなりません。

結果を明示的にソートする[ORDER BY](../../../sql-reference/statements/select/order-by.md)句がない場合、結果に選ばれる行は任意で非決定的なものになる可能性があります。

:::note    
結果セット内の行数は、[limit](../../../operations/settings/settings.md#limit)設定にも依存する場合があります。
:::

## LIMIT ... WITH TIES修飾子

`LIMIT n[,m]`に`WITH TIES`修飾子を設定し、`ORDER BY expr_list`を指定した場合、`LIMIT n`で指定した位置`n`または`LIMIT n,m`で指定した位置`m`にある行と同じ`ORDER BY`フィールドの値を持つすべての行と、最初の`n`または`n,m`行を結果として取得します。

この修飾子は、[ORDER BY ... WITH FILL修飾子](../../../sql-reference/statements/select/order-by.md#orderby-with-fill)とも組み合わせることができます。

例えば、次のクエリ

``` sql
SELECT * FROM (
    SELECT number%50 AS n FROM numbers(100)
) ORDER BY n LIMIT 0,5
```

は、次の結果を返します。

``` text
┌─n─┐
│ 0 │
│ 0 │
│ 1 │
│ 1 │
│ 2 │
└───┘
```

しかし、`WITH TIES`修飾子を適用すると

``` sql
SELECT * FROM (
    SELECT number%50 AS n FROM numbers(100)
) ORDER BY n LIMIT 0,5 WITH TIES
```

別の行セットが返されます。

``` text
┌─n─┐
│ 0 │
│ 0 │
│ 1 │
│ 1 │
│ 2 │
│ 2 │
└───┘
```

これは、行番号6がフィールド`n`に対して行番号5と同じ値「2」を持っているためです。
