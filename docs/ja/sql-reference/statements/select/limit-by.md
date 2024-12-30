---
slug: /ja/sql-reference/statements/select/limit-by
sidebar_label: LIMIT BY
---

# LIMIT BY 句

`LIMIT n BY expressions` 句を持つクエリは、`expressions` の各異なる値の最初の `n` 行を選択します。`LIMIT BY` のキーには任意の数の[式](../../../sql-reference/syntax.md#syntax-expressions)を含めることができます。

ClickHouse は以下の構文のバリエーションをサポートしています：

- `LIMIT [offset_value, ]n BY expressions`
- `LIMIT n OFFSET offset_value BY expressions`

クエリの処理中、ClickHouse はソートキーによってデータを選択します。ソートキーは[ORDER BY](order-by.md#select-order-by) 句を使用して明示的に設定するか、テーブルエンジンのプロパティとして暗黙的に設定されます（行順序が保証されるのは [ORDER BY](order-by.md#select-order-by) を使用した場合のみであり、そうでなければマルチスレッドのために行ブロックは順序付けられません）。その後、ClickHouse は `LIMIT n BY expressions` を適用し、`expressions` の各異なる組み合わせの最初の `n` 行を返します。`OFFSET` が指定されている場合、`expressions` の異なる組み合わせに属する各データブロックに対して、ClickHouse はブロックの最初から `offset_value` の行数をスキップし、最大 `n` 行を結果として返します。`offset_value` がデータブロック内の行数を超えている場合、ClickHouse はブロックからゼロ行を返します。

:::note    
`LIMIT BY` は [LIMIT](../../../sql-reference/statements/select/limit.md) とは関係ありません。両方を同じクエリ内で使用することができます。
:::

`LIMIT BY` 句でカラム名の代わりにカラム番号を使用したい場合は、設定 [enable_positional_arguments](../../../operations/settings/settings.md#enable-positional-arguments) を有効にしてください。

## 例

サンプルテーブル：

``` sql
CREATE TABLE limit_by(id Int, val Int) ENGINE = Memory;
INSERT INTO limit_by VALUES (1, 10), (1, 11), (1, 12), (2, 20), (2, 21);
```

クエリ：

``` sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 2 BY id
```

``` text
┌─id─┬─val─┐
│  1 │  10 │
│  1 │  11 │
│  2 │  20 │
│  2 │  21 │
└────┴─────┘
```

``` sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 1, 2 BY id
```

``` text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  21 │
└────┴─────┘
```

`SELECT * FROM limit_by ORDER BY id, val LIMIT 2 OFFSET 1 BY id` クエリは同じ結果を返します。

以下のクエリは、各 `domain, device_type` ペアの上位5つのリファラーを、最大100行の合計（`LIMIT n BY + LIMIT`）で返します。

``` sql
SELECT
    domainWithoutWWW(URL) AS domain,
    domainWithoutWWW(REFERRER_URL) AS referrer,
    device_type,
    count() cnt
FROM hits
GROUP BY domain, referrer, device_type
ORDER BY cnt DESC
LIMIT 5 BY domain, device_type
LIMIT 100
```

