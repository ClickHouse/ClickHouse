---
description: 'LIMIT BY 句のドキュメント'
sidebar_label: 'LIMIT BY'
slug: /sql-reference/statements/select/limit-by
title: 'LIMIT BY 句'
doc_type: 'reference'
---

`LIMIT n BY expressions` 句を含むクエリは、`expressions` の値の異なる組み合わせごとに、先頭から `n` 行を選択します。`LIMIT BY` のキーには、任意の数の [expressions](/ja/sql-reference/syntax#expressions) を含めることができます。

ClickHouse は次の構文バリエーションをサポートしています。

* `LIMIT [offset_value, ]n BY expressions`
* `LIMIT n OFFSET offset_value BY expressions`

クエリの処理中、ClickHouse はソートキー順に並んだデータを選択します。ソートキーは、[ORDER BY](/ja/sql-reference/statements/select/order-by) 句で明示的に設定することも、テーブルエンジンのプロパティとして暗黙的に設定することもできます (行の順序が保証されるのは [ORDER BY](/ja/sql-reference/statements/select/order-by) を使用した場合のみです。それ以外では、マルチスレッド処理のため行ブロックの順序は保証されません) 。その後、ClickHouse は `LIMIT n BY expressions` を適用し、`expressions` の異なる組み合わせごとに先頭の `n` 行を返します。`OFFSET` が指定されている場合、ClickHouse は `expressions` の異なる組み合わせに属する各データブロックについて、ブロックの先頭から `offset_value` 行をスキップし、結果として最大 `n` 行を返します。`offset_value` がデータブロック内の行数より大きい場合、ClickHouse はそのブロックからは行を返しません。

:::note
`LIMIT BY` は [LIMIT](../../../sql-reference/statements/select/limit.md) とは無関係です。両方を同じクエリ内で使用できます。
:::

`LIMIT BY` 句でカラム名の代わりにカラム番号を使用するには、設定 [enable&#95;positional&#95;arguments](/ja/operations/settings/settings#enable_positional_arguments) を有効にしてください。

<div id="examples">
  ## 例
</div>

サンプルテーブル:

```sql
CREATE TABLE limit_by(id Int, val Int) ENGINE = Memory;
INSERT INTO limit_by VALUES (1, 10), (1, 11), (1, 12), (2, 20), (2, 21);
```

クエリ:

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 2 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  10 │
│  1 │  11 │
│  2 │  20 │
│  2 │  21 │
└────┴─────┘
```

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 1, 2 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  21 │
└────┴─────┘
```

`SELECT * FROM limit_by ORDER BY id, val LIMIT 2 OFFSET 1 BY id` クエリでも、同じ結果が得られます。

次のクエリは、各 `domain, device_type` の組み合わせごとに上位 5 件のリファラーを返し、全体の行数は最大 100 行です (`LIMIT n BY + LIMIT`) 。

```sql
SELECT
    domainWithoutWWW(URL) AS domain,
    domainWithoutWWW(REFERRER_URL) AS referrer,
    device_type,
    count() cnt
FROM hits
GROUP BY domain, referrer, device_type
ORDER BY cnt DESC
LIMIT 5 BY domain, device_type
LIMIT 100;
```

`LIMIT BY` は、負の LIMIT や OFFSET でも使用できます。[負の LIMIT 句](/ja/sql-reference/statements/select/limit#negative-limits) と同様に、`LIMIT BY` でも負の値を使って、各グループの*末尾*から行を選択できます。

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT -2 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  20 │
│  2 │  21 │
└────┴─────┘
```

各 `id` について、最後の 2 行を返します。`id = 1` では `11` と `12` の行が返されます。`id = 2` では、グループには 2 行しかないため、2 行とも返されます。

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT -1 OFFSET -1 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  11 │
│  2 │  20 │
└────┴─────┘
```

各`id`について最後から2番目の行を返します。末尾の`OFFSET -1`で各グループの最後の行を除外し、先頭の`-1`で残った中の最後の行を保持します。

符号の異なる`LIMIT`と`OFFSET`を組み合わせることもできます。たとえば、各グループの先頭の行を除外してから、残った中の最後の2行を保持するには:

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT -2 OFFSET 1 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  21 │
└────┴─────┘
```

`id = 1` の場合、最初の行 (`10`) はスキップされ、`11, 12` の最後の 2 行が返されます。`id = 2` の場合、最初の行 (`20`) はスキップされ、`21` だけが残ります。

<div id="limit-by-all">
  ## LIMIT BY ALL
</div>

`LIMIT BY ALL` は、集約関数ではない `SELECT` 対象のすべての式を列挙するのと同等です。

例:

```sql
SELECT col1, col2, col3 FROM table LIMIT 2 BY ALL;
```

と同じです

```sql
SELECT col1, col2, col3 FROM table LIMIT 2 BY col1, col2, col3;
```

特別なケースとして、集約関数とその他のフィールドの両方を引数に取る関数がある場合、`LIMIT BY` のキーには、そこから抽出できる非集約フィールドが最大限含まれます。

例:

```sql
SELECT substring(a, 4, 2), substring(substring(a, 1, 2), 1, count(b)) FROM t LIMIT 2 BY ALL;
```

と同じです

```sql
SELECT substring(a, 4, 2), substring(substring(a, 1, 2), 1, count(b)) FROM t LIMIT 2 BY substring(a, 4, 2), substring(a, 1, 2);
```

<div id="examples">
  ## 例
</div>

サンプルテーブル：

```sql
CREATE TABLE limit_by(id Int, val Int) ENGINE = Memory;
INSERT INTO limit_by VALUES (1, 10), (1, 11), (1, 12), (2, 20), (2, 21);
```

クエリ:

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 2 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  10 │
│  1 │  11 │
│  2 │  20 │
│  2 │  21 │
└────┴─────┘
```

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 1, 2 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  21 │
└────┴─────┘
```

`SELECT * FROM limit_by ORDER BY id, val LIMIT 2 OFFSET 1 BY id` クエリでも同じ結果が返されます。

`LIMIT BY ALL` を使用すると:

```sql
SELECT id, val FROM limit_by ORDER BY id, val LIMIT 2 BY ALL;
```

これは以下と同等です:

```sql
SELECT id, val FROM limit_by ORDER BY id, val LIMIT 2 BY id, val;
```