---
description: 'PREWHERE 句のドキュメント'
sidebar_label: 'PREWHERE'
slug: /sql-reference/statements/select/prewhere
title: 'PREWHERE 句'
doc_type: 'reference'
---

Prewhere は、フィルタリングをより効率的に適用するための最適化です。`PREWHERE` 句を明示的に指定しなくても、デフォルトで有効になっています。これは、[WHERE](../../../sql-reference/statements/select/where.md) 条件の一部を自動的に prewhere 段階へ移すことで機能します。`PREWHERE` 句の役割は、この最適化を制御することだけです。デフォルトの動作よりも自分のほうが適切に制御できると考える場合に使用します。

prewhere 最適化では、まず prewhere 式の実行に必要なカラムだけが読み取られます。次に、クエリの残りの部分の実行に必要なほかのカラムが読み取られますが、これは少なくとも一部の行で prewhere 式が `true` になるブロックに対してのみ行われます。すべての行で prewhere 式が `false` になるブロックが多く、かつ prewhere に必要なカラム数がクエリのほかの部分より少ない場合は、クエリ実行時にディスクから読み取るデータ量を大幅に減らせることがよくあります。

<div id="controlling-prewhere-manually">
  ## PREWHERE の手動制御
</div>

この句は `WHERE` 句と同じ意味を持ちます。違いは、テーブルからどのデータを読み取るかにあります。クエリ内で少数のカラムにしか使われない一方で、高い絞り込み効果を持つフィルタ条件に対して PREWHERE を手動で制御すると、読み取るデータ量を削減できます。

クエリでは PREWHERE と `WHERE` を同時に指定できます。この場合、PREWHERE が `WHERE` に先に適用されます。

[optimize&#95;move&#95;to&#95;prewhere](../../../operations/settings/settings.md#optimize_move_to_prewhere) 設定が 0 に設定されている場合、式の一部を `WHERE` から PREWHERE に自動的に移動するヒューリスティクスは無効になります。

クエリに [FINAL](/ja/sql-reference/statements/select/from#final-modifier) 修飾子がある場合、PREWHERE の最適化は必ずしも正しく機能するとは限りません。これは、[optimize&#95;move&#95;to&#95;prewhere](../../../operations/settings/settings.md#optimize_move_to_prewhere) と [optimize&#95;move&#95;to&#95;prewhere&#95;if&#95;final](../../../operations/settings/settings.md#optimize_move_to_prewhere_if_final) の両方の設定が有効になっている場合にのみ有効です。

:::note
PREWHERE 句は `FINAL` より前に実行されるため、テーブルの `ORDER BY` 句に含まれないフィールドで PREWHERE を使用すると、`FROM ... FINAL` クエリの結果が偏る可能性があります。
:::

<div id="limitations">
  ## 制限事項
</div>

`PREWHERE` は、[*MergeTree](../../../engines/table-engines/mergetree-family/index.md) 系のテーブルでのみサポートされています。

<div id="example">
  ## 例
</div>

```sql
CREATE TABLE mydata
(
    `A` Int64,
    `B` Int8,
    `C` String
)
ENGINE = MergeTree
ORDER BY A AS
SELECT
    number,
    0,
    if(number between 1000 and 2000, 'x', toString(number))
FROM numbers(10000000);

SELECT count()
FROM mydata
WHERE (B = 0) AND (C = 'x');

1 row in set. Elapsed: 0.074 sec. Processed 10.00 million rows, 168.89 MB (134.98 million rows/s., 2.28 GB/s.)

-- let's enable tracing to see which predicate are moved to PREWHERE
set send_logs_level='debug';

MergeTreeWhereOptimizer: condition "B = 0" moved to PREWHERE  
-- Clickhouse moves automatically `B = 0` to PREWHERE, but it has no sense because B is always 0.

-- Let's move other predicate `C = 'x'` 

SELECT count()
FROM mydata
PREWHERE C = 'x'
WHERE B = 0;

1 row in set. Elapsed: 0.069 sec. Processed 10.00 million rows, 158.89 MB (144.90 million rows/s., 2.30 GB/s.)

-- This query with manual `PREWHERE` processes slightly less data: 158.89 MB VS 168.89 MB
```