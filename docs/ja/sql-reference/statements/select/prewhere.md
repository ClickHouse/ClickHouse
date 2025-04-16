---
slug: /ja/sql-reference/statements/select/prewhere
sidebar_label: PREWHERE
---

# PREWHERE句

Prewhereは、より効率的にフィルタリングを適用するための最適化です。`PREWHERE`句が明示的に指定されていなくてもデフォルトで有効化されています。[WHERE](../../../sql-reference/statements/select/where.md)条件の一部を自動的にprewhere段階に移動させることで動作します。`PREWHERE`句の役割は、デフォルトの動作よりも優れた方法で最適化を制御できると思われる場合に、その最適化を制御することです。

prewhere最適化を使用することで、まずprewhere式の実行に必要なカラムだけが読み込まれます。その後、クエリの残りを実行するために必要な他のカラムが読み込まれますが、それはprewhere式が少なくとも一部の行で`true`であるブロックだけです。もしprewhere式がすべての行で`false`であるブロックが多く、他のクエリの部分よりもprewhereで必要なカラムの数が少ない場合、クエリ実行のためにディスクから読み込むデータ量を大幅に減らすことができます。

## Prewhereの手動制御

この句は`WHERE`句と同じ意味を持ちます。違いはテーブルから読み込まれるデータにあります。クエリ内のカラムの一部しか使用しないが、強力なデータフィルタリングを提供するフィルタリング条件で`PREWHERE`を手動で制御すると、読み込むデータの量が減少します。

クエリは`PREWHERE`と`WHERE`を同時に指定することができます。この場合、`PREWHERE`が`WHERE`に先行します。

[optimize_move_to_prewhere](../../../operations/settings/settings.md#optimize_move_to_prewhere)設定が0に設定されている場合、`WHERE`から`PREWHERE`への式の部分を自動的に移動するヒューリスティクスは無効化されます。

クエリが[FINAL](from.md#select-from-final)修飾子を持つ場合、`PREWHERE`の最適化は必ずしも正しくありません。それは[optimize_move_to_prewhere](../../../operations/settings/settings.md#optimize_move_to_prewhere)と[optimize_move_to_prewhere_if_final](../../../operations/settings/settings.md#optimize_move_to_prewhere_if_final)の両設定がオンになっている場合にのみ有効です。

:::note    
`PREWHERE`セクションは`FINAL`の前に実行されるため、テーブルの`ORDER BY`セクションにないフィールドを使用する`PREWHERE`を使用すると、`FROM ... FINAL`クエリの結果が偏ることがあります。
:::

## 制限

`PREWHERE`は[*MergeTree](../../../engines/table-engines/mergetree-family/index.md)ファミリのテーブルでのみサポートされています。

## 例

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

-- どの述語がPREWHEREに移動されたかを見るためにトレースを有効にしましょう
set send_logs_level='debug';

MergeTreeWhereOptimizer: condition "B = 0" moved to PREWHERE  
-- Clickhouseは自動で`B = 0`をPREWHEREに移動しますが、Bは常に0なので意味がありません。

-- 他の述語`C = 'x'`を移動しましょう

SELECT count()
FROM mydata
PREWHERE C = 'x'
WHERE B = 0;

1 row in set. Elapsed: 0.069 sec. Processed 10.00 million rows, 158.89 MB (144.90 million rows/s., 2.30 GB/s.)

-- 手動の`PREWHERE`を使ったこのクエリはわずかに少ないデータを処理します: 158.89 MB VS 168.89 MB
```
