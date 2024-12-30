---
slug: /ja/operations/analyzer
sidebar_label: アナライザー
title: アナライザー
description: ClickHouseのクエリアナライザーについての詳細
keywords: [アナライザー]
---

# アナライザー

<BetaBadge />

## 既知の非互換性

ClickHouseバージョン`24.3`では、新しいクエリアナライザーがデフォルトで有効になりました。多くのバグを修正し新たな最適化を導入した一方で、ClickHouseの動作にいくつかの破壊的変更が加わっています。新しいアナライザーに合わせてクエリを書き直す方法を理解するために、以下の変更をお読みください。

### 無効なクエリは最適化されなくなりました

以前のクエリプランニングインフラストラクチャでは、クエリの検証ステップよりも前にASTレベルの最適化が適用されていました。この最適化により、初期のクエリが書き換えられ、実行可能になっていました。

新しいアナライザーでは、最適化ステップの前にクエリの検証が行われます。これにより、以前は実行可能だった無効なクエリはサポートされなくなりました。このような場合、クエリを手動で修正する必要があります。

**例1:**

```sql
SELECT number
FROM numbers(1)
GROUP BY toString(number)
```

このクエリでは、`number`カラムが集計後に利用可能な`toString(number)`だけでプロジェクションリストに使用されています。古いアナライザーでは、`GROUP BY toString(number)`が`GROUP BY number`に最適化され、クエリが有効になっていました。

**例2:**

```sql
SELECT
    number % 2 AS n,
    sum(number)
FROM numbers(10)
GROUP BY n
HAVING number > 5
```

このクエリでも同じ問題が発生します。カラム`number`が異なるキーで集計後に使用されています。以前のクエリアナライザーはこのクエリを修正し、`HAVING`句から`WHERE`句に`number > 5`フィルタを移動しました。

クエリを修正するには、非集計カラムに適用されるすべての条件を標準SQL構文に従って`WHERE`セクションに移動する必要があります。
```sql
SELECT
    number % 2 AS n,
    sum(number)
FROM numbers(10)
WHERE number > 5
GROUP BY n
```

### 無効なクエリでのCREATE VIEW

新しいアナライザーでは常に型チェックが行われます。以前は、無効な`SELECT`クエリで`VIEW`を作成することが可能でした。最初の`SELECT`または`INSERT`（`MATERIALIZED VIEW`の場合）で失敗していました。

今では、そのような`VIEW`を作成することはできません。

**例:**

```sql
CREATE TABLE source (data String) ENGINE=MergeTree ORDER BY tuple();

CREATE VIEW some_view
AS SELECT JSONExtract(data, 'test', 'DateTime64(3)')
FROM source;
```

### `JOIN`句の既知の非互換性

#### プロジェクションからカラムを使用するジョイン

デフォルトでは、エイリアスは`SELECT`リストから`JOIN USING`キーとして使用できません。

新しい設定`analyzer_compatibility_join_using_top_level_identifier`を有効にすると、`JOIN USING`の動作が変わり、`SELECT`クエリのプロジェクションリストからの式に基づいて識別子を解決するのが優先され、左テーブルから直接カラムを使用しません。

**例:**

```sql
SELECT a + 1 AS b, t2.s
FROM Values('a UInt64, b UInt64', (1, 1)) AS t1
JOIN Values('b UInt64, s String', (1, 'one'), (2, 'two')) t2
USING (b);
```

`analyzer_compatibility_join_using_top_level_identifier`を`true`に設定すると、ジョイン条件は`t1.a + 1 = t2.b`と解釈され、従来のバージョンの動作と一致します。結果は`2, 'two'`となります。
設定が`false`の場合、ジョイン条件は`t1.b = t2.b`にデフォルト設定され、クエリは`2, 'one'`を返します。
`t1`に`b`が存在しない場合、クエリはエラーで失敗します。

#### `JOIN USING`と`ALIAS`/`MATERIALIZED`カラムの動作変更

新しいアナライザーでは、`ALIAS`や`MATERIALIZED`カラムを含む`JOIN USING`クエリで`*`を使用すると、デフォルトでこれらのカラムが結果セットに含まれます。

**例:**

```sql
CREATE TABLE t1 (id UInt64, payload ALIAS sipHash64(id)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t1 VALUES (1), (2);

CREATE TABLE t2 (id UInt64, payload ALIAS sipHash64(id)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t2 VALUES (2), (3);

SELECT * FROM t1
FULL JOIN t2 USING (payload);
```

新しいアナライザーでは、このクエリの結果には両方のテーブルから`id`とともに`payload`カラムが含まれます。これに対し、以前のアナライザーはこれらの`ALIAS`カラムを含めるために特定の設定（`asterisk_include_alias_columns`または`asterisk_include_materialized_columns`）を有効にする必要があり、カラムの順序が異なる場合がありました。

特に古いクエリを新しいアナライザーに移行する際には、`*`の代わりに`SELECT`句で明示的にカラムを指定することを推奨します。

#### `USING`句におけるタイプ修飾子の処理

新しいアナライザーでは、`USING`句で指定されたカラムの共通スーパータイプを求めるルールが標準化され、特に`LowCardinality`や`Nullable`のようなタイプ修飾子を扱う際に、より予測可能な結果を生み出します。

- `LowCardinality(T)`と`T`：`LowCardinality(T)`型のカラムが`T`型のカラムと結合される場合、共通のスーパータイプは`T`となり、`LowCardinality`修飾子は破棄されます。

- `Nullable(T)`と`T`：`Nullable(T)`型のカラムが`T`型のカラムと結合される場合、共通のスーパータイプは`Nullable(T)`となり、nullableの特性は保持されます。

**例:**

```sql
SELECT id, toTypeName(id) FROM Values('id LowCardinality(String)', ('a')) AS t1
FULL OUTER JOIN Values('id String', ('b')) AS t2
USING (id);
```

このクエリでは、`id`の共通スーパータイプは`String`と判断され、`t1`の`LowCardinality`修飾子は破棄されます。

### プロジェクションカラム名の変更

プロジェクション名の計算中、エイリアスは置き換えられません。

```sql
SELECT
    1 + 1 AS x,
    x + 1
SETTINGS enable_analyzer = 0
FORMAT PrettyCompact

   ┌─x─┬─plus(plus(1, 1), 1)─┐
1. │ 2 │                   3 │
   └───┴─────────────────────┘

SELECT
    1 + 1 AS x,
    x + 1
SETTINGS enable_analyzer = 1
FORMAT PrettyCompact

   ┌─x─┬─plus(x, 1)─┐
1. │ 2 │          3 │
   └───┴────────────┘
```

### 非互換の関数引数タイプ

新しいアナライザーでは、初期クエリアナリシス中に型推論が行われます。この変更により、型チェックはショートサーキット評価の前に行われ、`if`関数の引数は常に共通のスーパータイプを持たなければなりません。

**例:**

次のクエリは`There is no supertype for types Array(UInt8), String because some of them are Array and some of them are not`というエラーで失敗します：

```sql
SELECT toTypeName(if(0, [2, 3, 4], 'String'))
```

### 異種クラスター

新しいアナライザーはクラスタ内のサーバー間の通信プロトコルを大幅に変更しました。そのため、異なる`enable_analyzer`設定値を持つサーバーで分散クエリを実行することは不可能です。

### ミューテーションは以前のアナライザーによって解釈されます

ミューテーションはまだ古いアナライザーを使用しており、このため、新しいClickHouse SQL機能はミューテーションで使用できません。例えば、`QUALIFY`句です。ステータスは[こちら](https://github.com/ClickHouse/ClickHouse/issues/61563)で確認できます。

### サポートされていない機能

新しいアナライザーが現時点でサポートしていない機能の一覧:

- Annoyインデックス。
- Hypothesisインデックス。進行中の作業は[こちら](https://github.com/ClickHouse/ClickHouse/pull/48381)。
- ウィンドウビューはサポートされていません。将来的にサポートされる予定はありません。
