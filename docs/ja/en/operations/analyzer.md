---
description: 'ClickHouseのクエリアナライザについて詳しく説明するページ'
keywords: ['analyzer']
sidebar_label: 'アナライザ'
slug: /operations/analyzer
title: 'アナライザ'
doc_type: 'reference'
---

ClickHouse バージョン `24.3` では、新しいクエリアナライザがデフォルトで有効化されました。
その仕組みの詳細については、[こちら](/ja/guides/developer/understanding-query-execution-with-the-analyzer#analyzer)をご覧ください。

<div id="known-incompatibilities">
  ## 既知の非互換性
</div>

多数のバグ修正と新たな最適化の導入に加え、ClickHouse の動作には後方互換性のない変更もいくつか含まれています。アナライザ向けにクエリをどのように書き換える必要があるかを判断するため、以下の変更点をお読みください。

<div id="invalid-queries-are-no-longer-optimized">
  ### 無効なクエリは最適化されなくなりました
</div>

従来のクエリプランニングのインフラストラクチャでは、クエリの検証ステップの前に AST レベルの最適化が適用されていました。
最適化によって、初期クエリが有効で実行可能な形に書き換えられることがありました。

アナライザでは、クエリの検証は最適化ステップの前に行われます。
そのため、以前は実行できていた無効なクエリは、現在ではサポートされません。
このような場合は、クエリを手動で修正する必要があります。

<div id="example-1">
  #### 例 1
</div>

次のクエリでは、集約後に `toString(number)` しか使えないにもかかわらず、PROJECTIONリストでカラム `number` を使用しています。
古いアナライザでは、`GROUP BY toString(number)` が `GROUP BY number,` に最適化されていたため、このクエリは有効でした。

```sql
SELECT number
FROM numbers(1)
GROUP BY toString(number)
```

<div id="example-2">
  #### 例 2
</div>

このクエリでも同じ問題が発生します。カラム `number` は、別のキーで集約した後に使用されています。
従来のクエリアナライザでは、`number > 5` のフィルタを `HAVING` 句から `WHERE` 句に移動することで、このクエリを修正していました。

```sql
SELECT
    number % 2 AS n,
    sum(number)
FROM numbers(10)
GROUP BY n
HAVING number > 5
```

クエリを修正するには、標準的な SQL 構文に従い、非集計カラムに対するすべての条件を `WHERE` 句に移動する必要があります。

```sql
SELECT
    number % 2 AS n,
    sum(number)
FROM numbers(10)
WHERE number > 5
GROUP BY n
```

<div id="create-view-with-invalid-query">
  ### 無効なクエリでの `CREATE VIEW`
</div>

アナライザは常に型チェックを行います。
以前は、無効な `SELECT` クエリを含む `VIEW` を作成できました。
その場合、最初の `SELECT` 実行時、または `MATERIALIZED VIEW` では最初の `INSERT` 実行時に失敗していました。

現在は、このような `VIEW` を作成することはできません。

<div id="example-view">
  #### 例
</div>

```sql
CREATE TABLE source (data String)
ENGINE=MergeTree
ORDER BY tuple();

CREATE VIEW some_view
AS SELECT JSONExtract(data, 'test', 'DateTime64(3)')
FROM source;
```

<div id="known-incompatibilities-of-the-join-clause">
  ### `JOIN` 句の既知の非互換性
</div>

<div id="join-using-column-from-projection">
  #### PROJECTIONのカラムを使用する `JOIN`
</div>

`SELECT` リストのエイリアスは、デフォルトでは `JOIN USING` のキーとして使用できません。

新しい設定 `analyzer_compatibility_join_using_top_level_identifier` を有効にすると、`JOIN USING` の動作が変わり、左テーブルのカラムを直接使うのではなく、`SELECT` クエリのPROJECTIONリスト内の式に基づいて識別子を優先的に解決するようになります。

例えば:

```sql
SELECT a + 1 AS b, t2.s
FROM VALUES('a UInt64, b UInt64', (1, 1)) AS t1
JOIN VALUES('b UInt64, s String', (1, 'one'), (2, 'two')) t2
USING (b);
```

`analyzer_compatibility_join_using_top_level_identifier` を `true` に設定すると、JOIN 条件は `t1.a + 1 = t2.b` と解釈され、以前のバージョンの動作と一致します。
結果は `2, 'two'` になります。
設定が `false` の場合、JOIN 条件はデフォルトで `t1.b = t2.b` となり、クエリは `2, 'one'` を返します。
`t1` に `b` が存在しない場合、クエリはエラーになります。

<div id="changes-in-behavior-with-join-using-and-aliasmaterialized-columns">
  #### `JOIN USING` と `ALIAS`/`MATERIALIZED` カラムに関する動作変更
</div>

アナライザでは、`ALIAS` または `MATERIALIZED` カラムを含む `JOIN USING` クエリで `*` を使用すると、デフォルトでそれらのカラムが結果セットに含まれます。

たとえば:

```sql
CREATE TABLE t1 (id UInt64, payload ALIAS sipHash64(id)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t1 VALUES (1), (2);

CREATE TABLE t2 (id UInt64, payload ALIAS sipHash64(id)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t2 VALUES (2), (3);

SELECT * FROM t1
FULL JOIN t2 USING (payload);
```

アナライザでは、このクエリの結果に、両方のテーブルの `id` に加えて `payload` カラムが含まれます。
一方、以前のアナライザでは、これらの `ALIAS` カラムは特定の設定 (`asterisk_include_alias_columns` または `asterisk_include_materialized_columns`) が有効になっている場合にのみ含まれ、
さらに、カラムの順序が異なる可能性もありました。

一貫性があり想定どおりの結果を得るため、特に古いクエリをアナライザに移行する際は、`*` を使用するのではなく、`SELECT` 句でカラムを明示的に指定することを推奨します。

<div id="handling-of-type-modifiers-for-columns-in-using-clause">
  #### `USING` 句におけるカラムの型修飾子の扱い
</div>

新しいバージョンのアナライザでは、`USING` 句で指定されたカラムの共通スーパータイプを決定するルールが標準化され、より予測しやすい結果が得られるようになりました。
特に、`LowCardinality` や `Nullable` のような型修飾子を扱う場合に効果を発揮します。

* `LowCardinality(T)` と `T`: 型 `LowCardinality(T)` のカラムを型 `T` のカラムと結合すると、結果の共通スーパータイプは `T` となり、`LowCardinality` 修飾子は実質的に取り除かれます。
* `Nullable(T)` と `T`: 型 `Nullable(T)` のカラムを型 `T` のカラムと結合すると、結果の共通スーパータイプは `Nullable(T)` となり、Nullable の性質が維持されます。

例えば:

```sql
SELECT id, toTypeName(id)
FROM VALUES('id LowCardinality(String)', ('a')) AS t1
FULL OUTER JOIN VALUES('id String', ('b')) AS t2
USING (id);
```

このクエリでは、`id` の共通のスーパータイプは `String` と判断され、`t1` の `LowCardinality` 修飾子は破棄されます。

<div id="projection-column-names-changes">
  ### PROJECTIONのカラム名の変更
</div>

PROJECTION名の計算時には、別名は置換されません。

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

<div id="incompatible-function-arguments-types">
  ### 互換性のない関数引数の型
</div>

アナライザでは、型推論は初期クエリ分析の段階で行われます。
この変更により、型チェックは短絡評価より前に実行されるようになりました。そのため、`if` 関数の引数は常に共通のスーパータイプである必要があります。

たとえば、次のクエリは `There is no supertype for types Array(UInt8), String because some of them are Array and some of them are not` というエラーで失敗します。

```sql
SELECT toTypeName(if(0, [2, 3, 4], 'String'))
```

<div id="heterogeneous-clusters">
  ### 異種クラスター
</div>

アナライザは、クラスター内のサーバー間の通信プロトコルを大きく変更します。そのため、`enable_analyzer` の設定値が異なるサーバー間では、分散クエリを実行できません。

<div id="mutations-are-interpreted-by-previous-analyzer">
  ### ミューテーションは旧アナライザで解釈されます
</div>

ミューテーションでは、現在も旧アナライザが使用されています。
そのため、ClickHouse SQL の新しい機能の一部はミューテーションでは使用できません。たとえば、`QUALIFY` 句です。
対応状況は[こちら](https://github.com/ClickHouse/ClickHouse/issues/61563)で確認できます。

<div id="unsupported-features">
  ### 未サポートの機能
</div>

現在アナライザがサポートしていない機能の一覧を以下に示します。

* Annoy 索引。
* Hypothesis 索引。現在[こちら](https://github.com/ClickHouse/ClickHouse/pull/48381)で対応が進められています。
* Window view はサポートされていません。今後もサポートする予定はありません。

<div id="cloud-migration">
  ## Cloud 移行
</div>

新しいクエリアナライザによる新機能とパフォーマンス最適化をサポートするため、現在無効化されているすべてのインスタンスでこれを有効にします。この変更により SQL のスコープ規則がより厳格に適用されるため、お客様は準拠していないクエリを手動で更新する必要があります。

<div id="migration-workflow">
  ### 移行ワークフロー
</div>

1. `normalized_query_hash` を使って `system.query_log` を絞り込み、対象のクエリを特定します。

```sql
SELECT query 
FROM clusterAllReplicas(default, system.query_log)
WHERE normalized_query_hash='{hash}' 
LIMIT 1 
SETTINGS skip_unavailable_shards=1
```

2. これらの設定を追加し、アナライザを有効にしてクエリを実行します。

```sql
SETTINGS
    enable_analyzer=1,
    analyzer_compatibility_join_using_top_level_identifier=1
```

3. クエリ結果を調整して検証し、アナライザを無効にしたときに生成される出力と一致することを確認します。

内部テストで特に頻繁に見られた非互換性については、以下を参照してください。

<div id="unknown-expression-identifier">
  ### 不明な式識別子
</div>

エラー: `Unknown expression identifier ... in scope ... (UNKNOWN_IDENTIFIER)`. 例外コード: 47

原因: フィルター内で計算済みの別名を参照したり、曖昧なサブクエリの選択項目や「動的」な CTE スコープを使ったりするなど、非標準で寛容なレガシー動作に依存するクエリは、現在では無効として適切に判定され、即座に拒否されます。

解決策: 次のように SQL パターンを修正してください。

* フィルターロジック: 結果に対してフィルタリングする場合は条件を WHERE から HAVING に移し、元データに対してフィルタリングする場合は WHERE に同じ式を重ねて記述します。
* サブクエリのスコープ: 外側のクエリで必要になるカラムは、すべて明示的に選択します。
* 結合キー: キーが別名の場合は、USING ではなく ON を使って完全な式を指定します。
* 外側のクエリでは、その中のテーブルではなく、サブクエリ/CTE 自体の別名を参照します。

<div id="non-aggregated-columns-in-group-by">
  ### GROUP BY 内の非集計カラム
</div>

エラー: `Column ... is not under aggregate function and not in GROUP BY keys (NOT_AN_AGGREGATE)`。Exception code: 215

原因: 以前のアナライザでは、GROUP BY 句に含まれていないカラムも選択できました (その場合、多くは任意の値が選ばれていました) 。アナライザは標準 SQL に従うため、選択する各カラムは集計対象であるか、グループ化キーである必要があります。

解決策: カラムを `any()` または `argMax()` で囲むか、GROUP BY に追加してください。

```sql
/* ORIGINAL QUERY */
-- device_id is ambiguous
SELECT user_id, device_id FROM table GROUP BY user_id

/* FIXED QUERY */
SELECT user_id, any(device_id) FROM table GROUP BY user_id
-- OR
SELECT user_id, device_id FROM table GROUP BY user_id, device_id
```

<div id="duplicate-cte-names">
  ### 重複する CTE 名
</div>

エラー: `CTE with name ... already exists (MULTIPLE_EXPRESSIONS_FOR_ALIAS)`. 例外コード: 179

原因: 以前のアナライザでは、同じ名前の複数の共通テーブル式 (WITH ...) を定義でき、後から定義したものが先に定義したものを隠すことが許可されていました。アナライザでは、このような曖昧さは許可されません。

解決策: 重複している CTE の名前をリネームして、一意にしてください。

```sql
/* ORIGINAL QUERY */
WITH 
  data AS (SELECT 1 AS id), 
  data AS (SELECT 2 AS id) -- Redefined
SELECT * FROM data;

/* FIXED QUERY */
WITH 
  raw_data AS (SELECT 1 AS id), 
  processed_data AS (SELECT 2 AS id)
SELECT * FROM processed_data;
```

<div id="ambiguous-column-identifiers">
  ### あいまいなカラム識別子
</div>

エラー: `JOIN [JOIN TYPE] ambiguous identifier ... (AMBIGUOUS_IDENTIFIER)` 例外コード: 207

原因: クエリで、JOIN 内の複数のテーブルに存在するカラム名を、どのテーブルのものか指定せずに参照しています。古いアナライザは内部ロジックに基づいてカラムを推測することがよくありましたが、アナライザでは明示的に名前を指定する必要があります。

解決策: `table&#95;alias.column&#95;name` のように、カラムを完全修飾してください。

```sql
/* ORIGINAL QUERY */
SELECT table1.ID AS ID FROM table1, table2 WHERE ID...

/* FIXED QUERY */
SELECT table1.ID AS ID_RENAMED FROM table1, table2 WHERE ID_RENAMED...
```

<div id="invalid-usage-of-final">
  ### FINAL の無効な使用
</div>

エラー: `Table expression modifiers FINAL are not supported for subquery...` または `Storage ... doesn't support FINAL` (`UNSUPPORTED_METHOD`)。例外コード: 1, 181

原因: FINAL はテーブルストレージ (特に [Shared]ReplacingMergeTree) に対する修飾子です。アナライザは、次の対象に FINAL を適用すると拒否します。

* サブクエリまたは派生テーブル (例: FROM (SELECT ...) FINAL) 。
* FINAL をサポートしていないテーブルエンジン (例: SharedMergeTree) 。

解決策: FINAL はサブクエリ内のソーステーブルにのみ適用するか、エンジンがサポートしていない場合は削除してください。

```sql
/* ORIGINAL QUERY */
SELECT * FROM (SELECT * FROM my_table) AS subquery FINAL ...

/* FIXED QUERY */
SELECT * FROM (SELECT * FROM my_table FINAL) AS subquery ...
```

<div id="countdistinct-case-insensitivity">
  ### `countDistinct()` 関数の大文字と小文字の非区別
</div>

エラー: `Function with name countdistinct does not exist (UNKNOWN_FUNCTION)`. 例外コード: 46

原因: 関数名では大文字と小文字が区別されるか、アナライザで厳密にマッピングされます。`countdistinct` (すべて小文字) は自動的に解決されなくなりました。

解決策: 標準の `countDistinct` (camelCase) または ClickHouse 固有の uniq を使用してください。