---
description: 'ClickHouse でクエリ条件キャッシュ機能を使用および設定するためのガイド'
sidebar_label: 'クエリ条件キャッシュ'
sidebar_position: 64
slug: /operations/query-condition-cache
title: 'クエリ条件キャッシュ'
doc_type: 'guide'
---

:::note
クエリ条件キャッシュは、デフォルト値である [enable&#95;analyzer](https://clickhouse.com/docs/operations/settings/settings#enable_analyzer) が true に設定されている場合にのみ動作します。
:::

実際のワークロードの多くでは、同じデータ、またはほぼ同じデータ (たとえば既存データに新しいデータが追加されたもの) に対してクエリが繰り返し実行されます。
ClickHouse は、このようなクエリパターンに対応するためのさまざまな最適化手法を提供しています。
1 つは、索引構造 (例: 主キー索引、スキッピング索引、プロジェクション) や事前計算 (materialized view) を用いて、物理的なデータレイアウトを調整する方法です。
もう 1 つは、ClickHouse の [クエリキャッシュ](query-cache.md) を使って、クエリの評価を繰り返さないようにする方法です。
最初の方法の欠点は、データベース管理者による手動の介入と監視が必要になることです。
2 つ目の方法は、古い結果を返す可能性があります (クエリキャッシュはトランザクション整合性を保証しないため) 。これを許容できるかどうかは、ユースケースによって異なります。

クエリ条件キャッシュは、この 2 つの問題に対する洗練された解決策を提供します。
これは、同じデータに対してフィルタ条件 (たとえば `WHERE col = 'xyz'`) を評価すれば、結果は常に同じになるという考え方に基づいています。
より具体的には、クエリ条件キャッシュは、評価済みの各フィルタと各グラニュール (= デフォルトでは 8192 行のブロック) について、そのグラニュール内にフィルタ条件を満たす行が 1 つも存在しないかどうかを記憶します。
この情報は 1 ビットで記録されます。0 ビットはフィルタに一致する行が存在しないことを表し、1 ビットは少なくとも 1 行は一致する行が存在することを意味します。
前者の場合、ClickHouse はフィルタ評価時に対応するグラニュールをスキップできます。後者の場合、そのグラニュールは読み込んで評価しなければなりません。

クエリ条件キャッシュが効果を発揮するのは、次の 3 つの前提条件が満たされている場合です。

* 1 つ目は、ワークロードで同じフィルタ条件が繰り返し評価されることです。これは同じクエリが複数回実行される場合には自然に発生しますが、2 つのクエリが同じフィルタを共有する場合にも起こりえます。たとえば、`SELECT product FROM products WHERE quality > 3` と `SELECT vendor, count() FROM products WHERE quality > 3` です。
* 2 つ目は、データの大部分が不変であること、つまりクエリ間で変化しないことです。ClickHouse では通常これが当てはまります。というのも、パーツは不変であり、INSERT によってのみ作成されるためです。
* 3 つ目は、フィルタの選択性が高いこと、つまりフィルタ条件を満たす行が比較的少ないことです。フィルタ条件に一致する行が少ないほど、0 ビット (一致する行なし) として記録されるグラニュールが増え、後続のフィルタ評価で「刈り込み」できるデータも多くなります。

<div id="memory-consumption">
  ## メモリ使用量
</div>

クエリ条件キャッシュは、フィルタ条件とグラニュールごとに 1 ビットしか格納しないため、消費するメモリはごくわずかです。
クエリ条件キャッシュの最大サイズは、サーバー設定 [`query_condition_cache_size`](server-configuration-parameters/settings.md#query_condition_cache_size) (デフォルト: 100 MB) で設定できます。
キャッシュサイズが 100 MB の場合、100 * 1024 * 1024 * 8 = 838,860,800 エントリに相当します。
各エントリは 1 つのマーク (デフォルトでは 8192 行) を表すため、このキャッシュは単一のカラムで最大 6,871,947,673,600 (6.8 兆) 行までカバーできます。
実際には、フィルタは複数のカラムに対して評価されるため、この数値はフィルタ対象のカラム数で割る必要があります。

<div id="configuration-settings-and-usage">
  ## 設定と使用
</div>

[use&#95;query&#95;condition&#95;cache](settings/settings#use_query_condition_cache) 設定では、特定のクエリ、または現在のセッション内のすべてのクエリで クエリ条件キャッシュ を使用するかどうかを制御します。

たとえば、クエリを初めて実行すると

```sql
SELECT col1, col2
FROM table
WHERE col1 = 'x'
SETTINGS use_query_condition_cache = true;
```

predicateを満たさないテーブルの範囲が保存されます。
その後、同じクエリを `use_query_condition_cache = true` パラメータ付きで実行すると、クエリ条件キャッシュを利用してスキャンするデータ量を減らせます。

<div id="administration">
  ## 管理
</div>

クエリ条件キャッシュは、ClickHouse を再起動しても保持されません。

クエリ条件キャッシュをクリアするには、[`SYSTEM CLEAR QUERY CONDITION CACHE`](../sql-reference/statements/system.md#drop-query-condition-cache) を実行します。

キャッシュの内容は、システムテーブル [system.query&#95;condition&#95;cache](system-tables/query_condition_cache.md) に表示されます。
クエリ条件キャッシュの現在のサイズを MB 単位で計算するには、`SELECT formatReadableSize(sum(entry_size)) FROM system.query_condition_cache` を実行します。
個々のフィルタ条件を調査したい場合は、`system.query_condition_cache` のフィールド `condition` を確認できます。なお、このフィールドはデバッグビルドでのみ使用できます。

データベースの起動以降のクエリ条件キャッシュのヒット数とミス数は、システムテーブル [system.events](system-tables/events.md) にイベント &quot;QueryConditionCacheHits&quot; および &quot;QueryConditionCacheMisses&quot; として表示されます。
これら両方のカウンターは、setting `use_query_condition_cache = true` を指定して実行された `SELECT` クエリでのみ更新され、その他のクエリは &quot;QueryCacheMisses&quot; に影響しません。

<div id="related-content">
  ## 関連コンテンツ
</div>

* ブログ: [Introducing the Query Condition Cache](https://clickhouse.com/blog/introducing-the-clickhouse-query-condition-cache)
* [Predicate Caching: Query-Driven Secondary Indexing for Cloud Data Warehouses (Schmidt et. al., 2024)](https://doi.org/10.1145/3626246.3653395)