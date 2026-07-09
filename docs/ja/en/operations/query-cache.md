---
description: 'ClickHouse のクエリキャッシュ機能の使用方法と設定に関するガイド'
sidebar_label: 'クエリキャッシュ'
sidebar_position: 65
slug: /operations/query-cache
title: 'クエリキャッシュ'
doc_type: 'guide'
---

クエリキャッシュを使用すると、`SELECT` クエリの計算は一度で済み、同じクエリの以降の実行では結果をキャッシュから直接返せます。
クエリの種類によっては、これにより ClickHouse server のレイテンシとリソース消費を大幅に削減できます。

<div id="background-design-and-limitations">
  ## 背景、設計、および制限事項
</div>

クエリキャッシュは、一般に、トランザクション整合性があるものとないものに分けられます。

* トランザクション整合性のあるキャッシュでは、`SELECT` クエリの結果が変化した場合、または変化する可能性がある場合に、データベースがキャッシュされたクエリ結果を無効化 (破棄) します。ClickHouse では、データを変更する操作として、table への insert/update/delete や、折りたたみ
  merge などがあります。トランザクション整合性のあるキャッシュは、たとえば
  [MySQL](https://dev.mysql.com/doc/refman/5.6/en/query-cache.html) (クエリキャッシュ は v8.0 以降で削除) や
  [Oracle](https://docs.oracle.com/database/121/TGDBA/tune_result_cache.htm) のような OLTP データベースに特に適しています。
* トランザクション整合性のないキャッシュでは、すべての cache entries に有効期間が設定され、
  その期間を過ぎると失効すること (例: 1 分) 、およびその期間中に基盤となるデータがほとんど変化しないことを前提として、
  クエリ結果に多少の不正確さが含まれることを許容します。このアプローチは、全体として OLAP データベースにより適しています。トランザクション整合性のないキャッシュで十分な例として、
  複数のユーザーが同時にアクセスするレポーティングツールの時間別売上レポートを考えてみてください。通常、売上データの変化は十分に緩やかなため、
  データベースはレポートを 1 回だけ計算すれば済みます (最初の `SELECT` クエリに相当) 。それ以降のクエリは、
  クエリキャッシュ から直接返せます。この例では、妥当な有効期間は 30 分です。

トランザクション整合性のないキャッシュは、従来、データベースとやり取りする client ツールや proxy package (例:
[chproxy](https://www.chproxy.org/configuration/caching/)) によって提供されてきました。その結果、同じキャッシュロジックや
configuration が重複して実装されることがよくあります。ClickHouse の クエリキャッシュ では、キャッシュロジックが server 側に移されます。これにより、保守
工数が削減され、重複も回避できます.

<div id="configuration-settings-and-usage">
  ## 設定と使用方法
</div>

:::note
ClickHouse Cloud では、クエリキャッシュの設定を変更するには [クエリレベル設定](/ja/operations/settings/query-level) を使用する必要があります。[config レベル設定](/ja/operations/configuration-files) の編集は現在サポートされていません。
:::

:::note
[clickhouse-local](utilities/clickhouse-local.md) は一度に 1 つのクエリしか実行できません。クエリ結果のキャッシュは意味をなさないため、clickhouse-local ではクエリ結果キャッシュは無効になっています。
:::

設定 [use&#95;query&#95;cache](/ja/operations/settings/settings#use_query_cache) を使用すると、特定のクエリ、または現在のセッション内のすべてのクエリでクエリキャッシュを利用するかどうかを制御できます。たとえば、クエリを最初に実行すると

```sql
SELECT some_expensive_calculation(column_1, column_2)
FROM table
SETTINGS use_query_cache = true;
```

クエリ結果は クエリキャッシュ に保存されます。同じクエリを後続で実行した場合 (パラメータ `use_query_cache = true` を指定した場合も含む) 、計算済みの結果を cache から読み取り、即座に返します。

:::note
`use_query_cache` およびそのほかの クエリキャッシュ 関連設定は、単独の `SELECT` ステートメントに対してのみ有効です。特に、
`CREATE VIEW AS SELECT [...] SETTINGS use_query_cache = true` で作成されたビューに対する `SELECT` の結果は、その `SELECT`
ステートメントが `SETTINGS use_query_cache = true` を指定して実行されない限り、cache されません。
:::

cache の利用方法は、設定 [enable&#95;writes&#95;to&#95;query&#95;cache](/ja/operations/settings/settings#enable_writes_to_query_cache)
および [enable&#95;reads&#95;from&#95;query&#95;cache](/ja/operations/settings/settings#enable_reads_from_query_cache) (どちらもデフォルトで `true`) を使って、さらに細かく構成できます。前者の設定は、
クエリ結果を cache に保存するかどうかを制御し、後者の設定は、database が cache からクエリ
結果を取得しようとするかどうかを決定します。たとえば、次のクエリは cache を受動的にのみ使用し、つまり cache からの読み取りは試みますが、その
結果は保存しません。

```sql
SELECT some_expensive_calculation(column_1, column_2)
FROM table
SETTINGS use_query_cache = true, enable_writes_to_query_cache = false;
```

最大限の制御を行うには、通常、設定 `use_query_cache`、`enable_writes_to_query_cache` および
`enable_reads_from_query_cache` は特定のクエリに対してのみ指定することが推奨されます。ユーザーレベルまたはプロファイルレベルでキャッシュを有効にすることも可能です (たとえば `SET
use_query_cache = true` を使用) が、その場合はすべての `SELECT` クエリがキャッシュされた結果を返す可能性がある点に注意してください。

クエリキャッシュは、ステートメント `SYSTEM CLEAR QUERY CACHE` を使用してクリアできます。クエリキャッシュの内容はシステムテーブル
[system.query&#95;cache](system-tables/query_cache.md) に表示されます。データベースの起動以降のクエリキャッシュのヒット数とミス数は、システムテーブル
[system.events](system-tables/events.md) のイベント
&quot;QueryCacheHits&quot; および &quot;QueryCacheMisses&quot; として表示されます。これらのカウンターは、設定
`use_query_cache = true` で実行された `SELECT` クエリについてのみ更新され、その他のクエリは &quot;QueryCacheMisses&quot; に影響しません。システムテーブル
[system.query&#95;log](system-tables/query_log.md) のフィールド `query_cache_usage` には、実行された各クエリについて、そのクエリ結果がクエリキャッシュに書き込まれたか、
クエリキャッシュから読み取られたかが示されます。システムテーブル
[system.metrics](system-tables/metrics.md) のメトリクス `QueryCacheEntries` と `QueryCacheBytes`
には、現在クエリキャッシュに含まれているエントリ数 / バイト数が表示されます。

クエリキャッシュは ClickHouse server process ごとに 1 つ存在します。ただし、キャッシュされた結果はデフォルトではユーザー間で共有されません。これは
変更できます (以下を参照) が、セキュリティ上の理由から推奨されません。

クエリ結果は、クエリキャッシュ内で、そのクエリの [Abstract Syntax Tree (AST)](https://en.wikipedia.org/wiki/Abstract_syntax_tree) によって
参照されます。これは、キャッシュが大文字/小文字を区別しないことを意味し、たとえば `SELECT 1` と `select 1` は同じクエリとして扱われます。より自然に一致させるために、クエリキャッシュおよび [出力フォーマット](settings/settings-formats.md)) に関連するすべてのクエリレベル設定は
AST から削除されます。

クエリが例外またはユーザーによるキャンセルによって中断された場合、クエリキャッシュにはエントリは書き込まれません。

クエリキャッシュのサイズ (バイト単位) 、cache entries の最大数、および個々の cache entries の最大サイズ (バイト単位および
レコード単位) は、さまざまな [server configuration options](/ja/operations/server-configuration-parameters/settings#query_cache) を使用して設定できます。

```xml
<query_cache>
    <max_size_in_bytes>1073741824</max_size_in_bytes>
    <max_entries>1024</max_entries>
    <max_entry_size_in_bytes>1048576</max_entry_size_in_bytes>
    <max_entry_size_in_rows>30000000</max_entry_size_in_rows>
</query_cache>
```

[settings profiles](settings/settings-profiles.md) と [settings
constraints](settings/constraints-on-settings.md) を使用すると、個々のユーザーの cache 使用量を制限することもできます。具体的には、ユーザーが
クエリキャッシュ に割り当てられるメモリの最大量 (バイト単位) と、保存できる query results の最大数を制限できます。そのためには、まず
`users.xml` のユーザープロファイルで [query&#95;cache&#95;max&#95;size&#95;in&#95;bytes](/ja/operations/settings/settings#query_cache_max_size_in_bytes) と
[query&#95;cache&#95;max&#95;entries](/ja/operations/settings/settings#query_cache_max_entries) を設定し、その後、両方の設定を
readonly にします:

```xml
<profiles>
    <default>
        <!-- The maximum cache size in bytes for user/profile 'default' -->
        <query_cache_max_size_in_bytes>10000</query_cache_max_size_in_bytes>
        <!-- The maximum number of SELECT query results stored in the cache for user/profile 'default' -->
        <query_cache_max_entries>100</query_cache_max_entries>
        <!-- Make both settings read-only so the user cannot change them -->
        <constraints>
            <query_cache_max_size_in_bytes>
                <readonly/>
            </query_cache_max_size_in_bytes>
            <query_cache_max_entries>
                <readonly/>
            <query_cache_max_entries>
        </constraints>
    </default>
</profiles>
```

結果をキャッシュ可能にするためにクエリが少なくともどれだけ実行されている必要があるかを指定するには、設定
[query&#95;cache&#95;min&#95;query&#95;duration](/ja/operations/settings/settings#query_cache_min_query_duration)を使用できます。たとえば、次のクエリの結果

```sql
SELECT some_expensive_calculation(column_1, column_2)
FROM table
SETTINGS use_query_cache = true, query_cache_min_query_duration = 5000;
```

クエリは、実行時間が 5 秒を超えた場合にのみキャッシュされます。また、結果が
キャッシュされるまでにクエリを何回実行する必要があるかを指定することもできます。その場合は設定 [query&#95;cache&#95;min&#95;query&#95;runs](/ja/operations/settings/settings#query_cache_min_query_runs) を使用します。

クエリキャッシュ内のエントリは、一定の時間 (有効期限 (TTL)) が経過すると古くなります。デフォルトでは、この期間は 60 秒ですが、設定 [query&#95;cache&#95;ttl](/ja/operations/settings/settings#query_cache_ttl) を使用して、セッション、プロファイル、またはクエリ
レベルで別の値を指定できます。クエリキャッシュはエントリを「遅延的に」削除します。つまり、エントリが古くなっても、すぐにはキャッシュから削除されません。代わりに、新しいエントリを
クエリキャッシュに挿入しようとすると、データベースはその新しいエントリのためにキャッシュに十分な空き容量があるかどうかを確認します。十分でない
場合、データベースは古くなったエントリをすべて削除しようとします。それでもキャッシュに十分な空き容量がない場合、新しいエントリは挿入されません。

クエリが HTTP 経由で実行された場合、ClickHouse は `Age` および `Expires` ヘッダーに、キャッシュされたエントリの経過時間 (秒単位) と有効期限のタイムスタンプを設定します。

クエリキャッシュ内のエントリは、デフォルトで圧縮されています。これにより、クエリキャッシュへの書き込み / クエリキャッシュからの読み取りは遅くなりますが、
全体的なメモリ消費量は削減されます。圧縮を無効にするには、設定 [query&#95;cache&#95;compress&#95;entries](/ja/operations/settings/settings#query_cache_compress_entries) を使用します。

同じクエリに対して複数の結果をキャッシュしておくと便利な場合があります。これは、クエリキャッシュのエントリに対するラベル (またはネームスペース) として機能する設定
[query&#95;cache&#95;tag](/ja/operations/settings/settings#query_cache_tag) を使用することで実現できます。クエリキャッシュは、
同じクエリであってもタグが異なれば別の結果として扱います。

同じクエリに対して 3 つの異なるクエリキャッシュエントリを作成する例:

```sql
SELECT 1 SETTINGS use_query_cache = true; -- query_cache_tag is implicitly '' (empty string)
SELECT 1 SETTINGS use_query_cache = true, query_cache_tag = 'tag 1';
SELECT 1 SETTINGS use_query_cache = true, query_cache_tag = 'tag 2';
```

クエリキャッシュからタグ `tag` の付いたエントリだけを削除するには、ステートメント `SYSTEM CLEAR QUERY CACHE TAG 'tag'` を使用します。

<div id="subquery-caching">
  ## サブクエリのキャッシュ
</div>

デフォルトでは、外側のクエリで設定した `use_query_cache` はサブクエリには引き継がれません。つまり、各サブクエリで明示的にキャッシュを有効にする必要があります。

```sql
SELECT *
FROM (SELECT number FROM system.numbers LIMIT 1000 SETTINGS use_query_cache = true)
WHERE number > 500;
```

この例では、キャッシュされるのは内側のサブクエリの結果のみです。外側のクエリはキャッシュされません。

すべてのサブクエリに対して一括でキャッシュを有効にするには、設定 `query_cache_for_subqueries` を使用します。

```sql
SELECT *
FROM (SELECT number FROM system.numbers LIMIT 1000)
WHERE number > 500
SETTINGS use_query_cache = true, query_cache_for_subqueries = true;
```

一括伝播が有効な場合に特定のサブクエリでキャッシュを明示的に無効にするには、そのサブクエリで `use_query_cache = false` を設定します。

```sql
SELECT *
FROM (SELECT number FROM system.numbers LIMIT 1000 SETTINGS use_query_cache = false)
WHERE number > 500
SETTINGS use_query_cache = true, query_cache_for_subqueries = true;
```

サブクエリのキャッシュエントリは、`is_subquery = 1` として [system.query&#95;cache](system-tables/query_cache.md) に表示されます。`query_cache_ttl` 設定はサブクエリのキャッシュエントリにも適用され、サブクエリごとに設定できます。

ClickHouse は、テーブルデータを [max&#95;block&#95;size](/ja/operations/settings/settings#max_block_size) 行のブロック単位で読み取ります。フィルタリングや集約などの影響により、
結果ブロックは通常 &#39;max&#95;block&#95;size&#39; よりかなり小さくなりますが、逆にそれよりかなり大きくなる場合もあります。設定
[query&#95;cache&#95;squash&#95;partial&#95;results](/ja/operations/settings/settings#query_cache_squash_partial_results) (デフォルトで有効) は、結果ブロックを
クエリ結果
キャッシュに挿入する前に、 (非常に小さい場合は) まとめるか、 (大きい場合は) &#39;max&#95;block&#95;size&#39; サイズのブロックに分割するかを制御します。これにより クエリキャッシュ への書き込み性能は低下しますが、キャッシュエントリの圧縮率が向上し、
後でクエリ結果が クエリキャッシュ から返される際に、より自然なブロック粒度が得られます。

その結果、クエリキャッシュ は各クエリについて複数の (部分的な)
結果ブロックを保存します。この動作は適切なデフォルトですが、設定
[query&#95;cache&#95;squash&#95;partial&#95;results](/ja/operations/settings/settings#query_cache_squash_partial_results) を使うことで無効にできます。

また、非決定論的関数を含むクエリの結果は、デフォルトではキャッシュされません。こうした関数には次のものが含まれます。

* ディクショナリにアクセスする関数: [`dictGet()`](/ja/sql-reference/functions/ext-dict-functions) など
* XML の
  定義で `<deterministic>true</deterministic>` タグを持たない [ユーザー定義関数](../sql-reference/statements/create/function.md)
* 現在の日付または時刻を返す関数: [`now()`](../sql-reference/functions/date-time-functions.md#now),
  [`today()`](../sql-reference/functions/date-time-functions.md#today),
  [`yesterday()`](../sql-reference/functions/date-time-functions.md#yesterday) など
* ランダムな値を返す関数: [`randomString()`](../sql-reference/functions/random-functions.md#randomString),
  [`fuzzBits()`](../sql-reference/functions/random-functions.md#fuzzBits) など
* クエリ処理に使用される内部的な chunk のサイズや順序に結果が依存する関数:
  [`nowInBlock()`](../sql-reference/functions/date-time-functions.md#nowInBlock) など、
  [`rowNumberInBlock()`](../sql-reference/functions/other-functions.md#rowNumberInBlock),
  [`runningDifference()`](../sql-reference/functions/other-functions.md#runningDifference),
  [`blockSize()`](../sql-reference/functions/other-functions.md#blockSize) など
* 環境に依存する関数: [`currentUser()`](../sql-reference/functions/other-functions.md#currentUser),
  [`queryID()`](/ja/sql-reference/functions/other-functions#queryID),
  [`getMacro()`](../sql-reference/functions/other-functions.md#getMacro) など

非決定論的関数を含むクエリの結果を、それでも強制的にキャッシュするには、設定
[query&#95;cache&#95;nondeterministic&#95;function&#95;handling](/ja/operations/settings/settings#query_cache_nondeterministic_function_handling) を使用します。

システムテーブルを含むクエリ (例: [system.processes](system-tables/processes.md)&#96; または
[information&#95;schema.tables](system-tables/information_schema.md)) の結果は、デフォルトではキャッシュされません。システムテーブルを含むクエリの結果を
それでも強制的にキャッシュするには、設定 [query&#95;cache&#95;system&#95;table&#95;handling](/ja/operations/settings/settings#query_cache_system_table_handling) を使用します。

最後に、セキュリティ上の理由から、クエリキャッシュ内のエントリはユーザー間で共有されません。たとえば、ユーザー A が、同じクエリを実行することで、
そのようなポリシーが存在しない別のユーザー B には適用されないテーブルの
行ポリシーを回避できてはなりません。ただし、必要に応じて、設定
[query&#95;cache&#95;share&#95;between&#95;users](/ja/operations/settings/settings#query_cache_share_between_users) を指定することで、キャッシュエントリを
ほかのユーザーからアクセス可能 (つまり共有) としてマークできます。

<div id="related-content">
  ## 関連コンテンツ
</div>

* ブログ: [ClickHouseのクエリキャッシュの紹介](https://clickhouse.com/blog/introduction-to-the-clickhouse-query-cache-and-design)