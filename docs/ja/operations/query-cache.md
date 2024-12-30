---
slug: /ja/operations/query-cache
sidebar_position: 65
sidebar_label: クエリキャッシュ
---

# クエリキャッシュ

クエリキャッシュは、`SELECT` クエリを一度計算し、同じクエリの再実行をキャッシュから直接提供できるようにします。
クエリの種類によっては、ClickHouseサーバーのレイテンシーとリソース消費を劇的に削減できます。

## 背景、設計、および制限

クエリキャッシュは、一般的にトランザクション的に整合性があるか、一貫性がないものと見なされます。

- トランザクション的に一貫性のあるキャッシュでは、`SELECT` クエリの結果が変化した場合、または変化する可能性がある場合、データベースはキャッシュされたクエリ結果を無効にします（破棄します）。
  ClickHouseにおけるデータを変更する操作は、テーブルへの挿入/更新/削除またはコラプシングマージなどです。トランザクション的に整合性のあるキャッシュは、特にOLTPデータベースに適しており、
  例えば [MySQL](https://dev.mysql.com/doc/refman/5.6/en/query-cache.html)（v8.0以降クエリキャッシュを削除）や
  [Oracle](https://docs.oracle.com/database/121/TGDBA/tune_result_cache.htm)などがあります。
- トランザクション的に整合性のないキャッシュでは、クエリ結果のわずかな不正確さが許容され、キャッシュエントリが一定の有効期間（例：1分）を持ち、その期間中に基礎となるデータがほとんど変化しないという仮定の下でキャッシュが機能します。
  このアプローチは全体としてOLAPデータベースにより適していると言えます。トランザクション的に整合性のないキャッシュが十分な例として、報告ツールで複数のユーザーが同時にアクセスする毎時売上報告を考えてみましょう。
  売上データは通常ゆっくりと変化するため、データベースはレポートを一度計算するだけで済みます（最初の`SELECT`クエリで表されます）。その後のクエリはクエリキャッシュから直接提供されます。
  この例では、妥当な有効期間は30分になるかもしれません。

トランザクション的に整合性のないキャッシングは、通常、クライアントツールやデータベースと対話するプロキシパッケージ（例:
[chproxy](https://www.chproxy.org/configuration/caching/)）によって提供されます。その結果、同じキャッシングロジックと構成がしばしば重複されます。
ClickHouseのクエリキャッシュでは、キャッシングロジックがサーバー側に移動します。これにより、メンテナンスの手間が減り、冗長性が回避されます。

## 設定と使用法

:::note
ClickHouse Cloudでは、クエリキャッシュ設定を編集するために[クエリレベル設定](/ja/operations/settings/query-level)を使用する必要があります。[コンフィグレベル設定](/ja/operations/configuration-files)の編集は現在サポートされていません。
:::

[use_query_cache](settings/settings.md#use-query-cache) 設定を使用して、特定のクエリまたは現在のセッションのすべてのクエリがクエリキャッシュを利用するかどうかを制御できます。たとえば、以下のクエリの最初の実行は、

```sql
SELECT some_expensive_calculation(column_1, column_2)
FROM table
SETTINGS use_query_cache = true;
```

クエリの結果をクエリキャッシュに保存します。同じクエリの後続の実行（パラメータ`use_query_cache = true`も指定したもの）は、計算済みの結果をキャッシュから読み込み、即座に返します。

:::note
`use_query_cache` 設定と他のクエリキャッシュ関連の設定は、単独の`SELECT`文に対してのみ効果があります。特に、`CREATE VIEW AS SELECT [...] SETTINGS use_query_cache = true`によって作成されたビューへの`SELECT`の結果は、その`SELECT`文が`SETTINGS use_query_cache = true`で実行されない限りキャッシュされません。
:::

キャッシュの利用方法は、[enable_writes_to_query_cache](settings/settings.md#enable-writes-to-query-cache)と[enable_reads_from_query_cache](settings/settings.md#enable-reads-from-query-cache)（どちらもデフォルトで`true`）の設定を使用して詳細に構成できます。
前者の設定はクエリ結果がキャッシュに保存されるかどうかを制御し、後者の設定はデータベースがキャッシュからクエリ結果を取得しようとするかどうかを決定します。たとえば、次のクエリはキャッシュをパッシブにのみ使用し、キャッシュから読み込むことを試みますが、その結果をキャッシュに保存しません:

```sql
SELECT some_expensive_calculation(column_1, column_2)
FROM table
SETTINGS use_query_cache = true, enable_writes_to_query_cache = false;
```

最大限の制御を行うために、特定のクエリに対してのみ`use_query_cache`, `enable_writes_to_query_cache`, `enable_reads_from_query_cache`の設定を行うことが一般的に推奨されます。
また、ユーザーやプロファイルレベルでキャッシュを有効にすることもできます（例：`SET use_query_cache = true`）。ただし、その場合、すべての`SELECT`クエリがキャッシュされた結果を返すことがあることに注意してください。

クエリキャッシュは、文`SYSTEM DROP QUERY CACHE`を使用してクリアできます。クエリキャッシュの内容はシステムテーブル[system.query_cache](system-tables/query_cache.md)に表示されます。
データベース開始以降のクエリキャッシュヒットとミスの数は、システムテーブル[system.events](system-tables/events.md)のイベント"QueryCacheHits"および"QueryCacheMisses"として示されています。両方のカウンターは、`use_query_cache = true`設定で実行された`SELECT`クエリに対してのみ更新され、それ以外のクエリは"QueryCacheMisses"に影響を与えません。
システムテーブル[system.query_log](system-tables/query_log.md)のフィールド`query_cache_usage`は、実行されたクエリごとに、そのクエリ結果がクエリキャッシュに書き込まれたか、もしくはキャッシュから読み取られたかを示しています。
システムテーブル[system.asynchronous_metrics](system-tables/asynchronous_metrics.md)の非同期メトリック"QueryCacheEntries"および"QueryCacheBytes"は、クエリキャッシュが現在含んでいるエントリ/バイト数を示しています。

クエリキャッシュは、ClickHouseサーバープロセスごとに1つ存在します。ただし、デフォルトではキャッシュ結果はユーザー間で共有されません。セキュリティ上の理由から、これを変更することもできますが（下記参照）、推奨されません。

クエリキャッシュ内のクエリ結果は、クエリの[抽象構文木 (AST)](https://en.wikipedia.org/wiki/Abstract_syntax_tree)によって参照されます。これは、キャッシングが大文字/小文字に依存しないことを意味し、例えば`SELECT 1`と`select 1`が同じクエリとして扱われます。
キャッシュのマッチングをより自然にするために、クエリキャッシュに関連するすべてのクエリレベル設定はASTから削除されます。

クエリが例外やユーザーのキャンセルにより中止された場合、エントリはクエリキャッシュに書き込まれません。

クエリキャッシュのサイズ（バイト単位）、最大キャッシュエントリ数、および個々のキャッシュエントリの最大サイズ（バイトおよびレコード数）は、異なる[サーバーコンフィグレーションオプション](server-configuration-parameters/settings.md#server_configuration_parameters_query-cache)を使用して構成できます。

```xml
<query_cache>
    <max_size_in_bytes>1073741824</max_size_in_bytes>
    <max_entries>1024</max_entries>
    <max_entry_size_in_bytes>1048576</max_entry_size_in_bytes>
    <max_entry_size_in_rows>30000000</max_entry_size_in_rows>
</query_cache>
```

また、[設定プロファイル](settings/settings-profiles.md)および[設定制約](settings/constraints-on-settings.md)を使用して、個々のユーザーのキャッシュ使用量を制限することもできます。
より具体的には、ユーザーがクエリキャッシュで割り当てることができるメモリの最大量（バイト単位）と、ストアされるクエリ結果の最大数を制限できます。
そのためには、まずユーザープロファイル内の`users.xml`で[query_cache_max_size_in_bytes](settings/settings.md#query-cache-max-size-in-bytes)と[query_cache_max_entries](settings/settings.md#query-cache-max-entries)の設定を行い、次に両方の設定を読み取り専用にします：

``` xml
<profiles>
    <default>
        <!-- ユーザー/プロファイル 'default' の最大キャッシュサイズ（バイト単位） -->
        <query_cache_max_size_in_bytes>10000</query_cache_max_size_in_bytes>
        <!-- ユーザー/プロファイル 'default' のキャッシュに保存されるSELECTクエリ結果の最大数 -->
        <query_cache_max_entries>100</query_cache_max_entries>
        <!-- 両方の設定を読み取り専用にしてユーザーがそれを変更できないように設定 -->
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

クエリの結果がキャッシュされるには一定時間以上の実行が必要な場合、設定[query_cache_min_query_duration](settings/settings.md#query-cache-min-query-duration)を使用できます。
たとえば、以下のクエリの結果は、

``` sql
SELECT some_expensive_calculation(column_1, column_2)
FROM table
SETTINGS use_query_cache = true, query_cache_min_query_duration = 5000;
```

クエリが5秒以上実行される場合にのみキャッシュされます。また、クエリの結果がキャッシュされるまでに何回実行される必要があるかを指定することも可能です。それには設定[query_cache_min_query_runs](settings/settings.md#query-cache-min-query-runs)を使用します。

クエリキャッシュ内のエントリは、一定期間（有効期限 (TTL)）が経過すると古くなります。デフォルトでは、この期間は60秒ですが、設定[query_cache_ttl](settings/settings.md#query-cache-ttl)を使用して、セッション、プロファイル、またはクエリレベルで異なる値を指定できます。
クエリキャッシュは「遅延」エントリ追放を行います。つまり、エントリが古くなった場合、すぐにキャッシュから削除されるわけではありません。
代わりに、新しいエントリをクエリキャッシュに挿入する必要がある場合、データベースはキャッシュが新しいエントリのために十分な空きスペースを持っているかどうかを確認します。
これが行われない場合、データベースはすべての古いエントリを削除しようとします。それでもキャッシュに十分な空きスペースがない場合、新しいエントリは挿入されません。

クエリキャッシュ内のエントリはデフォルトで圧縮されます。これにより、クエリキャッシュへの読み書きが遅くなりますが、全体のメモリ消費量が削減されます。
圧縮を無効化するには、設定[query_cache_compress_entries](settings/settings.md#query-cache-compress-entries)を使用します。

時には、同じクエリに対する複数の結果をキャッシュしておくと便利です。これを実現するために、クエリキャッシュエントリのタグ（またはネームスペース）として機能する設定[query_cache_tag](settings/settings.md#query-cache-tag)が使用できます。
クエリキャッシュは、異なるタグを持つ同じクエリの結果を異なるものとして扱います。

同じクエリに対して3つの異なるクエリキャッシュエントリを作成する例：

```sql
SELECT 1 SETTINGS use_query_cache = true; -- query_cache_tagは暗黙的に''（空文字列）
SELECT 1 SETTINGS use_query_cache = true, query_cache_tag = 'tag 1';
SELECT 1 SETTINGS use_query_cache = true, query_cache_tag = 'tag 2';
```

クエリキャッシュからタグ`tag`のエントリのみを削除するには、文`SYSTEM DROP QUERY CACHE TAG 'tag'`を使用します。

ClickHouseは、[max_block_size](settings/settings.md#setting-max_block_size)行のブロックでテーブルデータを読み込みます。
フィルタリングや集約などにより、結果ブロックは通常'max_block_size'よりもはるかに小さくなりますが、場合によっては非常に大きくなることもあります。
設定[query_cache_squash_partial_results](settings/settings.md#query-cache-squash-partial-results)（デフォルトで有効）は、結果ブロックが（極小の場合）'max_block_size'のサイズにスクワッシュされるか、（大きい場合）にブロックがスプリットされるかを制御します。
これにより、クエリキャッシュへの書き込みのパフォーマンスは低下しますが、キャッシュエントリの圧縮率は改善され、後でクエリキャッシュから結果が提供される際に、より自然なブロック粒度が得られます。

結果として、クエリキャッシュはクエリごとに複数の（部分的な）結果ブロックを保存します。この振る舞いは良いデフォルトですが、設定[query_cache_squash_partial_results](settings/settings.md#query-cache-squash-partial-results)を使用して抑制することも可能です。

また、非決定的な関数を含むクエリの結果はデフォルトでキャッシュされません。これには、
- Dictionaryアクセス用関数: [`dictGet()`](../sql-reference/functions/ext-dict-functions.md#dictGet) など、
- [ユーザー定義関数](../sql-reference/statements/create/function.md)、
- 現在の日付や時刻を返す関数: [`now()`](../sql-reference/functions/date-time-functions.md#now),
  [`today()`](../sql-reference/functions/date-time-functions.md#today),
  [`yesterday()`](../sql-reference/functions/date-time-functions.md#yesterday) など、
- ランダムな値を返す関数: [`randomString()`](../sql-reference/functions/random-functions.md#randomString),
  [`fuzzBits()`](../sql-reference/functions/random-functions.md#fuzzBits) など、
- クエリ処理に使用される内部チャンクのサイズと順序に依存する関数:  
  [`nowInBlock()`](../sql-reference/functions/date-time-functions.md#nowInBlock) など、
  [`rowNumberInBlock()`](../sql-reference/functions/other-functions.md#rowNumberInBlock),
  [`runningDifference()`](../sql-reference/functions/other-functions.md#runningDifference),
  [`blockSize()`](../sql-reference/functions/other-functions.md#blockSize) など、
- 環境に依存する関数: [`currentUser()`](../sql-reference/functions/other-functions.md#currentUser),
  [`queryID()`](../sql-reference/functions/other-functions.md#queryID),
  [`getMacro()`](../sql-reference/functions/other-functions.md#getMacro) などが含まれます。

非決定的な関数を含むクエリの結果を強制的にキャッシュする場合は、設定[query_cache_nondeterministic_function_handling](settings/settings.md#query-cache-nondeterministic-function-handling)を使用します。

システムテーブル（例：[system.processes](system-tables/processes.md)または[information_schema.tables](system-tables/information_schema.md)）を含むクエリの結果はデフォルトでキャッシュされません。
システムテーブルを含むクエリの結果を強制的にキャッシュするには、設定[query_cache_system_table_handling](settings/settings.md#query-cache-system-table-handling)を使用します。

最後に、セキュリティ上の理由からクエリキャッシュ内のエントリはユーザー間で共有されません。たとえば、ユーザーAは、ユーザーB（そのようなポリシーが存在しないユーザー）のために同じクエリを実行することにより、テーブル上の行ポリシーを回避することができません。しかし、必要に応じて、キャッシュエントリは他のユーザーによってアクセス可能（すなわち共有可能）としてマークすることができますが、設定
[query_cache_share_between_users](settings/settings.md#query-cache-share-between-users)を供給することにより実現できます。

## 関連コンテンツ

- ブログ: [Introducing the ClickHouse Query Cache](https://clickhouse.com/blog/introduction-to-the-clickhouse-query-cache-and-design)
