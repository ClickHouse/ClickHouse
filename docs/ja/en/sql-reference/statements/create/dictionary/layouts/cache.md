---
slug: /sql-reference/statements/create/dictionary/layouts/cache
title: 'cache Dictionary のレイアウト'
sidebar_label: 'cache'
sidebar_position: 6
description: 'Dictionary を固定サイズの in-memory cache に格納します。'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

`cached` 辞書レイアウト型は、固定数のセルを持つキャッシュに辞書を格納します。
これらのセルには、頻繁に使用される要素が含まれます。

辞書キーの型は [UInt64](/ja/sql-reference/data-types/int-uint.md) です。

辞書を検索する際は、まずキャッシュが検索されます。データの各ブロックについて、キャッシュ内に見つからないキー、または期限切れのキーはすべて、`SELECT attrs... FROM db.table WHERE id IN (k1, k2, ...)` を使用してソースにリクエストされます。取得したデータはその後キャッシュに書き込まれます。

辞書内にキーが見つからない場合は、キャッシュ更新タスクが作成され、更新キューに追加されます。更新キューのプロパティは、設定 `max_update_queue_size`、`update_queue_push_timeout_milliseconds`、`query_wait_timeout_milliseconds`、`max_threads_for_updates` で制御できます。

キャッシュ辞書では、キャッシュ内データの有効期限 [lifetime](../lifetime.md) を設定できます。セル内のデータを読み込んでから `lifetime` を超える時間が経過すると、そのセルの値は使用されず、キーは期限切れになります。そのキーは、次に必要になったときに再度リクエストされます。この動作は、設定 `allow_read_expired_keys` で構成できます。

これは、辞書を格納するすべての方法の中で最も効率が低いものです。キャッシュの速度は、適切な設定と使用シナリオに大きく依存します。キャッシュ型の辞書が良好に機能するのは、ヒット率が十分に高い場合のみです (推奨は 99% 以上) 。平均ヒット率は [system.dictionaries](/ja/operations/system-tables/dictionaries.md) テーブルで確認できます。

設定 `allow_read_expired_keys` を 1 にすると (デフォルトは 0) 、辞書は非同期更新をサポートできます。クライアントがキーをリクエストし、それらがすべてキャッシュ内にあっても、一部が期限切れであれば、辞書は期限切れのキーをクライアントに返し、それらをソースに非同期でリクエストします。

キャッシュのパフォーマンスを向上させるには、`LIMIT` を含むサブクエリを使用し、辞書の外側で関数を呼び出します。

すべての種類のソースがサポートされています。

設定の例:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(CACHE(SIZE_IN_CELLS 1000000000))
    ```
  </TabItem>

  <TabItem value="xml" label="設定ファイル">
    ```xml
    <layout>
        <cache>
            <!-- キャッシュのサイズ。セル数で指定します。2 の累乗に切り上げられます。 -->
            <size_in_cells>1000000000</size_in_cells>
            <!-- 期限切れキーの読み取りを許可します。 -->
            <allow_read_expired_keys>0</allow_read_expired_keys>
            <!-- 更新キューの最大サイズ。 -->
            <max_update_queue_size>100000</max_update_queue_size>
            <!-- 更新タスクをキューに追加する際の最大タイムアウト（ミリ秒）。 -->
            <update_queue_push_timeout_milliseconds>10</update_queue_push_timeout_milliseconds>
            <!-- 更新タスクの完了を待機する最大タイムアウト（ミリ秒）。 -->
            <query_wait_timeout_milliseconds>60000</query_wait_timeout_milliseconds>
            <!-- キャッシュ辞書更新用の最大スレッド数。 -->
            <max_threads_for_updates>4</max_threads_for_updates>
        </cache>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

十分に大きなキャッシュサイズを設定してください。セル数を選ぶには試行が必要です:

1. 何らかの値を設定します。
2. キャッシュが完全にいっぱいになるまでクエリを実行します。
3. `system.dictionaries` テーブルを使用してメモリ消費量を評価します。
4. 必要なメモリ消費量に達するまでセル数を増減します。

:::note
このレイアウトのソースとして ClickHouse を使用することは推奨されません。Dictionary のルックアップにはランダムなポイントリードが必要ですが、これは ClickHouse が最適化しているアクセスパターンではありません。
:::