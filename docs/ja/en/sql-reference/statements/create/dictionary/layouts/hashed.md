---
slug: /sql-reference/statements/create/dictionary/layouts/hashed
title: 'hashed Dictionary レイアウトタイプ'
sidebar_label: 'hashed'
sidebar_position: 3
description: 'ハッシュテーブルを使用して Dictionary をメモリに格納するレイアウト: hashed, sparse_hashed, complex_key_hashed, complex_key_sparse_hashed'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="hashed">
  ## hashed
</div>

このDictionaryは、ハッシュテーブルの形式で完全にメモリ上に格納されます。Dictionaryには、任意の識別子を持つ要素をいくつでも含めることができます。実際には、キー数は数千万件に達することがあります。

Dictionaryキーは [UInt64](/ja/sql-reference/data-types/int-uint.md) 型です。

すべての種類のソースがサポートされています。更新時には、データ (ファイルまたはテーブルからのデータ) を全件読み込みます。

設定例:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(HASHED())
    ```
  </TabItem>

  <TabItem value="xml" label="設定ファイル">
    ```xml
    <layout>
      <hashed />
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

設定付きの構成例:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
    ```
  </TabItem>

  <TabItem value="xml" label="設定ファイル">
    ```xml
    <layout>
      <hashed>
        <!-- shards が 1 より大きい場合（デフォルトは `1`）、Dictionaryは
             データを並列に読み込みます。1 つの
             Dictionaryに非常に多くの要素がある場合に有効です。 -->
        <shards>10</shards>

        <!-- 並列キュー内のブロック用バックログサイズ。

             並列読み込みにおけるボトルネックは rehash であるため、
             rehash を実行しているスレッドによって
             処理が停滞しないよう、ある程度の
             バックログが必要です。

             10000 はメモリと速度のバランスが良好な値です。
             10e10 要素の場合でも、滞りなく負荷全体を処理できます。 -->
        <shard_load_queue_backlog>10000</shard_load_queue_backlog>

        <!-- ハッシュテーブルの最大負荷率です。値が大きいほど、メモリを
             より効率的に利用できます（メモリの無駄が減ります）が、読み取り性能が
             低下する可能性があります。

             有効な値: [0.5, 0.99]
             デフォルト: 0.5 -->
        <max_load_factor>0.5</max_load_factor>
      </hashed>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

<div id="sparse_hashed">
  ## sparse_hashed
</div>

`hashed` と似ていますが、CPU 使用量が増える代わりに、メモリ使用量を抑えられます。

Dictionaryキーの型は [UInt64](/ja/sql-reference/data-types/int-uint.md) です。

設定例:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(SPARSE_HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
    ```
  </TabItem>

  <TabItem value="xml" label="設定ファイル">
    ```xml
    <layout>
      <sparse_hashed>
        <!-- <shards>1</shards> -->
        <!-- <shard_load_queue_backlog>10000</shard_load_queue_backlog> -->
        <!-- <max_load_factor>0.5</max_load_factor> -->
      </sparse_hashed>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

この種類のDictionaryでも `shards` を使用できます。また、`sparse_hashed` は `hashed` より低速なため、`hashed` の場合以上に `sparse_hashed` ではその重要性が高くなります。

<div id="complex_key_hashed">
  ## complex_key_hashed
</div>

このストレージタイプは、複合[キー](../attributes.md#composite-key)で使用します。`hashed` に似ています。

設定例:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(COMPLEX_KEY_HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
    ```
  </TabItem>

  <TabItem value="xml" label="設定ファイル">
    ```xml
    <layout>
      <complex_key_hashed>
        <!-- <shards>1</shards> -->
        <!-- <shard_load_queue_backlog>10000</shard_load_queue_backlog> -->
        <!-- <max_load_factor>0.5</max_load_factor> -->
      </complex_key_hashed>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

<div id="complex_key_sparse_hashed">
  ## complex_key_sparse_hashed
</div>

このストレージタイプは、複合[キー](../attributes.md#composite-key)に使用します。[sparse&#95;hashed](#sparse_hashed)に似ています。

設定例:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(COMPLEX_KEY_SPARSE_HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
    ```
  </TabItem>

  <TabItem value="xml" label="設定ファイル">
    ```xml
    <layout>
      <complex_key_sparse_hashed>
        <!-- <shards>1</shards> -->
        <!-- <shard_load_queue_backlog>10000</shard_load_queue_backlog> -->
        <!-- <max_load_factor>0.5</max_load_factor> -->
      </complex_key_sparse_hashed>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />