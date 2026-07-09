---
slug: /sql-reference/statements/create/dictionary/layouts/ssd-cache
title: 'ssd_cache Dictionary のレイアウトタイプ'
sidebar_label: 'ssd_cache'
sidebar_position: 8
description: 'インメモリ索引を使用して Dictionary データを SSD に保存: ssd_cache または complex_key_ssd_cache タイプ'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="ssd_cache">
  ## ssd_cache
</div>

`cache` に似ていますが、データは SSD に格納され、索引は RAM に保持されます。更新キューに関連する cache Dictionary の設定は、ssd&#95;cache Dictionary にもすべて適用できます。

Dictionaryキーの型は [UInt64](/ja/sql-reference/data-types/int-uint.md) です。

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 16777216 READ_BUFFER_SIZE 1048576
        PATH '/var/lib/clickhouse/user_files/test_dict'))
    ```
  </TabItem>

  <TabItem value="xml" label="設定ファイル">
    ```xml
    <layout>
        <ssd_cache>
            <!-- バイト単位の基本読み取りブロックのサイズ。SSD のページサイズと同じにすることを推奨します。 -->
            <block_size>4096</block_size>
            <!-- バイト単位の cache ファイルの最大サイズ。 -->
            <file_size>16777216</file_size>
            <!-- SSD から要素を読み取るための、バイト単位の RAM バッファサイズ。 -->
            <read_buffer_size>131072</read_buffer_size>
            <!-- SSD にフラッシュする前に要素を集約するための、バイト単位の RAM バッファサイズ。 -->
            <write_buffer_size>1048576</write_buffer_size>
            <!-- cache ファイルを保存するパス。 -->
            <path>/var/lib/clickhouse/user_files/test_dict</path>
        </ssd_cache>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

<div id="complex_key_ssd_cache">
  ## complex_key_ssd_cache
</div>

このストレージタイプは、複合[キー](../attributes.md#composite-key)で使用するためのものです。`ssd_cache` と似ています。