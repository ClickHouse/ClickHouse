---
slug: /sql-reference/statements/create/dictionary/layouts/direct
title: 'direct Dictionaryレイアウト'
sidebar_label: 'direct'
sidebar_position: 9
description: 'キャッシュを使わず、ソースに対して直接クエリを実行するDictionaryレイアウト。'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="direct">
  ## direct
</div>

Dictionary はメモリに格納されず、リクエストの処理時にソースへ直接アクセスします。

辞書キーの型は [UInt64](/ja/sql-reference/data-types/int-uint.md) です。

ローカルファイルを除く、すべての種類の [SOURCES](../sources/#dictionary-sources) がサポートされています。

設定例:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(DIRECT())
    ```
  </TabItem>

  <TabItem value="xml" label="設定ファイル">
    ```xml
    <layout>
      <direct />
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

<div id="complex_key_direct">
  ## complex_key_direct
</div>

このストレージタイプは、複合[キー](../attributes.md#composite-key)で使用するためのものです。`direct` と似ています。