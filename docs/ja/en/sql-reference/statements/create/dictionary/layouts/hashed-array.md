---
slug: /sql-reference/statements/create/dictionary/layouts/hashed-array
title: 'hashed_array Dictionaryレイアウトタイプ'
sidebar_label: 'hashed_array'
sidebar_position: 4
description: '属性配列を持つハッシュテーブルを使用して、Dictionaryをメモリ上に格納します。'
doc_type: 'リファレンス'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="hashed_array">
  ## hashed_array
</div>

Dictionary は完全にメモリ内に格納されます。各属性は配列に格納されます。キー属性は、値として属性配列内の索引を持つハッシュテーブルの形式で格納されます。Dictionary には、任意の識別子を持つ要素を任意の数だけ含めることができます。実際には、キーの数は数千万件に達することがあります。

辞書キーの型は [UInt64](/ja/sql-reference/data-types/int-uint.md) です。

あらゆる種類のソースがサポートされています。更新時には、データ (ファイルまたはテーブルから) が全件読み込まれます。

設定例:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(HASHED_ARRAY([SHARDS 1]))
    ```
  </TabItem>

  <TabItem value="xml" label="設定ファイル">
    ```xml
    <layout>
      <hashed_array>
      </hashed_array>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

<div id="complex_key_hashed_array">
  ## complex_key_hashed_array
</div>

このストレージタイプは、複合[キー](../attributes.md#composite-key)で使用します。[hashed&#95;array](#hashed_array) に似ています。

設定例:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(COMPLEX_KEY_HASHED_ARRAY([SHARDS 1]))
    ```
  </TabItem>

  <TabItem value="xml" label="設定ファイル">
    ```xml
    <layout>
      <complex_key_hashed_array />
    </layout>
    ```
  </TabItem>
</Tabs>

<br />