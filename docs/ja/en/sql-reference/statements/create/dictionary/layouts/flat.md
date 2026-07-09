---
slug: /sql-reference/statements/create/dictionary/layouts/flat
title: 'flat Dictionary のレイアウト'
sidebar_label: 'flat'
sidebar_position: 2
description: 'Dictionary をメモリ上にフラットな Array として格納します。'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

`flat` レイアウトでは、辞書全体がフラットな配列としてメモリ上に格納されます。
使用されるメモリ量は、最大のキーの大きさ (占有領域) に比例します。

:::tip
このレイアウトタイプは、利用可能なすべての辞書保存方法の中で最も高いパフォーマンスを発揮します。
:::

辞書キーの型は [UInt64](/ja/sql-reference/data-types/int-uint.md) で、値は `max_array_size` に制限されます (デフォルトは 500,000) 。
辞書の作成時にこれより大きなキーが見つかった場合、ClickHouse は例外をスローし、辞書は作成されません。
辞書のフラット配列の初期サイズは、`initial_array_size` 設定で制御します (デフォルトは 1024) 。

すべての種類のソースがサポートされています。
辞書の更新時には、データ (ファイルまたはテーブルから) が全件読み込まれます。

設定例:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(FLAT(INITIAL_ARRAY_SIZE 50000 MAX_ARRAY_SIZE 5000000))
    ```
  </TabItem>

  <TabItem value="xml" label="設定ファイル">
    ```xml
    <layout>
      <flat>
        <initial_array_size>50000</initial_array_size>
        <max_array_size>5000000</max_array_size>
      </flat>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />