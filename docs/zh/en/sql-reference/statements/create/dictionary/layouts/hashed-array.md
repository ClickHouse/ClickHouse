---
slug: /sql-reference/statements/create/dictionary/layouts/hashed-array
title: 'hashed_array 字典布局类型'
sidebar_label: 'hashed_array'
sidebar_position: 4
description: '使用带属性数组的哈希表将字典存储在内存中。'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="hashed_array">
  ## hashed_array
</div>

字典完全存储在内存中。每个属性都存储在数组里。键属性以哈希表的形式存储，其中值为属性数组中的索引。字典可包含任意数量、使用任意标识符的元素。在实际场景中，键的数量可达数千万个。

字典键的类型为 [UInt64](/zh/sql-reference/data-types/int-uint.md)。

支持所有类型的数据源。更新时，会完整读取全部数据 (来自文件或表) 。

配置示例：

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(HASHED_ARRAY([SHARDS 1]))
    ```
  </TabItem>

  <TabItem value="xml" label="配置文件">
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

这种存储类型适用于复合[键](../attributes.md#composite-key)。与 [hashed&#95;array](#hashed_array) 类似。

配置示例：

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(COMPLEX_KEY_HASHED_ARRAY([SHARDS 1]))
    ```
  </TabItem>

  <TabItem value="xml" label="配置文件">
    ```xml
    <layout>
      <complex_key_hashed_array />
    </layout>
    ```
  </TabItem>
</Tabs>

<br />