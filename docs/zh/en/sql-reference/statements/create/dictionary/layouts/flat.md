---
slug: /sql-reference/statements/create/dictionary/layouts/flat
title: 'flat 字典布局'
sidebar_label: 'flat'
sidebar_position: 2
description: '以扁平数组形式将字典存储在内存中。'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

使用 `flat` 布局时，字典会以平面数组的形式完整存储在内存中。
内存用量与最大键值的大小 (按占用空间计) 成正比。

:::tip
在所有可用的字典存储方法中，这种布局类型的性能最佳。
:::

字典键的类型为 [UInt64](/zh/sql-reference/data-types/int-uint.md)，键值不能超过 `max_array_size` (默认值为 500,000) 。
如果在创建字典时发现更大的键值，ClickHouse 会抛出异常，并且不会创建该字典。
字典平面数组的初始大小由 `initial_array_size` 设置控制 (默认值为 1024) 。

支持所有类型的源。
更新字典时，会完整读取数据 (来自文件或表) 。

配置示例：

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(FLAT(INITIAL_ARRAY_SIZE 50000 MAX_ARRAY_SIZE 5000000))
    ```
  </TabItem>

  <TabItem value="xml" label="配置文件">
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