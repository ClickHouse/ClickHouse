---
slug: /sql-reference/statements/create/dictionary/layouts/direct
title: 'direct 字典布局'
sidebar_label: 'direct'
sidebar_position: 9
description: '一种不使用缓存、直接查询源的字典布局。'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="direct">
  ## direct
</div>

字典不会存储在内存中，而是在处理请求时直接访问数据源。

字典键的类型为 [UInt64](/zh/sql-reference/data-types/int-uint.md)。

支持除本地文件外的所有[数据源](../sources/#dictionary-sources)类型。

配置示例：

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(DIRECT())
    ```
  </TabItem>

  <TabItem value="xml" label="配置文件">
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

此类存储适用于复合[键](../attributes.md#composite-key)。与 `direct` 类似。