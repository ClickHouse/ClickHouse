---
slug: /sql-reference/statements/create/dictionary/layouts/hashed
title: 'hashed 字典布局类型'
sidebar_label: 'hashed'
sidebar_position: 3
description: '使用哈希表在内存中存储字典：hashed、sparse_hashed、complex_key_hashed、complex_key_sparse_hashed'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="hashed">
  ## hashed
</div>

该字典以哈希表的形式完整存储在内存中。字典可包含任意数量、使用任意标识符的元素。实际情况下，键的数量可达数千万个。

字典键的类型为 [UInt64](/zh/sql-reference/data-types/int-uint.md)。

支持所有类型的源。更新时，会完整读取全部数据 (来自文件或表) 。

配置示例：

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(HASHED())
    ```
  </TabItem>

  <TabItem value="xml" label="配置文件">
    ```xml
    <layout>
      <hashed />
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

带设置的配置示例：

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
    ```
  </TabItem>

  <TabItem value="xml" label="配置文件">
    ```xml
    <layout>
      <hashed>
        <!-- 如果分片数大于 1（默认值为 `1`），字典将并行加载
             数据；如果单个字典中的元素数量非常大，这会很有帮助。 -->
        <shards>10</shards>

        <!-- 并行队列中块的积压容量。

             由于并行加载时的瓶颈在于重新哈希，为了避免
             线程在执行重新哈希时造成停顿，需要保留一定的
             积压空间。

             10000 在内存占用与速度之间取得了较好的平衡。
             即使对于 10e10 个元素，也能处理全部负载而不会出现饥饿。 -->
        <shard_load_queue_backlog>10000</shard_load_queue_backlog>

        <!-- 哈希表的最大负载因子。值越大，内存
             利用率越高（浪费的内存更少），但读取性能
             可能会下降。

             有效值：[0.5, 0.99]
             默认值：0.5 -->
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

与 `hashed` 类似，但会以增加 CPU 使用率为代价来降低内存占用。

字典键的类型为 [UInt64](/zh/sql-reference/data-types/int-uint.md)。

配置示例：

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(SPARSE_HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
    ```
  </TabItem>

  <TabItem value="xml" label="配置文件">
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

这种类型的字典也可以使用 `shards`，而且相较于 `hashed`，这一点对 `sparse_hashed` 更为重要，因为 `sparse_hashed` 的速度更慢。

<div id="complex_key_hashed">
  ## complex_key_hashed
</div>

这种存储类型适用于复合[键](../attributes.md#composite-key)，与 `hashed` 类似。

配置示例：

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(COMPLEX_KEY_HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
    ```
  </TabItem>

  <TabItem value="xml" label="配置文件">
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

这种存储类型适用于复合[键](../attributes.md#composite-key)。与 [sparse&#95;hashed](#sparse_hashed) 类似。

配置示例：

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(COMPLEX_KEY_SPARSE_HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
    ```
  </TabItem>

  <TabItem value="xml" label="配置文件">
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