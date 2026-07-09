---
slug: /sql-reference/statements/create/dictionary/layouts/cache
title: '缓存字典布局'
sidebar_label: '缓存'
sidebar_position: 6
description: '将字典存储在固定大小的内存缓存中。'
doc_type: '参考'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

`cached` 字典布局类型会将字典存储在一个具有固定单元数的缓存中。
这些单元保存的是常用元素。

字典键的类型为 [UInt64](/zh/sql-reference/data-types/int-uint.md)。

查找字典时，会先查缓存。对于每个数据块，所有未在缓存中命中或已过期的键，都会通过 `SELECT attrs... FROM db.table WHERE id IN (k1, k2, ...)` 从源中请求。收到的数据随后会写入缓存。

如果在字典中未找到键，则会创建缓存更新任务并将其加入更新队列。更新队列的相关属性可通过设置 `max_update_queue_size`、`update_queue_push_timeout_milliseconds`、`query_wait_timeout_milliseconds`、`max_threads_for_updates` 控制。

对于缓存字典，可以设置缓存中数据的过期 [lifetime](../lifetime.md)。如果某个单元中的数据自加载以来经过的时间超过了 `lifetime`，则不会使用该单元中的值，并且该键会变为过期状态。该键会在下次需要使用时重新请求。此行为可通过设置 `allow_read_expired_keys` 进行配置。

这是所有字典存储方式中效率最低的一种。缓存的速度在很大程度上取决于设置是否正确以及具体使用场景。缓存类型字典只有在命中率足够高时性能才会较好 (建议达到 99% 及以上) 。你可以在 [system.dictionaries](/zh/operations/system-tables/dictionaries.md) 表中查看平均命中率。

如果将设置 `allow_read_expired_keys` 设为 1 (默认值为 0) ，则字典支持异步更新。如果客户端请求的键都在缓存中，但其中部分已过期，则字典会先向客户端返回这些过期键，并异步从源中重新请求它们。

要提升缓存性能，请使用带有 `LIMIT` 的子查询，并在字典外部调用该函数。

支持所有类型的源。

设置示例：

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(CACHE(SIZE_IN_CELLS 1000000000))
    ```
  </TabItem>

  <TabItem value="xml" label="配置文件">
    ```xml
    <layout>
        <cache>
            <!-- 缓存大小，以单元数计。向上取整为 2 的幂。 -->
            <size_in_cells>1000000000</size_in_cells>
            <!-- 允许读取过期键。 -->
            <allow_read_expired_keys>0</allow_read_expired_keys>
            <!-- 更新队列的最大大小。 -->
            <max_update_queue_size>100000</max_update_queue_size>
            <!-- 将更新任务推入队列的最大超时时间（毫秒）。 -->
            <update_queue_push_timeout_milliseconds>10</update_queue_push_timeout_milliseconds>
            <!-- 等待更新任务完成的最大超时时间（毫秒）。 -->
            <query_wait_timeout_milliseconds>60000</query_wait_timeout_milliseconds>
            <!-- 缓存字典更新使用的最大线程数。 -->
            <max_threads_for_updates>4</max_threads_for_updates>
        </cache>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

设置足够大的缓存。你需要通过试验来确定单元数量：

1. 设置一个值。
2. 运行查询，直到缓存被完全填满。
3. 使用 `system.dictionaries` 表评估内存消耗。
4. 增加或减少单元数量，直到达到所需的内存消耗。

:::note
不建议将 ClickHouse 用作此布局的源。字典查找需要随机点读，而这并不是 ClickHouse 优化的访问模式。
:::