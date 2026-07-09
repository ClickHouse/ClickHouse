---
description: '在内存中存储字典的布局类型'
sidebar_label: '概览'
sidebar_position: 1
slug: /sql-reference/statements/create/dictionary/layouts
title: '字典布局类型'
doc_type: 'reference'
---

import CloudDetails from '@site/docs/sql-reference/statements/create/dictionary/_snippet_dictionary_in_cloud.md';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="storing-dictionaries-in-memory">
  ## 字典布局类型
</div>

字典有多种内存存储方式，每种方式都需要在 CPU 和 RAM 使用量之间进行权衡。

| 布局                                                                                                         | 描述                                                              |
| ---------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------- |
| [flat](./flat.md)                                                                                          | 将数据存储在按键索引的扁平数组中。这是最快的布局，但键必须是 `UInt64`，且受 `max_array_size` 限制。 |
| [hashed](./hashed.md)                                                                                      | 将数据存储在哈希表中。键大小没有限制，支持任意数量的元素。                                   |
| [sparse&#95;hashed](./hashed.md#sparse_hashed)                                                             | 类似 `hashed`，但以更高的 CPU 开销换取更低的内存使用量。                             |
| [complex&#95;key&#95;hashed](./hashed.md#complex_key_hashed)                                               | 类似 `hashed`，用于复合键。                                              |
| [complex&#95;key&#95;sparse&#95;hashed](./hashed.md#complex_key_sparse_hashed)                             | 类似 `sparse_hashed`，用于复合键。                                       |
| [hashed&#95;array](./hashed-array.md)                                                                      | 属性存储在数组中，并使用哈希表将键映射到数组索引。适合属性较多的场景，内存效率较高。                      |
| [complex&#95;key&#95;hashed&#95;array](./hashed-array.md#complex_key_hashed_array)                         | 类似 `hashed_array`，用于复合键。                                        |
| [range&#95;hashed](./range-hashed.md)                                                                      | 带有有序范围的哈希表。支持按键 + 日期/时间范围进行查找。                                  |
| [complex&#95;key&#95;range&#95;hashed](./range-hashed.md#complex_key_range_hashed)                         | 类似 `range_hashed`，用于复合键。                                        |
| [cache](./cache.md)                                                                                        | 固定大小的内存缓存。仅存储经常访问的键。                                            |
| [complex&#95;key&#95;cache](/zh/sql-reference/statements/create/dictionary/layouts/hashed#complex_key_hashed) | 类似 `cache`，用于复合键。                                               |
| [ssd&#95;cache](./ssd-cache.md)                                                                            | 类似 `cache`，但将数据存储在 SSD 上，并使用内存索引。                               |
| [complex&#95;key&#95;ssd&#95;cache](./ssd-cache.md#complex_key_ssd_cache)                                  | 类似 `ssd_cache`，用于复合键。                                           |
| [direct](./direct.md)                                                                                      | 不使用内存存储——每次请求都直接查询数据源。                                          |
| [complex&#95;key&#95;direct](./direct.md#complex_key_direct)                                               | 类似 `direct`，用于复合键。                                              |
| [ip&#95;trie](./ip-trie.md)                                                                                | 用于快速 IP 前缀查找 (基于 CIDR) 的 Trie 结构。                               |

:::tip 推荐布局
[flat](./flat.md)、[hashed](./hashed.md) 和 [complex&#95;key&#95;hashed](./hashed.md#complex_key_hashed) 可提供最佳查询性能。
缓存类布局不推荐使用，因为其性能可能较差且参数调优困难——详见 [cache](./cache.md)。
:::

<div id="specify-dictionary-layout">
  ## 指定字典布局
</div>

<CloudDetails />

你可以使用 `LAYOUT` 子句 (用于 DDL) ，或在配置文件定义中使用 `layout` 设置来配置字典布局。

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY (...)
    ...
    LAYOUT(LAYOUT_TYPE(param value)) -- 布局设置
    ...
    ```
  </TabItem>

  <TabItem value="xml" label="配置文件">
    ```xml
    <clickhouse>
        <dictionary>
            ...
            <layout>
                <layout_type>
                    <!-- 布局设置 -->
                </layout_type>
            </layout>
            ...
        </dictionary>
    </clickhouse>
    ```
  </TabItem>
</Tabs>

<br />

另请参阅 [CREATE DICTIONARY](../overview.md) 了解完整的 DDL 语法。

布局名称中不包含 `complex-key*` 的字典使用 [UInt64](/zh/sql-reference/data-types/int-uint.md) 类型的键；`complex-key*` 字典则使用复合键 (复杂键，可包含任意类型) 。

**数值键示例** (列 key&#95;column 的类型为 [UInt64](/zh/sql-reference/data-types/int-uint.md)) ：

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY dict_name (
        key_column UInt64,
        ...
    )
    PRIMARY KEY key_column
    ```
  </TabItem>

  <TabItem value="xml" label="配置文件">
    ```xml
    <structure>
        <id>
            <name>key_column</name>
        </id>
        ...
    </structure>
    ```
  </TabItem>
</Tabs>

<br />

**复合键示例** (键包含一个 [String](/zh/sql-reference/data-types/string.md) 类型的元素) ：

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY dict_name (
        country_code String,
        ...
    )
    PRIMARY KEY country_code
    ```
  </TabItem>

  <TabItem value="xml" label="配置文件">
    ```xml
    <structure>
        <key>
            <attribute>
                <name>country_code</name>
                <type>String</type>
            </attribute>
        </key>
        ...
    </structure>
    ```
  </TabItem>
</Tabs>

<div id="improve-performance">
  ## 提高字典性能
</div>

有几种方法可以提高字典性能：

* 在 `GROUP BY` 之后调用处理字典的函数。
* 将要提取的属性标记为 injective。
  如果不同的键对应不同的属性值，则该属性称为 injective。
  因此，当 `GROUP BY` 中使用了通过键获取属性值的函数时，该函数会自动从 `GROUP BY` 中移出。

ClickHouse 会在字典出现错误时抛出异常。
错误示例包括：

* 无法加载被访问的字典。
* 查询 `cached` 字典时出错。

你可以在 [system.dictionaries](/zh/operations/system-tables/dictionaries.md) 表中查看字典及其状态列表。