---
description: '创建和配置字典的文档'
sidebar_label: '概览'
sidebar_position: 1
slug: /sql-reference/statements/create/dictionary
title: 'CREATE DICTIONARY'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import CloudSupportedBadge from '@theme/badges/CloudSupportedBadge';

<div id="create-dictionary">
  # CREATE DICTIONARY
</div>

字典是一种映射 (`key -> attributes`) ，适合用于各种类型的参照列表。
ClickHouse 支持一组用于处理字典的特殊函数，并且可以在查询中使用。与对参照表使用 `JOIN` 相比，借助这些函数使用字典更简单，也更高效。

字典可以通过以下两种方式创建：

* [使用 DDL 查询](#creating-a-dictionary-with-a-ddl-query) (推荐)
* [使用配置文件](#creating-a-dictionary-with-a-configuration-file)

<div id="creating-a-dictionary-with-a-ddl-query">
  ## 使用 DDL 查询创建字典
</div>

<CloudSupportedBadge />

可以使用 DDL 查询创建字典。
这是推荐的方法，因为使用 DDL 创建的字典具有以下优势：

* 无需在服务器配置文件中添加额外记录。
* 字典可以像表或视图这类一等实体一样使用。
* 可以使用熟悉的 `SELECT` 语法直接读取数据，而不必使用字典表函数。请注意，通过 `SELECT` 语句直接访问字典时，缓存型字典只会返回已缓存的数据，而非缓存型字典会返回其存储的全部数据。
* 字典可以轻松重命名。

<div id="syntax">
  ### 语法
</div>

```sql
CREATE [OR REPLACE] DICTIONARY [IF NOT EXISTS] [db.]dictionary_name [ON CLUSTER cluster]
(
    key1  type1  [DEFAULT | EXPRESSION expr1] [IS_OBJECT_ID],
    key2  type2  [DEFAULT | EXPRESSION expr2],
    attr1 type2  [DEFAULT | EXPRESSION expr3] [HIERARCHICAL|INJECTIVE],
    attr2 type2  [DEFAULT | EXPRESSION expr4] [HIERARCHICAL|INJECTIVE]
)
PRIMARY KEY key1, key2
SOURCE(SOURCE_NAME([param1 value1 ... paramN valueN]))
LAYOUT(LAYOUT_NAME([param_name param_value]))
LIFETIME({MIN min_val MAX max_val | max_val})
SETTINGS(setting_name = setting_value, setting_name = setting_value, ...)
COMMENT 'Comment'
```

| Clause                                      | Description                                  |
| ------------------------------------------- | -------------------------------------------- |
| [属性](./attributes.md)                       | 字典属性的指定方式与表的列类似。唯一必需的属性为类型，其他属性都可以有默认值。      |
| PRIMARY KEY                                 | 定义用于字典查找的键列。根据布局，可将一个或多个属性指定为键。              |
| [`SOURCE`](./sources/overview.md)           | 定义字典的数据源 (例如 ClickHouse 表、HTTP、PostgreSQL) 。 |
| [`LAYOUT`](./layouts/overview.md)           | 控制字典在内存中的存储方式 (例如 `FLAT`、`HASHED`、`CACHE`) 。 |
| [`LIFETIME`](./lifetime.md)                 | 设置字典的刷新间隔。                                   |
| [`ON CLUSTER`](../../../distributed-ddl.md) | 在集群上创建字典。可选。                                 |
| `SETTINGS`                                  | 附加的字典设置。可选。                                  |
| `COMMENT`                                   | 为字典添加文本注释。可选。                                |

<div id="creating-a-dictionary-with-a-configuration-file">
  ## 使用配置文件创建字典
</div>

<CloudNotSupportedBadge />

:::note
ClickHouse Cloud 不支持使用配置文件创建字典。请改用 DDL (见上文) ，并以 `default` 用户创建字典。
:::

字典配置文件的格式如下：

```xml
<clickhouse>
    <comment>An optional element with any content. Ignored by the ClickHouse server.</comment>

    <!--Optional element. File name with substitutions-->
    <include_from>/etc/metrika.xml</include_from>


    <dictionary>
        <!-- Dictionary configuration. -->
        <!-- There can be any number of dictionary sections in a configuration file. -->
    </dictionary>

</clickhouse>
```

你可以在同一文件中配置任意数量的字典。

<div id="related-content">
  ## 相关内容
</div>

* [布局](/zh/sql-reference/statements/create/dictionary/layouts) — 字典在内存中的存储布局
* [数据源](/zh/sql-reference/statements/create/dictionary/sources) — 连接到数据源
* [生命周期](./lifetime.md) — 自动刷新配置
* [属性](./attributes.md) — 键和属性配置
* [嵌入式字典](./embedded.md) — 内置 geobase 字典
* [system.dictionaries](../../../../operations/system-tables/dictionaries.md) — 包含字典信息的系统表