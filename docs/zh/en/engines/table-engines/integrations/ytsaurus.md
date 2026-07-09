---
description: '可从 YTsaurus 集群导入数据的表引擎。'
sidebar_label: 'YTsaurus'
sidebar_position: 185
slug: /engines/table-engines/integrations/ytsaurus
title: 'YTsaurus 表引擎'
keywords: ['YTsaurus', '表引擎']
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="ytsaurus-table-engine">
  # YTsaurus 表引擎
</div>

<ExperimentalBadge />

<CloudNotSupportedBadge />

YTsaurus 表引擎可让您从 YTsaurus 集群导入数据。

<div id="creating-a-table">
  ## 创建表
</div>

```sql
    CREATE TABLE [IF NOT EXISTS] [db.]table_name
    (
        name1 [type1],
        name2 [type2], ...
    ) ENGINE = YTsaurus('http_proxy_url', 'cypress_path', 'oauth_token')
```

:::info
这是一个 Experimental 功能，可能会在未来的发行版中发生不向后兼容的变更。
通过设置 [`allow_experimental_ytsaurus_table_engine`](/zh/operations/settings/settings#allow_experimental_ytsaurus_table_engine)
启用 YTsaurus 表引擎。

可使用以下语句：

`SET allow_experimental_ytsaurus_table_engine = 1`.
:::

**引擎参数**

* `http_proxy_url` — YTsaurus HTTP 代理的 URL。
* `cypress_path` — 数据源的 Cypress 路径。
* `oauth_token` — OAuth 令牌。

<div id="usage-example">
  ## 使用示例
</div>

展示了创建 YTsaurus 表的查询：

```sql title="Query"
SHOW CREATE TABLE yt_saurus;
```

```sql title="Response"
CREATE TABLE yt_saurus
(
    `a` UInt32,
    `b` String
)
ENGINE = YTsaurus('http://localhost:8000', '//tmp/table', 'password')
```

要查询表中的数据，请运行：

```sql title="Query"
SELECT * FROM yt_saurus;
```

```response title="Response"
 ┌──a─┬─b──┐
 │ 10 │ 20 │
 └────┴────┘
```

<div id="data-types">
  ## 数据类型
</div>

<div id="primitive-data-types">
  ### 基本类型
</div>

| YTsaurus 数据类型                       | ClickHouse 数据类型 |
| ----------------------------------- | --------------- |
| `int8`                              | `Int8`          |
| `int16`                             | `Int16`         |
| `int32`                             | `Int32`         |
| `int64`                             | `Int64`         |
| `uint8`                             | `UInt8`         |
| `uint16`                            | `UInt16`        |
| `uint32`                            | `UInt32`        |
| `uint64`                            | `UInt64`        |
| `float`                             | `Float32`       |
| `double`                            | `Float64`       |
| `boolean`                           | `Bool`          |
| `string`                            | `String`        |
| `utf8`                              | `String`        |
| `json`                              | `JSON`          |
| `yson(type_v3)`                     | `JSON`          |
| `uuid`                              | `UUID`          |
| `date32`                            | `Date` (尚不支持)   |
| `datetime64`                        | `Int64`         |
| `timestamp64`                       | `Int64`         |
| `interval64`                        | `Int64`         |
| `date`                              | `Date` (尚不支持)   |
| `datetime`                          | `DateTime`      |
| `timestamp`                         | `DateTime64(6)` |
| `interval`                          | `UInt64`        |
| `any`                               | `String`        |
| `null`                              | `Nothing`       |
| `void`                              | `Nothing`       |
| `带有 `required = False`的`T&#96;&#96; | `Nullable(T)`   |

<div id="composite-data-types">
  ### 复合类型
</div>

| YTsaurus 数据类型 | ClickHouse 数据类型        |
| ------------- | ---------------------- |
| `decimal`     | `Decimal`              |
| `optional`    | `Nullable`             |
| `list`        | `Array`                |
| `struct`      | `NamedTuple`           |
| `tuple`       | `Tuple`                |
| `variant`     | `Variant`              |
| `dict`        | &#96;Array(Tuple(...)) |
| `tagged`      | `T`                    |

**另请参阅**

* [ytsaurus](../../../sql-reference/table-functions/ytsaurus.md) 表函数
* [ytsaurus 数据 schema](https://ytsaurus.tech/docs/en/user-guide/storage/static-schema)
* [ytsaurus 数据类型](https://ytsaurus.tech/docs/en/user-guide/storage/data-types)