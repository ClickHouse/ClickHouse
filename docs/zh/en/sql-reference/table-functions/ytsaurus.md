---
description: '该表函数支持从 YTsaurus 集群读取数据。'
sidebar_label: 'ytsaurus'
sidebar_position: 85
slug: /sql-reference/table-functions/ytsaurus
title: 'ytsaurus'
doc_type: '参考'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="ytsaurus-table-function">
  # ytsaurus 表函数
</div>

<ExperimentalBadge />

该表函数可用于从 YTsaurus 集群读取数据。

<div id="syntax">
  ## 语法
</div>

```sql
ytsaurus(http_proxy_url, cypress_path, oauth_token, format)
```

:::info
这是一个实验性功能，未来的发行版中可能会发生不向后兼容的更改。
如需启用 YTsaurus 表函数，请启用 [allow&#95;experimental&#95;ytsaurus&#95;table&#95;function](/zh/operations/settings/settings#allow_experimental_ytsaurus_table_engine) 设置。
输入命令 `set allow_experimental_ytsaurus_table_function = 1`。
:::

<div id="arguments">
  ## 参数
</div>

* `http_proxy_url` — YTsaurus HTTP 代理的 URL。
* `cypress_path` — 数据源的 Cypress 路径。
* `oauth_token` — OAuth 令牌。
* `format` — 数据源的[格式](/zh/interfaces/formats)。

**返回值**

一个具有指定结构的表，用于读取 YTsaurus 集群中指定 Cypress 路径下的数据。

**另请参阅**

* [YTsaurus 引擎](/zh/engines/table-engines/integrations/ytsaurus.md)