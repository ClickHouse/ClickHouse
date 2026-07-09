---
description: '设置概览页面。'
sidebar_position: 1
slug: /operations/settings/overview
title: '设置概览'
doc_type: '参考'
---

<div id="overview">
  ## 概述
</div>

:::note
基于 XML 的 Settings Profiles 和[配置文件](/zh/operations/configuration-files)目前尚不支持 ClickHouse Cloud。要为您的 ClickHouse Cloud 服务指定设置，必须使用 [SQL 驱动的 Settings Profiles](/zh/operations/access-rights#settings-profiles-management)。
:::

ClickHouse 设置主要分为以下几组：

* 全局服务器设置
* 会话设置
* 查询设置
* 后台操作设置

全局设置默认生效，除非在更具体的级别上被覆盖。会话设置可以通过 profile、用户配置和 SET 命令指定。查询设置可以通过 SETTINGS 子句提供，并应用于单个查询。后台操作设置适用于变更、合并以及可能的其他操作，这些操作会在后台异步执行。

<div id="see-non-default-settings">
  ## 查看非默认设置
</div>

要查看哪些设置已偏离默认值，可以查询
`system.settings` 表：

```sql
SELECT name, value FROM system.settings WHERE changed
```

如果没有任何设置被修改为非默认值，那么 ClickHouse 将
不会返回任何内容。

要查看某个特定设置的值，你可以在
查询中指定该设置的 `name`：

```sql
SELECT name, value FROM system.settings WHERE name = 'max_threads'
```

将返回类似如下的内容：

```response
┌─name────────┬─value───┐
│ max_threads │ auto(8) │
└─────────────┴─────────┘

1 row in set. Elapsed: 0.002 sec.
```

<div id="further-reading">
  ## 延伸阅读
</div>

* 请参阅 [全局服务器设置](/zh/operations/server-configuration-parameters/settings.md)，详细了解如何在全局服务器级别配置您的
  ClickHouse server。
* 请参阅 [会话设置](/zh/operations/settings/settings-query-level.md)，详细了解如何在会话级别配置您的 ClickHouse
  server。
* 请参阅 [上下文层次结构](/zh/development/architecture.md#context)，详细了解 ClickHouse 的配置处理机制。