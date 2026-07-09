---
slug: /sql-reference/statements/create/dictionary/sources/ytsaurus
title: 'YTsaurus 字典源'
sidebar_position: 13
sidebar_label: 'YTsaurus'
description: '将 YTsaurus 配置为 ClickHouse 中的字典源。'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<ExperimentalBadge />

<CloudNotSupportedBadge />

:::info
这是一个 Experimental 功能，可能会在未来的发行版中发生向后不兼容的变更。
通过设置 [`allow_experimental_ytsaurus_dictionary_source`](/zh/operations/settings/settings#allow_experimental_ytsaurus_dictionary_source) 启用 YTsaurus 字典源。
:::

设置示例：

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(YTSAURUS(
        http_proxy_urls 'http://localhost:8000'
        cypress_path '//tmp/test'
        oauth_token 'password'
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="配置文件">
    ```xml
    <source>
        <ytsaurus>
            <http_proxy_urls>http://localhost:8000</http_proxy_urls>
            <cypress_path>//tmp/test</cypress_path>
            <oauth_token>password</oauth_token>
            <check_table_schema>1</check_table_schema>
        </ytsaurus>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

设置字段：

| 设置项               | 描述                     |
| ----------------- | ---------------------- |
| `http_proxy_urls` | YTsaurus HTTP 代理的 URL。 |
| `cypress_path`    | 表源的 Cypress 路径。        |
| `oauth_token`     | OAuth 令牌。              |