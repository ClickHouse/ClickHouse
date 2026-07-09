---
slug: /sql-reference/statements/create/dictionary/sources/ytsaurus
title: 'Источник словаря YTsaurus'
sidebar_position: 13
sidebar_label: 'YTsaurus'
description: 'Настройка YTsaurus в качестве источника словаря в ClickHouse.'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<ExperimentalBadge />

<CloudNotSupportedBadge />

:::info
Это экспериментальная возможность, которая в будущих релизах может измениться несовместимым с предыдущими версиями образом.
Чтобы включить источник словаря YTsaurus, используйте настройку [`allow_experimental_ytsaurus_dictionary_source`](/ru/operations/settings/settings#allow_experimental_ytsaurus_dictionary_source).
:::

Пример настроек:

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

  <TabItem value="xml" label="Файл конфигурации">
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

Поля настройки:

| Настройка         | Описание                         |
| ----------------- | -------------------------------- |
| `http_proxy_urls` | URL HTTP-прокси YTsaurus.        |
| `cypress_path`    | Путь Cypress к исходной таблице. |
| `oauth_token`     | Токен OAuth.                     |