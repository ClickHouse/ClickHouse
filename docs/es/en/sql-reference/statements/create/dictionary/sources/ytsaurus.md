---
slug: /sql-reference/statements/create/dictionary/sources/ytsaurus
title: 'YTsaurus como fuente de diccionario'
sidebar_position: 13
sidebar_label: 'YTsaurus'
description: 'Configure YTsaurus como fuente de diccionario en ClickHouse.'
doc_type: 'referencia'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<ExperimentalBadge />

<CloudNotSupportedBadge />

:::info
Esta es una funcionalidad experimental que puede cambiar de forma incompatible con versiones anteriores en versiones futuras.
Habilite el uso de la fuente de diccionario de YTsaurus
mediante la configuración [`allow_experimental_ytsaurus_dictionary_source`](/es/operations/settings/settings#allow_experimental_ytsaurus_dictionary_source).
:::

Ejemplo de configuración:

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

  <TabItem value="xml" label="Archivo de configuración">
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

Campos de configuración:

| Setting           | Description                           |
| ----------------- | ------------------------------------- |
| `http_proxy_urls` | URL del proxy HTTP de YTsaurus.       |
| `cypress_path`    | Ruta de Cypress a la tabla de origen. |
| `oauth_token`     | Token de OAuth.                       |