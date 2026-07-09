---
slug: /sql-reference/statements/create/dictionary/sources/ytsaurus
title: 'Fonte de dicionário do YTsaurus'
sidebar_position: 13
sidebar_label: 'YTsaurus'
description: 'Configure o YTsaurus como fonte de dicionário no ClickHouse.'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<ExperimentalBadge />

<CloudNotSupportedBadge />

:::info
Este é um recurso experimental que pode mudar de formas incompatíveis com versões anteriores em lançamentos futuros.
Habilite o uso da origem de dicionário YTsaurus
usando a configuração [`allow_experimental_ytsaurus_dictionary_source`](/pt-BR/operations/settings/settings#allow_experimental_ytsaurus_dictionary_source).
:::

Exemplo de configurações:

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

  <TabItem value="xml" label="Arquivo de configuração">
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

Campos de configuração:

| Configuração      | Descrição                                   |
| ----------------- | ------------------------------------------- |
| `http_proxy_urls` | URL para o proxy HTTP do YTsaurus.          |
| `cypress_path`    | Caminho do Cypress para a tabela de origem. |
| `oauth_token`     | Token OAuth.                                |