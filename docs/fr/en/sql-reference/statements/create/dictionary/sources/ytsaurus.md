---
slug: /sql-reference/statements/create/dictionary/sources/ytsaurus
title: 'Source de dictionnaire YTsaurus'
sidebar_position: 13
sidebar_label: 'YTsaurus'
description: 'Configurer YTsaurus comme source de dictionnaire dans ClickHouse.'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<ExperimentalBadge />

<CloudNotSupportedBadge />

:::info
Il s’agit d’une fonctionnalité expérimentale qui pourra changer de manière incompatible avec les versions précédentes dans de futures versions.
Activez l’utilisation de la source de dictionnaire YTsaurus
à l’aide du paramètre [`allow_experimental_ytsaurus_dictionary_source`](/fr/operations/settings/settings#allow_experimental_ytsaurus_dictionary_source).
:::

Exemple de paramètres :

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

  <TabItem value="xml" label="Fichier de configuration">
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

Champs du paramètre :

| Paramètre         | Description                        |
| ----------------- | ---------------------------------- |
| `http_proxy_urls` | URL du proxy HTTP YTsaurus.        |
| `cypress_path`    | Chemin Cypress de la table source. |
| `oauth_token`     | Jeton OAuth.                       |