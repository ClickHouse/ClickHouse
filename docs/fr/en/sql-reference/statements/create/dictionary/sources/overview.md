---
slug: /sql-reference/statements/create/dictionary/sources
title: 'Sources de dictionnaire'
sidebar_position: 1
sidebar_label: "Vue d’ensemble"
doc_type: 'référence'
description: 'Configuration des types de sources de dictionnaire'
---

import CloudDetails from '@site/docs/sql-reference/statements/create/dictionary/_snippet_dictionary_in_cloud.md';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="dictionary-sources">
  ## Syntaxe
</div>

<CloudDetails />

Un dictionnaire peut être connecté à ClickHouse depuis de nombreuses sources différentes.
La source se configure dans la section `source` du fichier de configuration et à l’aide de la clause `SOURCE` dans l’instruction DDL.

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY dict_name (...)
    ...
    SOURCE(SOURCE_TYPE(param1 val1 ... paramN valN)) -- Configuration de la source
    ...
    ```
  </TabItem>

  <TabItem value="xml" label="Fichier de configuration">
    ```xml
    <clickhouse>
      <dictionary>
        ...
        <source>
          <source_type>
            <!-- Configuration de la source -->
          </source_type>
        </source>
        ...
      </dictionary>
      ...
    </clickhouse>
    ```
  </TabItem>
</Tabs>

<br />

<div id="supported-dictionary-sources">
  ## Sources de dictionnaire prises en charge
</div>

Les types de source (`SOURCE_TYPE`/`source_type`) suivants sont disponibles :

* [Fichier local](./local-file.md)
* [Fichier exécutable](./executable-file.md)
* [Pool d&#39;exécutables](./executable-pool.md)
* [HTTP(S)](./http.md)
* DBMS
  * [ODBC](./odbc.md)
  * [MySQL](./mysql.md)
  * [ClickHouse](./clickhouse.md)
  * [MongoDB](./mongodb.md)
  * [Redis](./redis.md)
  * [Cassandra](./cassandra.md)
  * [PostgreSQL](./postgresql.md)
  * [YTsaurus](./ytsaurus.md)
* [YAMLRegExpTree](./yamlregexptree.md)
* [Null](./null.md)

Pour les types de source [Fichier local](./local-file.md), [Fichier exécutable](./executable-file.md), [HTTP(s)](./http.md), [ClickHouse](./clickhouse.md),
des paramètres optionnels sont disponibles :

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(FILE(path './user_files/os.tsv' format 'TabSeparated'))
    --highlight-next-line
    SETTINGS(format_csv_allow_single_quotes = 0)
    ```
  </TabItem>

  <TabItem value="xml" label="Fichier de configuration">
    ```xml
    <source>
      <file>
        <path>/opt/dictionaries/os.tsv</path>
        <format>TabSeparated</format>
      </file>
      <settings>
    #highlight-next-line
          <format_csv_allow_single_quotes>0</format_csv_allow_single_quotes>
      </settings>
    </source>
    ```
  </TabItem>
</Tabs>