---
slug: /sql-reference/statements/create/dictionary/sources
title: 'Fuentes de diccionario'
sidebar_position: 1
sidebar_label: 'Descripción general'
doc_type: 'reference'
description: 'Configuración de tipos de fuentes de diccionario'
---

import CloudDetails from '@site/docs/sql-reference/statements/create/dictionary/_snippet_dictionary_in_cloud.md';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="dictionary-sources">
  ## Sintaxis
</div>

<CloudDetails />

Un diccionario puede conectarse a ClickHouse desde distintos tipos de fuentes.
La fuente se configura en la sección `source` del archivo de configuración o mediante la cláusula `SOURCE` en una sentencia DDL.

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY dict_name (...)
    ...
    SOURCE(SOURCE_TYPE(param1 val1 ... paramN valN)) -- Configuración de la fuente
    ...
    ```
  </TabItem>

  <TabItem value="xml" label="Archivo de configuración">
    ```xml
    <clickhouse>
      <dictionary>
        ...
        <source>
          <source_type>
            <!-- Configuración de la fuente -->
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
  ## Fuentes de diccionario compatibles
</div>

Están disponibles los siguientes tipos de fuente (`SOURCE_TYPE`/`source_type`):

* [Archivo local](./local-file.md)
* [Archivo ejecutable](./executable-file.md)
* [Grupo de ejecutables](./executable-pool.md)
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

Los tipos de fuente [Archivo local](./local-file.md), [Archivo ejecutable](./executable-file.md), [HTTP(s)](./http.md) y [ClickHouse](./clickhouse.md)
disponen de opciones de configuración opcionales:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(FILE(path './user_files/os.tsv' format 'TabSeparated'))
    --highlight-next-line
    SETTINGS(format_csv_allow_single_quotes = 0)
    ```
  </TabItem>

  <TabItem value="xml" label="Archivo de configuración">
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