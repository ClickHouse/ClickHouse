---
slug: /sql-reference/statements/create/dictionary/sources
title: 'Fontes de dicionário'
sidebar_position: 1
sidebar_label: 'Visão geral'
doc_type: 'referência'
description: 'Configuração dos tipos de fontes de dicionário'
---

import CloudDetails from '@site/docs/sql-reference/statements/create/dictionary/_snippet_dictionary_in_cloud.md';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="dictionary-sources">
  ## Sintaxe
</div>

<CloudDetails />

Um Dicionário pode ser conectado ao ClickHouse a partir de diversas fontes diferentes.
A fonte é configurada na seção `source` do arquivo de configuração ou por meio da cláusula `SOURCE` na instrução DDL.

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY dict_name (...)
    ...
    SOURCE(SOURCE_TYPE(param1 val1 ... paramN valN)) -- Configuração da fonte
    ...
    ```
  </TabItem>

  <TabItem value="xml" label="Arquivo de configuração">
    ```xml
    <clickhouse>
      <dictionary>
        ...
        <source>
          <source_type>
            <!-- Configuração da fonte -->
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
  ## Fontes de dicionário suportadas
</div>

Os seguintes tipos de fonte (`SOURCE_TYPE`/`source_type`) estão disponíveis:

* [Arquivo local](./local-file.md)
* [Arquivo executável](./executable-file.md)
* [Pool de executáveis](./executable-pool.md)
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

Para os tipos de fonte [Arquivo local](./local-file.md), [Arquivo executável](./executable-file.md), [HTTP(s)](./http.md), [ClickHouse](./clickhouse.md),
há configurações opcionais disponíveis:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(FILE(path './user_files/os.tsv' format 'TabSeparated'))
    --highlight-next-line
    SETTINGS(format_csv_allow_single_quotes = 0)
    ```
  </TabItem>

  <TabItem value="xml" label="Arquivo de configuração">
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