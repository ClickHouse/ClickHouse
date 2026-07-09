---
slug: /sql-reference/statements/create/dictionary/sources
title: 'Источники словарей'
sidebar_position: 1
sidebar_label: 'Обзор'
doc_type: 'справочник'
description: 'Настройка типов источников словарей'
---

import CloudDetails from '@site/docs/sql-reference/statements/create/dictionary/_snippet_dictionary_in_cloud.md';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="dictionary-sources">
  ## Синтаксис
</div>

<CloudDetails />

Словарь можно подключить к ClickHouse из множества источников.
Источник настраивается в разделе `source` файла конфигурации и с помощью предложения `SOURCE` в DDL-операторе.

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY dict_name (...)
    ...
    SOURCE(SOURCE_TYPE(param1 val1 ... paramN valN)) -- Конфигурация источника
    ...
    ```
  </TabItem>

  <TabItem value="xml" label="Файл конфигурации">
    ```xml
    <clickhouse>
      <dictionary>
        ...
        <source>
          <source_type>
            <!-- Конфигурация источника -->
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
  ## Поддерживаемые источники словарей
</div>

Доступны следующие типы источников (`SOURCE_TYPE`/`source_type`):

* [Локальный файл](./local-file.md)
* [Исполняемый файл](./executable-file.md)
* [Executable Pool](./executable-pool.md)
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

Для следующих типов источников: [Локальный файл](./local-file.md), [Исполняемый файл](./executable-file.md), [HTTP(s)](./http.md), [ClickHouse](./clickhouse.md)
доступны необязательные настройки:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(FILE(path './user_files/os.tsv' format 'TabSeparated'))
    --highlight-next-line
    SETTINGS(format_csv_allow_single_quotes = 0)
    ```
  </TabItem>

  <TabItem value="xml" label="Файл конфигурации">
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