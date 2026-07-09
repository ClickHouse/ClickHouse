---
slug: /sql-reference/statements/create/dictionary/sources
title: 'Dictionary ソース'
sidebar_position: 1
sidebar_label: '概要'
doc_type: 'reference'
description: 'Dictionary ソースタイプの設定'
---

import CloudDetails from '@site/docs/sql-reference/statements/create/dictionary/_snippet_dictionary_in_cloud.md';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="dictionary-sources">
  ## 構文
</div>

<CloudDetails />

Dictionary は、さまざまなソースを介して ClickHouse に接続できます。
ソースは、設定ファイルでは `source` セクションで、DDL ステートメントでは `SOURCE` 句で設定します。

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY dict_name (...)
    ...
    SOURCE(SOURCE_TYPE(param1 val1 ... paramN valN)) -- ソース設定
    ...
    ```
  </TabItem>

  <TabItem value="xml" label="設定ファイル">
    ```xml
    <clickhouse>
      <dictionary>
        ...
        <source>
          <source_type>
            <!-- ソース設定 -->
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
  ## サポートされている Dictionary ソース
</div>

以下のソースタイプ (`SOURCE_TYPE`/`source_type`) が利用できます。

* [ローカルファイル](./local-file.md)
* [実行可能ファイル](./executable-file.md)
* [実行可能プール](./executable-pool.md)
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

[ローカルファイル](./local-file.md)、[実行可能ファイル](./executable-file.md)、[HTTP(s)](./http.md)、[ClickHouse](./clickhouse.md) の各ソースタイプでは、
オプション設定を使用できます。

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(FILE(path './user_files/os.tsv' format 'TabSeparated'))
    --highlight-next-line
    SETTINGS(format_csv_allow_single_quotes = 0)
    ```
  </TabItem>

  <TabItem value="xml" label="設定ファイル">
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