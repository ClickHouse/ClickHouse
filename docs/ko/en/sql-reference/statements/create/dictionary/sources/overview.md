---
slug: /sql-reference/statements/create/dictionary/sources
title: '딕셔너리 소스'
sidebar_position: 1
sidebar_label: '개요'
doc_type: '참고'
description: '딕셔너리 소스 유형 구성'
---

import CloudDetails from '@site/docs/sql-reference/statements/create/dictionary/_snippet_dictionary_in_cloud.md';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="dictionary-sources">
  ## 구문
</div>

<CloudDetails />

딕셔너리는 다양한 소스를 통해 ClickHouse에 연결할 수 있습니다.
소스는 설정 파일의 `source` 섹션에서 구성하거나 DDL 문에서 `SOURCE` 절을 사용해 구성합니다.

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY dict_name (...)
    ...
    SOURCE(SOURCE_TYPE(param1 val1 ... paramN valN)) -- 소스 구성
    ...
    ```
  </TabItem>

  <TabItem value="xml" label="설정 파일">
    ```xml
    <clickhouse>
      <dictionary>
        ...
        <source>
          <source_type>
            <!-- 소스 구성 -->
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
  ## 지원되는 딕셔너리 소스
</div>

다음 소스 유형(`SOURCE_TYPE`/`source_type`)을 사용할 수 있습니다.

* [로컬 파일](./local-file.md)
* [실행형 파일](./executable-file.md)
* [실행형 풀](./executable-pool.md)
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

[로컬 파일](./local-file.md), [실행형 파일](./executable-file.md), [HTTP(s)](./http.md), [ClickHouse](./clickhouse.md) 소스 유형에서는
선택적 설정을 사용할 수 있습니다.

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(FILE(path './user_files/os.tsv' format 'TabSeparated'))
    --다음 줄 강조
    SETTINGS(format_csv_allow_single_quotes = 0)
    ```
  </TabItem>

  <TabItem value="xml" label="설정 파일">
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