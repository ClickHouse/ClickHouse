---
slug: /sql-reference/statements/create/dictionary/sources
title: 'مصادر القواميس'
sidebar_position: 1
sidebar_label: 'نظرة عامة'
doc_type: 'مرجع'
description: 'تهيئة أنواع مصادر القواميس'
---

import CloudDetails from '@site/docs/sql-reference/statements/create/dictionary/_snippet_dictionary_in_cloud.md';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="dictionary-sources">
  ## الصياغة
</div>

<CloudDetails />

يمكن ربط القاموس بـ ClickHouse من مصادر مختلفة عديدة.
يُضبط المصدر في قسم `source` في ملف التهيئة، وباستخدام العبارة `SOURCE` في تعليمة DDL.

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY dict_name (...)
    ...
    SOURCE(SOURCE_TYPE(param1 val1 ... paramN valN)) -- تهيئة المصدر
    ...
    ```
  </TabItem>

  <TabItem value="xml" label="ملف التهيئة">
    ```xml
    <clickhouse>
      <dictionary>
        ...
        <source>
          <source_type>
            <!-- تهيئة المصدر -->
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
  ## مصادر القواميس المدعومة
</div>

أنواع المصادر التالية (`SOURCE_TYPE`/`source_type`) متاحة:

* [ملف محلي](./local-file.md)
* [ملف قابل للتنفيذ](./executable-file.md)
* [مجموعة تنفيذية](./executable-pool.md)
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

بالنسبة إلى أنواع المصادر [ملف محلي](./local-file.md)، و[ملف قابل للتنفيذ](./executable-file.md)، و[HTTP(s)](./http.md)، و[ClickHouse](./clickhouse.md)،
فالإعدادات الاختيارية التالية متاحة:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(FILE(path './user_files/os.tsv' format 'TabSeparated'))
    --highlight-next-line
    SETTINGS(format_csv_allow_single_quotes = 0)
    ```
  </TabItem>

  <TabItem value="xml" label="ملف التهيئة">
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