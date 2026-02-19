---
slug: /sql-reference/statements/create/dictionary/sources
title: 'Dictionary Sources'
sidebar_position: 1
sidebar_label: 'Overview'
doc_type: 'reference'
description: 'Dictionary source types configuration'
---

import CloudDetails from '@site/docs/sql-reference/statements/create/dictionary/_snippet_dictionary_in_cloud.md';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Syntax {#dictionary-sources}

<CloudDetails />

A dictionary can be connected to ClickHouse from many different sources.
The source is configured in the `source` section for configuration file and using the `SOURCE` clause for DDL statement.

<Tabs>
<TabItem value="ddl" label="DDL" default>

```sql
CREATE DICTIONARY dict_name (...)
...
SOURCE(SOURCE_TYPE(param1 val1 ... paramN valN)) -- Source configuration
...
```

</TabItem>
<TabItem value="xml" label="Configuration file">

```xml
<clickhouse>
  <dictionary>
    ...
    <source>
      <source_type>
        <!-- Source configuration -->
      </source_type>
    </source>
    ...
  </dictionary>
  ...
</clickhouse>
```

</TabItem>
</Tabs>

<br/>

## Supported dictionary sources {#supported-dictionary-sources}

The following source types (`SOURCE_TYPE`/`source_type`) are available:

- [Local file](./local-file)
- [Executable File](./executable-file)
- [Executable Pool](./executable-pool)
- [HTTP(S)](./http)
- DBMS
  - [ODBC](./odbc)
  - [MySQL](./mysql)
  - [ClickHouse](./clickhouse)
  - [MongoDB](./mongodb)
  - [Redis](./redis)
  - [Cassandra](./cassandra)
  - [PostgreSQL](./postgresql)
  - [YTsaurus](./ytsaurus)
- [YAMLRegExpTree](./yamlregexptree.md)
- [Null](./null)

For source types [Local file](./local-file), [Executable file](./executable-file), [HTTP(s)](./http), [ClickHouse](./clickhouse)
optional settings are available:

<Tabs>
<TabItem value="ddl" label="DDL" default>

```sql
SOURCE(FILE(path './user_files/os.tsv' format 'TabSeparated'))
--highlight-next-line
SETTINGS(format_csv_allow_single_quotes = 0)
```

</TabItem>
<TabItem value="xml" label="Configuration file">

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
