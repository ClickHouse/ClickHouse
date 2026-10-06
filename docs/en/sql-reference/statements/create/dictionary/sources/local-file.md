---
slug: /sql-reference/statements/create/dictionary/sources/local-file
title: 'Local File dictionary source'
sidebar_position: 2
sidebar_label: 'Local File'
description: 'Configure a local file as a dictionary source in ClickHouse.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

The local file source loads dictionary data from a file on the local filesystem. This is useful for small, static lookup tables that can be stored as flat files in formats such as TSV, CSV, or any other [supported format](/sql-reference/formats).

Example of settings:

<Tabs>
<TabItem value="ddl" label="DDL" default>

```sql
SOURCE(FILE(path './user_files/os.tsv' format 'TabSeparated'))
```

</TabItem>
<TabItem value="xml" label="Configuration file">

```xml
<source>
  <file>
    <path>/opt/dictionaries/os.tsv</path>
    <format>TabSeparated</format>
  </file>
</source>
```

</TabItem>
</Tabs>

<br/>

Setting fields:

| Setting | Description |
|---------|-------------|
| `path` | The absolute path to the file. |
| `format` | The file format. All the formats described in [Formats](/sql-reference/formats) are supported. |

When a dictionary with source `FILE` is created via DDL command (`CREATE DICTIONARY ...`), the source file needs to be located in the `user_files` directory to prevent DB users from accessing arbitrary files on the ClickHouse node.

**See Also**

- [Dictionary function](/sql-reference/table-functions/dictionary)
