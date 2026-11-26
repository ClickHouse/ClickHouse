---
slug: /sql-reference/statements/create/dictionary/sources/yamlregexptree
title: 'YAMLRegExpTree dictionary source'
sidebar_position: 15
sidebar_label: 'YAMLRegExpTree'
description: 'Configure a YAML file as a source for regular expression tree dictionaries.'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge/>

The `YAMLRegExpTree` source loads a regular expression tree from a YAML file on the local filesystem.
It is designed exclusively for use with the [`regexp_tree`](../layouts/regexp-tree.md) dictionary layout
and provides hierarchical regex-to-attribute mappings for pattern-based lookups such as user agent parsing.

:::note
The `YAMLRegExpTree` source is only available in ClickHouse Open Source.
For ClickHouse Cloud, export the dictionary to CSV and load it via a [ClickHouse table source](./clickhouse.md) instead.
See [Using regexp_tree dictionaries in ClickHouse Cloud](../layouts/regexp-tree#use-regular-expression-tree-dictionary-in-clickhouse-cloud) for details.
:::

## Configuration {#configuration}

```sql
CREATE DICTIONARY regexp_dict
(
    regexp String,
    name String,
    version String
)
PRIMARY KEY(regexp)
SOURCE(YAMLRegExpTree(PATH '/var/lib/clickhouse/user_files/regexp_tree.yaml'))
LAYOUT(regexp_tree)
LIFETIME(0);
```

Setting fields:

| Setting | Description |
|---------|-------------|
| `PATH`  | The absolute path to the YAML file containing the regular expression tree. When created via DDL, the file must be in the `user_files` directory. |

## YAML file structure {#yaml-file-structure}

The YAML file contains a list of regular expression tree nodes. Each node can have attributes and child nodes, forming a hierarchy:

```yaml
- regexp: 'Linux/(\d+[\.\d]*).+tlinux'
  name: 'TencentOS'
  version: '\1'

- regexp: '\d+/tclwebkit(?:\d+[\.\d]*)'
  name: 'Android'
  versions:
    - regexp: '33/tclwebkit'
      version: '13'
    - regexp: '3[12]/tclwebkit'
      version: '12'
    - regexp: '30/tclwebkit'
      version: '11'
    - regexp: '29/tclwebkit'
      version: '10'
```

Each node has the following structure:

- **`regexp`**: The regular expression for this node.
- **attributes**: User-defined dictionary attributes (e.g. `name`, `version`). Attribute values may contain **back references** to capture groups in the regular expression, written as `\1` or `$1` (numbers 1-9). These are replaced with the matched capture group at query time.
- **child nodes**: A list of children, each with its own attributes and optionally more children. The name of the child list is arbitrary (e.g. `versions` above). String matching proceeds depth-first: if a string matches a node, its children are also checked. Attributes of the deepest matching node take precedence, overriding equally named parent attributes.

## Related pages {#related-pages}

- [regexp_tree dictionary layout](../layouts/regexp-tree.md) — layout configuration, query examples, and matching modes
- [dictGet](/sql-reference/functions/ext-dict-functions#dictGet), [dictGetAll](/sql-reference/functions/ext-dict-functions#dictGetAll) — functions for querying regexp tree dictionaries
