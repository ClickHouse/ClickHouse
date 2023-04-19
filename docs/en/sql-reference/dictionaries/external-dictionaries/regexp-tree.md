---
slug: /en/sql-reference/dictionaries/external-dictionaries/regexp-tree
sidebar_position: 47
sidebar_label: RegExp Tree Dictionary
title: "RegExp Tree Dictionary"
---
import CloudDetails from '@site/docs/en/sql-reference/dictionaries/external-dictionaries/_snippet_dictionary_in_cloud.md';

Regexp Tree dictionary stores multiple trees of regular expressions with attributions. Users can retrieve strings in the dictionary. If a string matches the root of the regexp tree, we will collect the corresponding attributes of the matched root and continue to walk the children. If any of the children matches the string, we will collect attributes and rewrite the old ones if conflicts occur, then continue the traverse until we reach leaf nodes.

Example of the ddl query for creating Regexp Tree dictionary:

<CloudDetails />

```sql
create dictionary regexp_dict
(
    regexp String,
    name String,
    version String
)
PRIMARY KEY(regexp)
SOURCE(YAMLRegExpTree(PATH '/var/lib/clickhouse/user_files/regexp_tree.yaml'))
LAYOUT(regexp_tree)
...
```

We only allow `YAMLRegExpTree` to work with regexp_tree dicitionary layout. If you want to use other sources, please set variable `regexp_dict_allow_other_sources` true.

**Source**

We introduce a type of source called `YAMLRegExpTree` representing the structure of Regexp Tree dictionary. An Example of a valid yaml config is like:

```xml
- regexp: 'Linux/(\d+[\.\d]*).+tlinux'
  name: 'TencentOS'
  version: '\1'

- regexp: '\d+/tclwebkit(?:\d+[\.\d]*)'
  name: 'Andriod'
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

The key `regexp` represents the regular expression of a tree node. The name of key is same as the dictionary key. The `name` and `version` is user-defined attributions in the dicitionary. The `versions` (which can be any name that not appear in attributions or the key) indicates the children nodes of this tree. 

**Back Reference**

The value of an attribution could contain a back reference which refers to a capture group of the matched regular expression. Reference number ranges from 1 to 9 and writes as `$1` or `\1`.

During the query execution, the back reference in the value will be replaced by the matched capture group.

**Query**

Due to the specialty of Regexp Tree dictionary, we only allow functions `dictGet`, `dictGetOrDefault` and `dictGetOrNull` work with it.

Example:

```sql
SELECT dictGet('regexp_dict', ('name', 'version'), '31/tclwebkit1024');
```

Result:

```
┌─dictGet('regexp_dict', ('name', 'version'), '31/tclwebkit1024')─┐
│ ('Andriod','12')                                                │
└─────────────────────────────────────────────────────────────────┘
```
