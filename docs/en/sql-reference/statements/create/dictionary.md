---
toc_priority: 38
toc_title: DICTIONARY
---

# CREATE DICTIONARY {#create-dictionary-query}

Creates a new [external dictionary](../../../sql-reference/dictionaries/external-dictionaries/external-dicts.md) with given [structure](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md), [source](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md), [layout](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md) and [lifetime](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md).

Syntax:

``` sql
CREATE DICTIONARY [IF NOT EXISTS] [db.]dictionary_name [ON CLUSTER cluster]
(
    key1 type1  [DEFAULT|EXPRESSION expr1] [IS_OBJECT_ID],
    key2 type2  [DEFAULT|EXPRESSION expr2] ,
    attr1 type2 [DEFAULT|EXPRESSION expr3] [HIERARCHICAL|INJECTIVE],
    attr2 type2 [DEFAULT|EXPRESSION expr4] [HIERARCHICAL|INJECTIVE]
)
PRIMARY KEY key1, key2
SOURCE(SOURCE_NAME([param1 value1 ... paramN valueN]))
LAYOUT(LAYOUT_NAME([param_name param_value]))
LIFETIME({MIN min_val MAX max_val | max_val})
```

External dictionary structure consists of attributes. Dictionary attributes are specified similarly to table columns. The only required attribute property is its type, all other properties may have default values.

`ON CLUSTER` clause allows creating dictionary on a cluster, see [Distributed DDL](../../../sql-reference/distributed-ddl.md).

Depending on dictionary [layout](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md) one or more attributes can be specified as dictionary keys.

For more information, see [External Dictionaries](../../../sql-reference/dictionaries/external-dictionaries/external-dicts.md) section.
