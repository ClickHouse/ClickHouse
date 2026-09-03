---
description: 'Dictionary layout types for storing dictionaries in memory'
sidebar_label: 'Overview'
sidebar_position: 1
slug: /sql-reference/statements/create/dictionary/layouts
title: 'Dictionary layouts'
doc_type: 'reference'
---

import CloudDetails from '@site/docs/sql-reference/statements/create/dictionary/_snippet_dictionary_in_cloud.md';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Dictionary layout types {#storing-dictionaries-in-memory}

There are a variety of ways to store dictionaries in memory, each with CPU and RAM-usage trade-offs.

| Layout | Description |
|---|---|
| [flat](./flat.md) | Stores data in flat arrays indexed by key. Fastest layout, but keys must be `UInt64` and bounded by `max_array_size`. |
| [hashed](./hashed.md) | Stores data in a hash table. No key size limit, supports any number of elements. |
| [sparse_hashed](./hashed.md#sparse_hashed) | Like `hashed`, but trades CPU for lower memory usage. |
| [complex_key_hashed](./hashed.md#complex_key_hashed) | Like `hashed`, for composite keys. |
| [complex_key_sparse_hashed](./hashed.md#complex_key_sparse_hashed) | Like `sparse_hashed`, for composite keys. |
| [hashed_array](./hashed-array.md) | Attributes stored in arrays with a hash table mapping keys to array indices. Memory-efficient for many attributes. |
| [complex_key_hashed_array](./hashed-array.md#complex_key_hashed_array) | Like `hashed_array`, for composite keys. |
| [range_hashed](./range-hashed.md) | Hash table with ordered ranges. Supports lookups by key + date/time range. |
| [complex_key_range_hashed](./range-hashed.md#complex_key_range_hashed) | Like `range_hashed`, for composite keys. |
| [cache](./cache.md) | Fixed-size in-memory cache. Only frequently accessed keys are stored. |
| [complex_key_cache](/sql-reference/statements/create/dictionary/layouts/hashed#complex_key_hashed) | Like `cache`, for composite keys. |
| [ssd_cache](./ssd-cache.md) | Like `cache`, but stores data on SSD with an in-memory index. |
| [complex_key_ssd_cache](./ssd-cache.md#complex_key_ssd_cache) | Like `ssd_cache`, for composite keys. |
| [direct](./direct.md) | No in-memory storage — queries the source directly for each request. |
| [complex_key_direct](./direct.md#complex_key_direct) | Like `direct`, for composite keys. |
| [ip_trie](./ip-trie.md) | Trie structure for fast IP prefix lookups (CIDR-based). |

:::tip Recommended layouts
[flat](./flat.md), [hashed](./hashed.md), and [complex_key_hashed](./hashed.md#complex_key_hashed) provide the best query performance.
Caching layouts are not recommended due to potentially poor performance and difficulty tuning parameters — see [cache](./cache.md) for details.
:::

## Specify dictionary layout {#specify-dictionary-layout}

<CloudDetails />

You can configure a dictionary layout with the `LAYOUT` clause (for DDL) or the `layout` setting for configuration file definitions.

<Tabs>
<TabItem value="ddl" label="DDL" default>

```sql
CREATE DICTIONARY (...)
...
LAYOUT(LAYOUT_TYPE(param value)) -- layout settings
...
```

</TabItem>
<TabItem value="xml" label="Configuration file">

```xml
<clickhouse>
    <dictionary>
        ...
        <layout>
            <layout_type>
                <!-- layout settings -->
            </layout_type>
        </layout>
        ...
    </dictionary>
</clickhouse>
```

</TabItem>
</Tabs>
<br/>

See also [CREATE DICTIONARY](../overview.md) for the full DDL syntax.

Dictionaries without word `complex-key*` in a layout have a key with [UInt64](/sql-reference/data-types/int-uint.md) type, `complex-key*` dictionaries have a composite key (complex, with arbitrary types).

**Numeric key example** (column key_column has [UInt64](/sql-reference/data-types/int-uint.md) type):

<Tabs>
<TabItem value="ddl" label="DDL" default>

```sql
CREATE DICTIONARY dict_name (
    key_column UInt64,
    ...
)
PRIMARY KEY key_column
```

</TabItem>
<TabItem value="xml" label="Configuration file">

```xml
<structure>
    <id>
        <name>key_column</name>
    </id>
    ...
</structure>
```

</TabItem>
</Tabs>
<br/>

**Composite key example** (key has one element with [String](/sql-reference/data-types/string.md) type):

<Tabs>
<TabItem value="ddl" label="DDL" default>

```sql
CREATE DICTIONARY dict_name (
    country_code String,
    ...
)
PRIMARY KEY country_code
```

</TabItem>
<TabItem value="xml" label="Configuration file">

```xml
<structure>
    <key>
        <attribute>
            <name>country_code</name>
            <type>String</type>
        </attribute>
    </key>
    ...
</structure>
```

</TabItem>
</Tabs>

## Improve dictionary performance {#improve-performance}

There are several ways to improve dictionary performance:

- Call the function for working with the dictionary after `GROUP BY`.
- Mark attributes to extract as injective.
  An attribute is called injective if different keys correspond to different attribute values.
  So when `GROUP BY` uses a function that fetches an attribute value by the key, this function is automatically taken out of `GROUP BY`.

ClickHouse generates an exception for errors with dictionaries.
Examples of errors can be:

- The dictionary being accessed could not be loaded.
- Error querying a `cached` dictionary.

You can view the list of dictionaries and their statuses in the [system.dictionaries](/operations/system-tables/dictionaries.md) table.
