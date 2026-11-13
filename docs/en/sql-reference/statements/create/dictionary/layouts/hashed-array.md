---
slug: /sql-reference/statements/create/dictionary/layouts/hashed-array
title: 'hashed_array dictionary layout types'
sidebar_label: 'hashed_array'
sidebar_position: 4
description: 'Store a dictionary in memory using a hash table with attribute arrays.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## hashed_array {#hashed_array}

The dictionary is completely stored in memory. Each attribute is stored in an array. The key attribute is stored in the form of a hashed table where value is an index in the attributes array. The dictionary can contain any number of elements with any identifiers. In practice, the number of keys can reach tens of millions of items.

The dictionary key has the [UInt64](/sql-reference/data-types/int-uint.md) type.

All types of sources are supported. When updating, data (from a file or from a table) is read in its entirety.

Configuration example:

<Tabs>
<TabItem value="ddl" label="DDL" default>

```sql
LAYOUT(HASHED_ARRAY([SHARDS 1]))
```

</TabItem>
<TabItem value="xml" label="Configuration file">

```xml
<layout>
  <hashed_array>
  </hashed_array>
</layout>
```

</TabItem>
</Tabs>
<br/>

## complex_key_hashed_array {#complex_key_hashed_array}

This type of storage is for use with composite [keys](../attributes.md#composite-key). Similar to [hashed_array](#hashed_array).

Configuration example:

<Tabs>
<TabItem value="ddl" label="DDL" default>

```sql
LAYOUT(COMPLEX_KEY_HASHED_ARRAY([SHARDS 1]))
```

</TabItem>
<TabItem value="xml" label="Configuration file">

```xml
<layout>
  <complex_key_hashed_array />
</layout>
```

</TabItem>
</Tabs>
<br/>
