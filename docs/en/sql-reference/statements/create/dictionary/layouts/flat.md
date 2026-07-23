---
slug: /sql-reference/statements/create/dictionary/layouts/flat
title: 'flat dictionary layout'
sidebar_label: 'flat'
sidebar_position: 2
description: 'Store a dictionary in memory as flat arrays.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

With the `flat` layout, the dictionary is completely stored in memory in the form of flat arrays.
The amount of memory used is proportional to the size of the largest key (in space used).

:::tip
This layout type provides the best performance among all available methods of storing a dictionary.
:::

The dictionary key has the [UInt64](/sql-reference/data-types/int-uint.md) type and the value is limited to `max_array_size` (by default — 500,000).
If a larger key is discovered when creating the dictionary, ClickHouse throws an exception and does not create the dictionary.
The initial size of dictionary flat arrays are controlled by the `initial_array_size` setting (by default — 1024).

All types of sources are supported.
When updating the dictionary, data (from a file or from a table) is read in its entirety.

Configuration example:

<Tabs>
<TabItem value="ddl" label="DDL" default>

```sql
LAYOUT(FLAT(INITIAL_ARRAY_SIZE 50000 MAX_ARRAY_SIZE 5000000))
```

</TabItem>
<TabItem value="xml" label="Configuration file">

```xml
<layout>
  <flat>
    <initial_array_size>50000</initial_array_size>
    <max_array_size>5000000</max_array_size>
  </flat>
</layout>
```

</TabItem>
</Tabs>
<br/>
