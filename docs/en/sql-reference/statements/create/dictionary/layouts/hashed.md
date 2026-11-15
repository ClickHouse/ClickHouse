---
slug: /sql-reference/statements/create/dictionary/layouts/hashed
title: 'hashed dictionary layout types'
sidebar_label: 'hashed'
sidebar_position: 3
description: 'Store a dictionary in memory using hash tables: hashed, sparse_hashed, complex_key_hashed, complex_key_sparse_hashed'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## hashed {#hashed}

The dictionary is completely stored in memory in the form of a hash table. The dictionary can contain any number of elements with any identifiers. In practice, the number of keys can reach tens of millions of items.

The dictionary key has the [UInt64](/sql-reference/data-types/int-uint.md) type.

All types of sources are supported. When updating, data (from a file or from a table) is read in its entirety.

Configuration example:

<Tabs>
<TabItem value="ddl" label="DDL" default>

```sql
LAYOUT(HASHED())
```

</TabItem>
<TabItem value="xml" label="Configuration file">

```xml
<layout>
  <hashed />
</layout>
```

</TabItem>
</Tabs>
<br/>

Configuration example with settings:

<Tabs>
<TabItem value="ddl" label="DDL" default>

```sql
LAYOUT(HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
```

</TabItem>
<TabItem value="xml" label="Configuration file">

```xml
<layout>
  <hashed>
    <!-- If shards greater then 1 (default is `1`) the dictionary will load
         data in parallel, useful if you have huge amount of elements in one
         dictionary. -->
    <shards>10</shards>

    <!-- Size of the backlog for blocks in parallel queue.

         Since the bottleneck in parallel loading is rehash, and so to avoid
         stalling because of thread is doing rehash, you need to have some
         backlog.

         10000 is good balance between memory and speed.
         Even for 10e10 elements and can handle all the load without starvation. -->
    <shard_load_queue_backlog>10000</shard_load_queue_backlog>

    <!-- Maximum load factor of the hash table, with greater values, the memory
         is utilized more efficiently (less memory is wasted) but read/performance
         may deteriorate.

         Valid values: [0.5, 0.99]
         Default: 0.5 -->
    <max_load_factor>0.5</max_load_factor>
  </hashed>
</layout>
```

</TabItem>
</Tabs>
<br/>

## sparse_hashed {#sparse_hashed}

Similar to `hashed`, but uses less memory in favor more CPU usage.

The dictionary key has the [UInt64](/sql-reference/data-types/int-uint.md) type.

Configuration example:

<Tabs>
<TabItem value="ddl" label="DDL" default>

```sql
LAYOUT(SPARSE_HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
```

</TabItem>
<TabItem value="xml" label="Configuration file">

```xml
<layout>
  <sparse_hashed>
    <!-- <shards>1</shards> -->
    <!-- <shard_load_queue_backlog>10000</shard_load_queue_backlog> -->
    <!-- <max_load_factor>0.5</max_load_factor> -->
  </sparse_hashed>
</layout>
```

</TabItem>
</Tabs>
<br/>

It is also possible to use `shards` for this type of dictionary, and again it is more important for `sparse_hashed` then for `hashed`, since `sparse_hashed` is slower.

## complex_key_hashed {#complex_key_hashed}

This type of storage is for use with composite [keys](../attributes.md#composite-key). Similar to `hashed`.

Configuration example:

<Tabs>
<TabItem value="ddl" label="DDL" default>

```sql
LAYOUT(COMPLEX_KEY_HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
```

</TabItem>
<TabItem value="xml" label="Configuration file">

```xml
<layout>
  <complex_key_hashed>
    <!-- <shards>1</shards> -->
    <!-- <shard_load_queue_backlog>10000</shard_load_queue_backlog> -->
    <!-- <max_load_factor>0.5</max_load_factor> -->
  </complex_key_hashed>
</layout>
```

</TabItem>
</Tabs>
<br/>

## complex_key_sparse_hashed {#complex_key_sparse_hashed}

This type of storage is for use with composite [keys](../attributes.md#composite-key). Similar to [sparse_hashed](#sparse_hashed).

Configuration example:

<Tabs>
<TabItem value="ddl" label="DDL" default>

```sql
LAYOUT(COMPLEX_KEY_SPARSE_HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
```

</TabItem>
<TabItem value="xml" label="Configuration file">

```xml
<layout>
  <complex_key_sparse_hashed>
    <!-- <shards>1</shards> -->
    <!-- <shard_load_queue_backlog>10000</shard_load_queue_backlog> -->
    <!-- <max_load_factor>0.5</max_load_factor> -->
  </complex_key_sparse_hashed>
</layout>
```

</TabItem>
</Tabs>
<br/>
