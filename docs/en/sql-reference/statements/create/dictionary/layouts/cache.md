---
slug: /sql-reference/statements/create/dictionary/layouts/cache
title: 'cache dictionary layout'
sidebar_label: 'cache'
sidebar_position: 6
description: 'Store a dictionary in a fixed-size in-memory cache.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

The `cached` dictionary layout type is stores the dictionary in a cache that has a fixed number of cells.
These cells contain frequently used elements.

The dictionary key has the [UInt64](../../../data-types/int-uint.md) type.

When searching for a dictionary, the cache is searched first. For each block of data, all keys that are not found in the cache or are outdated are requested from the source using `SELECT attrs... FROM db.table WHERE id IN (k1, k2, ...)`. The received data is then written to the cache.

If keys are not found in dictionary, then update cache task is created and added into update queue. Update queue properties can be controlled with settings `max_update_queue_size`, `update_queue_push_timeout_milliseconds`, `query_wait_timeout_milliseconds`, `max_threads_for_updates`.

For cache dictionaries, the expiration [lifetime](../lifetime.md#refreshing-dictionary-data-using-lifetime) of data in the cache can be set. If more time than `lifetime` has passed since loading the data in a cell, the cell's value is not used and key becomes expired. The key is re-requested the next time it needs to be used. This behaviour can be configured with setting `allow_read_expired_keys`.

This is the least effective of all the ways to store dictionaries. The speed of the cache depends strongly on correct settings and the usage scenario. A cache type dictionary performs well only when the hit rates are high enough (recommended 99% and higher). You can view the average hit rate in the [system.dictionaries](../../../../operations/system-tables/dictionaries.md) table.

If setting `allow_read_expired_keys` is set to 1, by default 0. Then dictionary can support asynchronous updates. If a client requests keys and all of them are in cache, but some of them are expired, then dictionary will return expired keys for a client and request them asynchronously from the source.

To improve cache performance, use a subquery with `LIMIT`, and call the function with the dictionary externally.

All types of sources are supported.

Example of settings:

<Tabs>
<TabItem value="ddl" label="DDL" default>

```sql
LAYOUT(CACHE(SIZE_IN_CELLS 1000000000))
```

</TabItem>
<TabItem value="xml" label="Configuration file">

```xml
<layout>
    <cache>
        <!-- The size of the cache, in number of cells. Rounded up to a power of two. -->
        <size_in_cells>1000000000</size_in_cells>
        <!-- Allows to read expired keys. -->
        <allow_read_expired_keys>0</allow_read_expired_keys>
        <!-- Max size of update queue. -->
        <max_update_queue_size>100000</max_update_queue_size>
        <!-- Max timeout in milliseconds for push update task into queue. -->
        <update_queue_push_timeout_milliseconds>10</update_queue_push_timeout_milliseconds>
        <!-- Max wait timeout in milliseconds for update task to complete. -->
        <query_wait_timeout_milliseconds>60000</query_wait_timeout_milliseconds>
        <!-- Max threads for cache dictionary update. -->
        <max_threads_for_updates>4</max_threads_for_updates>
    </cache>
</layout>
```

</TabItem>
</Tabs>
<br/>

Set a large enough cache size. You need to experiment to select the number of cells:

1.  Set some value.
2.  Run queries until the cache is completely full.
3.  Assess memory consumption using the `system.dictionaries` table.
4.  Increase or decrease the number of cells until the required memory consumption is reached.

:::note
ClickHouse is not recommended as a source for this layout. Dictionary lookups require random point reads, which are not the access pattern ClickHouse is optimized for.
:::
