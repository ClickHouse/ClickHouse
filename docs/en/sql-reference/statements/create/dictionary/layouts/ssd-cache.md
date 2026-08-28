---
slug: /sql-reference/statements/create/dictionary/layouts/ssd-cache
title: 'ssd_cache dictionary layout types'
sidebar_label: 'ssd_cache'
sidebar_position: 8
description: 'Store dictionary data on SSD with an in-memory index: ssd_cache or complex_key_ssd_cache types'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## ssd_cache {#ssd_cache}

Similar to `cache`, but stores data on SSD and index in RAM. All cache dictionary settings related to update queue can also be applied to SSD cache dictionaries.

The dictionary key has the [UInt64](/sql-reference/data-types/int-uint.md) type.

<Tabs>
<TabItem value="ddl" label="DDL" default>

```sql
LAYOUT(SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 16777216 READ_BUFFER_SIZE 1048576
    PATH '/var/lib/clickhouse/user_files/test_dict'))
```

</TabItem>
<TabItem value="xml" label="Configuration file">

```xml
<layout>
    <ssd_cache>
        <!-- Size of elementary read block in bytes. Recommended to be equal to SSD's page size. -->
        <block_size>4096</block_size>
        <!-- Max cache file size in bytes. -->
        <file_size>16777216</file_size>
        <!-- Size of RAM buffer in bytes for reading elements from SSD. -->
        <read_buffer_size>131072</read_buffer_size>
        <!-- Size of RAM buffer in bytes for aggregating elements before flushing to SSD. -->
        <write_buffer_size>1048576</write_buffer_size>
        <!-- Path where cache file will be stored. -->
        <path>/var/lib/clickhouse/user_files/test_dict</path>
    </ssd_cache>
</layout>
```

</TabItem>
</Tabs>
<br/>

## complex_key_ssd_cache {#complex_key_ssd_cache}

This type of storage is for use with composite [keys](../attributes.md#composite-key). Similar to `ssd_cache`.
