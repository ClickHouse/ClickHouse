---
slug: /sql-reference/statements/create/dictionary/sources/redis
title: 'Redis dictionary source'
sidebar_position: 10
sidebar_label: 'Redis'
description: 'Configure Redis as a dictionary source in ClickHouse.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Example of settings:

<Tabs>
<TabItem value="ddl" label="DDL" default>

```sql
SOURCE(REDIS(
    host 'localhost'
    port 6379
    storage_type 'simple'
    db_index 0
))
```

</TabItem>
<TabItem value="xml" label="Configuration file">

```xml
<source>
    <redis>
        <host>localhost</host>
        <port>6379</port>
        <storage_type>simple</storage_type>
        <db_index>0</db_index>
    </redis>
</source>
```

</TabItem>
</Tabs>
<br/>

Setting fields:

| Setting | Description |
|---------|-------------|
| `host` | The Redis host. |
| `port` | The port on the Redis server. |
| `storage_type` | The structure of the internal Redis storage used to work with keys. `simple` uses a flat key-value map and supports simple-key layouts as well as single-column complex-key layouts (such as `complex_key_cache` and `complex_key_direct`). `hash_map` uses a Redis hash and is required for composite complex keys; it expects exactly two key columns. Key columns must be of integer or string type. Ranged layouts are unsupported. Default value is `simple`. Optional. |
| `db_index` | The specific numeric index of Redis logical database. Default value is `0`. Optional. |
