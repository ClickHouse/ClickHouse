---
slug: /sql-reference/statements/create/dictionary/sources/mongodb
title: 'MongoDB dictionary source'
sidebar_position: 9
sidebar_label: 'MongoDB'
description: 'Configure MongoDB as a dictionary source in ClickHouse.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Example of settings:

<Tabs>
<TabItem value="ddl" label="DDL" default>

```sql
SOURCE(MONGODB(
    host 'localhost'
    port 27017
    user ''
    password ''
    db 'test'
    collection 'dictionary_source'
    options 'ssl=true'
))
```

Or using a URI:

```sql
SOURCE(MONGODB(
    uri 'mongodb://localhost:27017/clickhouse'
    collection 'dictionary_source'
))
```

</TabItem>
<TabItem value="xml" label="Configuration file">

```xml
<source>
    <mongodb>
        <host>localhost</host>
        <port>27017</port>
        <user></user>
        <password></password>
        <db>test</db>
        <collection>dictionary_source</collection>
        <options>ssl=true</options>
    </mongodb>
</source>
```

Or using a URI:

```xml
<source>
    <mongodb>
        <uri>mongodb://localhost:27017/test?ssl=true</uri>
        <collection>dictionary_source</collection>
    </mongodb>
</source>
```

</TabItem>
</Tabs>
<br/>

Setting fields:

| Setting | Description |
|---------|-------------|
| `host` | The MongoDB host. |
| `port` | The port on the MongoDB server. |
| `user` | Name of the MongoDB user. |
| `password` | Password of the MongoDB user. |
| `db` | Name of the database. |
| `collection` | Name of the collection. |
| `options` | MongoDB connection string options. Optional. |
| `uri` | URI for establishing the connection (alternative to individual host/port/db fields). |

[More information about the engine](/engines/table-engines/integrations/mongodb)
