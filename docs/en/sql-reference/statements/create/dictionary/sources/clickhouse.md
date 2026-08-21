---
slug: /sql-reference/statements/create/dictionary/sources/clickhouse
title: 'ClickHouse dictionary source'
sidebar_position: 8
sidebar_label: 'ClickHouse'
description: 'Configure a ClickHouse table as a dictionary source.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Example of settings:

<Tabs>
<TabItem value="ddl" label="DDL" default>

```sql
SOURCE(CLICKHOUSE(
    host 'example01-01-1'
    port 9000
    user 'default'
    password ''
    db 'default'
    table 'ids'
    where 'id=10'
    secure 1
    query 'SELECT id, value_1, value_2 FROM default.ids'
));
```

</TabItem>
<TabItem value="xml" label="Configuration file">

```xml
<source>
    <clickhouse>
        <host>example01-01-1</host>
        <port>9000</port>
        <user>default</user>
        <password></password>
        <db>default</db>
        <table>ids</table>
        <where>id=10</where>
        <secure>1</secure>
        <query>SELECT id, value_1, value_2 FROM default.ids</query>
    </clickhouse>
</source>
```

</TabItem>
</Tabs>
<br/>

Setting fields:

| Setting | Description |
|---------|-------------|
| `host` | The ClickHouse host. If it is a local host, the query is processed without any network activity. To improve fault tolerance, you can create a [Distributed](/engines/table-engines/special/distributed) table and enter it in subsequent configurations. |
| `port` | The port on the ClickHouse server. |
| `user` | Name of the ClickHouse user. |
| `password` | Password of the ClickHouse user. |
| `db` | Name of the database. |
| `table` | Name of the table. |
| `where` | The selection criteria. Optional. |
| `invalidate_query` | Query for checking the dictionary status. Optional. Read more in the section [Refreshing dictionary data using LIFETIME](../lifetime.md#refreshing-dictionary-data-using-lifetime). |
| `secure` | Use SSL for connection. |
| `query` | The custom query. Optional. |

:::note
The `table` or `where` fields cannot be used together with the `query` field. And either one of the `table` or `query` fields must be declared.
:::
