---
slug: /sql-reference/statements/create/dictionary/sources/postgresql
title: 'PostgreSQL dictionary source'
sidebar_position: 12
sidebar_label: 'PostgreSQL'
description: 'Configure PostgreSQL as a dictionary source in ClickHouse.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Example of settings:

<Tabs>
<TabItem value="ddl" label="DDL" default>

```sql
SOURCE(POSTGRESQL(
    port 5432
    host 'postgresql-hostname'
    user 'postgres_user'
    password 'postgres_password'
    db 'db_name'
    table 'table_name'
    replica(host 'example01-1' port 5432 priority 1)
    replica(host 'example01-2' port 5432 priority 2)
    where 'id=10'
    invalidate_query 'SQL_QUERY'
    query 'SELECT id, value_1, value_2 FROM db_name.table_name'
))
```

</TabItem>
<TabItem value="xml" label="Configuration file">

```xml
<source>
  <postgresql>
      <host>postgresql-hostname</hoat>
      <port>5432</port>
      <user>clickhouse</user>
      <password>qwerty</password>
      <db>db_name</db>
      <table>table_name</table>
      <where>id=10</where>
      <invalidate_query>SQL_QUERY</invalidate_query>
      <query>SELECT id, value_1, value_2 FROM db_name.table_name</query>
  </postgresql>
</source>
```

</TabItem>
</Tabs>
<br/>

Setting fields:

| Setting | Description |
|---------|-------------|
| `host` | The host on the PostgreSQL server. You can specify it for all replicas, or for each one individually (inside `<replica>`). |
| `port` | The port on the PostgreSQL server. You can specify it for all replicas, or for each one individually (inside `<replica>`). |
| `user` | Name of the PostgreSQL user. You can specify it for all replicas, or for each one individually (inside `<replica>`). |
| `password` | Password of the PostgreSQL user. You can specify it for all replicas, or for each one individually (inside `<replica>`). |
| `replica` | Section of replica configurations. There can be multiple sections. |
| `replica/host` | The PostgreSQL host. |
| `replica/port` | The PostgreSQL port. |
| `replica/priority` | The replica priority. When attempting to connect, ClickHouse traverses the replicas in order of priority. The lower the number, the higher the priority. |
| `db` | Name of the database. |
| `table` | Name of the table. |
| `where` | The selection criteria. The syntax for conditions is the same as for `WHERE` clause in PostgreSQL. For example, `id > 10 AND id < 20`. Optional. |
| `invalidate_query` | Query for checking the dictionary status. Optional. Read more in the section [Refreshing dictionary data using LIFETIME](../lifetime.md#refreshing-dictionary-data-using-lifetime). |
| `background_reconnect` | Reconnect to replica in background if connection fails. Optional. |
| `query` | The custom query. Optional. |

:::note
The `table` or `where` fields cannot be used together with the `query` field. And either one of the `table` or `query` fields must be declared.
:::
