---
slug: /sql-reference/statements/create/dictionary/sources/mysql
title: 'MySQL dictionary source'
sidebar_position: 7
sidebar_label: 'MySQL'
description: 'Configure MySQL as a dictionary source in ClickHouse.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Example of settings:

<Tabs>
<TabItem value="ddl" label="DDL" default>

```sql
SOURCE(MYSQL(
    port 3306
    user 'clickhouse'
    password 'qwerty'
    replica(host 'example01-1' priority 1)
    replica(host 'example01-2' priority 1)
    db 'db_name'
    table 'table_name'
    where 'id=10'
    invalidate_query 'SQL_QUERY'
    fail_on_connection_loss 'true'
    query 'SELECT id, value_1, value_2 FROM db_name.table_name'
))
```

</TabItem>
<TabItem value="xml" label="Configuration file">

```xml
<source>
  <mysql>
      <port>3306</port>
      <user>clickhouse</user>
      <password>qwerty</password>
      <replica>
          <host>example01-1</host>
          <priority>1</priority>
      </replica>
      <replica>
          <host>example01-2</host>
          <priority>1</priority>
      </replica>
      <db>db_name</db>
      <table>table_name</table>
      <where>id=10</where>
      <invalidate_query>SQL_QUERY</invalidate_query>
      <fail_on_connection_loss>true</fail_on_connection_loss>
      <query>SELECT id, value_1, value_2 FROM db_name.table_name</query>
  </mysql>
</source>
```

</TabItem>
</Tabs>
<br/>

Setting fields:

| Setting | Description |
|---------|-------------|
| `port` | The port on the MySQL server. You can specify it for all replicas, or for each one individually (inside `<replica>`). |
| `user` | Name of the MySQL user. You can specify it for all replicas, or for each one individually (inside `<replica>`). |
| `password` | Password of the MySQL user. You can specify it for all replicas, or for each one individually (inside `<replica>`). |
| `replica` | Section of replica configurations. There can be multiple sections. |
| `replica/host` | The MySQL host. |
| `replica/priority` | The replica priority. When attempting to connect, ClickHouse traverses the replicas in order of priority. The lower the number, the higher the priority. |
| `db` | Name of the database. |
| `table` | Name of the table. |
| `where` | The selection criteria. The syntax for conditions is the same as for `WHERE` clause in MySQL, for example, `id > 10 AND id < 20`. Optional. |
| `invalidate_query` | Query for checking the dictionary status. Optional. Read more in the section [Refreshing dictionary data using LIFETIME](../lifetime.md#refreshing-dictionary-data-using-lifetime). |
| `fail_on_connection_loss` | Controls behavior of the server on connection loss. If `true`, an exception is thrown immediately if the connection between client and server was lost. If `false`, the ClickHouse server retries to execute the query three times before throwing an exception. Note that retrying leads to increased response times. Default value: `false`. |
| `query` | The custom query. Optional. |

:::note
The `table` or `where` fields cannot be used together with the `query` field. And either one of the `table` or `query` fields must be declared.
:::

:::note
There is no explicit parameter `secure`. When establishing an SSL-connection security is mandatory.
:::

MySQL can be connected to on a local host via sockets. To do this, set `host` and `socket`.

Example of settings:

<Tabs>
<TabItem value="ddl" label="DDL" default>

```sql
SOURCE(MYSQL(
    host 'localhost'
    socket '/path/to/socket/file.sock'
    user 'clickhouse'
    password 'qwerty'
    db 'db_name'
    table 'table_name'
    where 'id=10'
    invalidate_query 'SQL_QUERY'
    fail_on_connection_loss 'true'
    query 'SELECT id, value_1, value_2 FROM db_name.table_name'
))
```

</TabItem>
<TabItem value="xml" label="Configuration file">

```xml
<source>
  <mysql>
      <host>localhost</host>
      <socket>/path/to/socket/file.sock</socket>
      <user>clickhouse</user>
      <password>qwerty</password>
      <db>db_name</db>
      <table>table_name</table>
      <where>id=10</where>
      <invalidate_query>SQL_QUERY</invalidate_query>
      <fail_on_connection_loss>true</fail_on_connection_loss>
      <query>SELECT id, value_1, value_2 FROM db_name.table_name</query>
  </mysql>
</source>
```

</TabItem>
</Tabs>
