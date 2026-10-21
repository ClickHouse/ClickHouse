---
slug: /sql-reference/statements/create/dictionary/sources/cassandra
title: 'Cassandra dictionary source'
sidebar_position: 11
sidebar_label: 'Cassandra'
description: 'Configure Cassandra as a dictionary source in ClickHouse.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Example of settings:

<Tabs>
<TabItem value="ddl" label="DDL" default>

```sql
SOURCE(CASSANDRA(
    host 'localhost'
    port 9042
    user 'username'
    password 'qwerty123'
    keyspace 'database_name'
    column_family 'table_name'
    allow_filtering 1
    partition_key_prefix 1
    consistency 'One'
    where '"SomeColumn" = 42'
    max_threads 8
    query 'SELECT id, value_1, value_2 FROM database_name.table_name'
))
```

</TabItem>
<TabItem value="xml" label="Configuration file">

```xml
<source>
    <cassandra>
        <host>localhost</host>
        <port>9042</port>
        <user>username</user>
        <password>qwerty123</password>
        <keyspase>database_name</keyspase>
        <column_family>table_name</column_family>
        <allow_filtering>1</allow_filtering>
        <partition_key_prefix>1</partition_key_prefix>
        <consistency>One</consistency>
        <where>"SomeColumn" = 42</where>
        <max_threads>8</max_threads>
        <query>SELECT id, value_1, value_2 FROM database_name.table_name</query>
    </cassandra>
</source>
```

</TabItem>
</Tabs>

Setting fields:

| Setting | Description |
|---------|-------------|
| `host` | The Cassandra host or comma-separated list of hosts. |
| `port` | The port on the Cassandra servers. If not specified, default port `9042` is used. |
| `user` | Name of the Cassandra user. |
| `password` | Password of the Cassandra user. |
| `keyspace` | Name of the keyspace (database). |
| `column_family` | Name of the column family (table). |
| `allow_filtering` | Flag to allow or not potentially expensive conditions on clustering key columns. Default value is `1`. |
| `partition_key_prefix` | Number of partition key columns in primary key of the Cassandra table. Required for compose key dictionaries. Order of key columns in the dictionary definition must be the same as in Cassandra. Default value is `1` (the first key column is a partition key and other key columns are clustering key). |
| `consistency` | Consistency level. Possible values: `One`, `Two`, `Three`, `All`, `EachQuorum`, `Quorum`, `LocalQuorum`, `LocalOne`, `Serial`, `LocalSerial`. Default value is `One`. |
| `where` | Optional selection criteria. |
| `max_threads` | The maximum number of threads to use for loading data from multiple partitions in compose key dictionaries. |
| `query` | The custom query. Optional. |

:::note
The `column_family` or `where` fields cannot be used together with the `query` field. And either one of the `column_family` or `query` fields must be declared.
:::
