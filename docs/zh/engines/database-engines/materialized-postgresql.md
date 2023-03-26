---
toc_priority: 30
toc_title: MaterializedPostgreSQL
---

# [experimental] MaterializedPostgreSQL {#materialize-postgresql}

使用PostgreSQL数据库表的初始数据转储创建ClickHouse数据库，并启动复制过程，即执行后台作业，以便在远程PostgreSQL数据库中的PostgreSQL数据库表上发生新更改时应用这些更改。

ClickHouse服务器作为PostgreSQL副本工作。它读取WAL并执行DML查询。DDL不是复制的，但可以处理（如下所述）。

## 创建数据库 {#creating-a-database}

``` sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster]
ENGINE = MaterializedPostgreSQL('host:port', ['database' | database], 'user', 'password') [SETTINGS ...]
```

**Engine参数**

-   `host:port` — PostgreSQL服务地址
-   `database` — PostgreSQL数据库名
-   `user` — PostgreSQL用户名
-   `password` — 用户密码

## 设置 {#settings}

-   [materialized_postgresql_max_block_size](../../operations/settings/settings.md#materialized-postgresql-max-block-size)

-   [materialized_postgresql_tables_list](../../operations/settings/settings.md#materialized-postgresql-tables-list)

-   [materialized_postgresql_allow_automatic_update](../../operations/settings/settings.md#materialized-postgresql-allow-automatic-update)

``` sql
CREATE DATABASE database1
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
SETTINGS materialized_postgresql_max_block_size = 65536,
         materialized_postgresql_tables_list = 'table1,table2,table3';

SELECT * FROM database1.table1;
```

## 必备条件 {#requirements}

- 在postgresql配置文件中将[wal_level](https://www.postgresql.org/docs/current/runtime-config-wal.html)设置为`logical`，将`max_replication_slots`设置为`2`。

- 每个复制表必须具有以下一个[replica identity](https://www.postgresql.org/docs/10/sql-altertable.html#SQL-CREATETABLE-REPLICA-IDENTITY):

1. **default** (主键)

2. **index**

``` bash
postgres# CREATE TABLE postgres_table (a Integer NOT NULL, b Integer, c Integer NOT NULL, d Integer, e Integer NOT NULL);
postgres# CREATE unique INDEX postgres_table_index on postgres_table(a, c, e);
postgres# ALTER TABLE postgres_table REPLICA IDENTITY USING INDEX postgres_table_index;
```

总是先检查主键。如果不存在，则检查索引(定义为副本标识索引)。
如果使用index作为副本标识，则表中必须只有一个这样的索引。
你可以用下面的命令来检查一个特定的表使用了什么类型:

``` bash
postgres# SELECT CASE relreplident
          WHEN 'd' THEN 'default'
          WHEN 'n' THEN 'nothing'
          WHEN 'f' THEN 'full'
          WHEN 'i' THEN 'index'
       END AS replica_identity
FROM pg_class
WHERE oid = 'postgres_table'::regclass;
```

## 注意 {#warning}

1. [**TOAST**](https://www.postgresql.org/docs/9.5/storage-toast.html)不支持值转换。将使用数据类型的默认值。

## 使用示例 {#example-of-use}

``` sql
CREATE DATABASE postgresql_db
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password');

SELECT * FROM postgresql_db.postgres_table;
```
