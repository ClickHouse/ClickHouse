---
toc_priority: 30
toc_title: MaterializedPostgreSQL
---

# MaterializedPostgreSQL {#materialize-postgresql}

## 创建数据库 {#creating-a-database}

``` sql
CREATE DATABASE test_database
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password'

SELECT * FROM test_database.postgres_table;
```

## 设置 {#settings}

1. `materialized_postgresql_max_block_size` - 在将数据刷新到表中之前收集的行数。默认值: `65536`.

2. `materialized_postgresql_tables_list` - 物化PostgreSQL数据库引擎的表列表。默认： `whole database`.

3. `materialized_postgresql_allow_automatic_update` - 当检测到模式更改时，允许在后台重新加载表。默认值: `0` (`false`).

``` sql
CREATE DATABASE test_database
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password'
SETTINGS materialized_postgresql_max_block_size = 65536,
         materialized_postgresql_tables_list = 'table1,table2,table3';

SELECT * FROM test_database.table1;
```

## 必备条件 {#requirements}

- 在postgresql配置文件中将`wal_level`设置为`logical`，将`max_replication_slots`设置为`2`。

- 每个复制表必须具有以下一个**replica identity**:

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

1. **TOAST** 不支持值转换。将使用数据类型的默认值。
   