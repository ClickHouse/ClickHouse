---
sidebar_position: 44
sidebar_label: DROP
---

# DROP语法 {#drop}

删除现有实体。 如果指定了`IF EXISTS`子句，如果实体不存在，这些查询不会返回错误。

## DROP DATABASE {#drop-database}

删除`db`数据库中的所有表，然后删除`db`数据库本身。

语法:

``` sql
DROP DATABASE [IF EXISTS] db [ON CLUSTER cluster]
```

## DROP TABLE {#drop-table}

删除数据表

语法:

``` sql
DROP [TEMPORARY] TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

## DROP DICTIONARY {#drop-dictionary}

删除字典。

语法:

``` sql
DROP DICTIONARY [IF EXISTS] [db.]name
```

## DROP USER {#drop-user-statement}

删除用户.

语法:

``` sql
DROP USER [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```

## DROP ROLE {#drop-role-statement}

删除角色。删除的角色将从分配给它的所有实体中撤消。

语法:

``` sql
DROP ROLE [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```

## DROP ROW POLICY {#drop-row-policy-statement}

删除行策略。 删除的行策略从分配到它的所有实体中撤消。

语法:

``` sql
DROP [ROW] POLICY [IF EXISTS] name [,...] ON [database.]table [,...] [ON CLUSTER cluster_name]
```

## DROP QUOTA {#drop-quota-statement}

Deletes a quota. The deleted quota is revoked from all the entities where it was assigned.

语法:

``` sql
DROP QUOTA [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```

## DROP SETTINGS PROFILE {#drop-settings-profile-statement}

删除配置文件。 已删除的设置配置文件将从分配给它的所有实体中撤销。

语法:

``` sql
DROP [SETTINGS] PROFILE [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```

## DROP VIEW {#drop-view}

删除视图。 视图也可以通过`DROP TABLE`命令删除，但`DROP VIEW`会检查`[db.]name`是否是一个视图。

语法:

``` sql
DROP VIEW [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

[原始文章](https://clickhouse.com/docs/zh/sql-reference/statements/drop/) <!--hide-->
