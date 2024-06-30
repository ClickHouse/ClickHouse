---
slug: /en/sql-reference/statements/drop
sidebar_position: 44
sidebar_label: DROP
---

# DROP Statements

Deletes existing entity. If the `IF EXISTS` clause is specified, these queries do not return an error if the entity does not exist. If the `SYNC` modifier is specified, the entity is dropped without delay.

## DROP DATABASE

Deletes all tables inside the `db` database, then deletes the `db` database itself.

Syntax:

``` sql
DROP DATABASE [IF EXISTS] db [ON CLUSTER cluster] [SYNC]
```

## DROP TABLE

Deletes one or more tables.

:::tip
To undo the deletion of a table, please see [UNDROP TABLE](/docs/en/sql-reference/statements/undrop.md)
:::

Syntax:

``` sql
DROP [TEMPORARY] TABLE [IF EXISTS] [IF EMPTY]  [db1.]name_1[, [db2.]name_2, ...] [ON CLUSTER cluster] [SYNC]
```

Limitations:
- If the clause `IF EMPTY` is specified, the server checks the emptiness of the table only on the replica which received the query.  
- Deleting multiple tables at once is not an atomic operation, i.e. if the deletion of a table fails, subsequent tables will not be deleted.

## DROP DICTIONARY

Deletes the dictionary.

Syntax:

``` sql
DROP DICTIONARY [IF EXISTS] [db.]name [SYNC]
```

## DROP USER

Deletes a user.

Syntax:

``` sql
DROP USER [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

## DROP ROLE

Deletes a role. The deleted role is revoked from all the entities where it was assigned.

Syntax:

``` sql
DROP ROLE [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

## DROP ROW POLICY

Deletes a row policy. Deleted row policy is revoked from all the entities where it was assigned.

Syntax:

``` sql
DROP [ROW] POLICY [IF EXISTS] name [,...] ON [database.]table [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

## DROP QUOTA

Deletes a quota. The deleted quota is revoked from all the entities where it was assigned.

Syntax:

``` sql
DROP QUOTA [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

## DROP SETTINGS PROFILE

Deletes a settings profile. The deleted settings profile is revoked from all the entities where it was assigned.

Syntax:

``` sql
DROP [SETTINGS] PROFILE [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

## DROP VIEW

Deletes a view. Views can be deleted by a `DROP TABLE` command as well but `DROP VIEW` checks that `[db.]name` is a view.

Syntax:

``` sql
DROP VIEW [IF EXISTS] [db.]name [ON CLUSTER cluster] [SYNC]
```

## DROP FUNCTION

Deletes a user defined function created by [CREATE FUNCTION](./create/function.md).
System functions can not be dropped.

**Syntax**

``` sql
DROP FUNCTION [IF EXISTS] function_name [on CLUSTER cluster]
```

**Example**

``` sql
CREATE FUNCTION linear_equation AS (x, k, b) -> k*x + b;
DROP FUNCTION linear_equation;
```

## DROP NAMED COLLECTION

Deletes a named collection.

**Syntax**

``` sql
DROP NAMED COLLECTION [IF EXISTS] name [on CLUSTER cluster]
```

**Example**

``` sql
CREATE NAMED COLLECTION foobar AS a = '1', b = '2';
DROP NAMED COLLECTION foobar;
```
