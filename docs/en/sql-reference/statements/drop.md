---
sidebar_position: 44
sidebar_label: DROP
---

# DROP Statements

Deletes existing entity. If the `IF EXISTS` clause is specified, these queries do not return an error if the entity does not exist.

## DROP DATABASE

Deletes all tables inside the `db` database, then deletes the `db` database itself.

Syntax:

``` sql
DROP DATABASE [IF EXISTS] db [ON CLUSTER cluster]
```

## DROP TABLE

Deletes the table.

Syntax:

``` sql
DROP [TEMPORARY] TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

## DROP DICTIONARY

Deletes the dictionary.

Syntax:

``` sql
DROP DICTIONARY [IF EXISTS] [db.]name
```

## DROP USER

Deletes a user.

Syntax:

``` sql
DROP USER [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```

## DROP ROLE

Deletes a role. The deleted role is revoked from all the entities where it was assigned.

Syntax:

``` sql
DROP ROLE [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```

## DROP ROW POLICY

Deletes a row policy. Deleted row policy is revoked from all the entities where it was assigned.

Syntax:

``` sql
DROP [ROW] POLICY [IF EXISTS] name [,...] ON [database.]table [,...] [ON CLUSTER cluster_name]
```

## DROP QUOTA

Deletes a quota. The deleted quota is revoked from all the entities where it was assigned.

Syntax:

``` sql
DROP QUOTA [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```

## DROP SETTINGS PROFILE

Deletes a settings profile. The deleted settings profile is revoked from all the entities where it was assigned.

Syntax:

``` sql
DROP [SETTINGS] PROFILE [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```

## DROP VIEW

Deletes a view. Views can be deleted by a `DROP TABLE` command as well but `DROP VIEW` checks that `[db.]name` is a view.

Syntax:

``` sql
DROP VIEW [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

## DROP FUNCTION

Deletes a user defined function created by [CREATE FUNCTION](./create/function.md).
System functions can not be dropped.

**Syntax**

``` sql
DROP FUNCTION [IF EXISTS] function_name
```

**Example**

``` sql
CREATE FUNCTION linear_equation AS (x, k, b) -> k*x + b;
DROP FUNCTION linear_equation;
```
