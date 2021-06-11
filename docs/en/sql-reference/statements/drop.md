---
toc_priority: 46
toc_title: DROP
---

# DROP Statements {#drop}

Deletes existing entity. If `IF EXISTS` clause is specified, these queries doesn’t return an error if the entity doesn’t exist.

## DROP DATABASE {#drop-database}

``` sql
DROP DATABASE [IF EXISTS] db [ON CLUSTER cluster]
```

Deletes all tables inside the `db` database, then deletes the ‘db’ database itself.

## DROP TABLE {#drop-table}

``` sql
DROP [TEMPORARY] TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

Deletes the table.

## DROP DICTIONARY {#drop-dictionary}

``` sql
DROP DICTIONARY [IF EXISTS] [db.]name
```

Deletes the dictionary.

## DROP USER {#drop-user-statement}

``` sql
DROP USER [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```

Deletes a user.

## DROP ROLE {#drop-role-statement}

``` sql
DROP ROLE [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```

Deletes a role.

Deleted role is revoked from all the entities where it was assigned.

## DROP ROW POLICY {#drop-row-policy-statement}

``` sql
DROP [ROW] POLICY [IF EXISTS] name [,...] ON [database.]table [,...] [ON CLUSTER cluster_name]
```

Deletes a row policy.

Deleted row policy is revoked from all the entities where it was assigned.

## DROP QUOTA {#drop-quota-statement}

``` sql
DROP QUOTA [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```

Deletes a quota.

Deleted quota is revoked from all the entities where it was assigned.

## DROP SETTINGS PROFILE {#drop-settings-profile-statement}

``` sql
DROP [SETTINGS] PROFILE [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```

Deletes a settings profile.

Deleted settings profile is revoked from all the entities where it was assigned.

## DROP VIEW {#drop-view}

``` sql
DROP VIEW [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

Deletes a view. Views can be deleted by a `DROP TABLE` command as well but `DROP VIEW` checks that `[db.]name` is a view.
