---
toc_priority: 45
toc_title: USER
---

# ALTER USER {#alter-user-statement}

Changes ClickHouse user accounts.

Syntax:

``` sql
ALTER USER [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [IDENTIFIED [WITH {PLAINTEXT_PASSWORD|SHA256_PASSWORD|DOUBLE_SHA1_PASSWORD}] BY {'password'|'hash'}]
    [[ADD|DROP] HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [DEFAULT ROLE role [,...] | ALL | ALL EXCEPT role [,...] ]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
```

To use `ALTER USER` you must have the [ALTER USER](../../../sql-reference/statements/grant.md#grant-access-management) privilege.

## Examples {#alter-user-examples}

Set assigned roles as default:

``` sql
ALTER USER user DEFAULT ROLE role1, role2
```

If roles aren’t previously assigned to a user, ClickHouse throws an exception.

Set all the assigned roles to default:

``` sql
ALTER USER user DEFAULT ROLE ALL
```

If a role is assigned to a user in the future, it will become default automatically.

Set all the assigned roles to default, excepting `role1` and `role2`:

``` sql
ALTER USER user DEFAULT ROLE ALL EXCEPT role1, role2
```
