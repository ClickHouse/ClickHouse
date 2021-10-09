---
toc_priority: 45
toc_title: USER
---

# ALTER USER {#alter-user-statement}

Changes ClickHouse user accounts.

Syntax:

``` sql
ALTER USER [IF EXISTS] name1 [ON CLUSTER cluster_name1] [RENAME TO new_name1]
        [, name2 [ON CLUSTER cluster_name2] [RENAME TO new_name2] ...]
    [NOT IDENTIFIED | IDENTIFIED {[WITH {no_password | plaintext_password | sha256_password | sha256_hash | double_sha1_password | double_sha1_hash}] BY {'password' | 'hash'}} | {WITH ldap SERVER 'server_name'} | {WITH kerberos [REALM 'realm']}]
    [[ADD | DROP] HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [DEFAULT ROLE role [,...] | ALL | ALL EXCEPT role [,...] ]
    [GRANTEES {user | role | ANY | NONE} [,...] [EXCEPT {user | role} [,...]]]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY | WRITABLE] | PROFILE 'profile_name'] [,...]
```

To use `ALTER USER` you must have the [ALTER USER](../../../sql-reference/statements/grant.md#grant-access-management) privilege.

## GRANTEES Clause {#grantees}

Specifies users or roles which are allowed to receive [privileges](../../../sql-reference/statements/grant.md#grant-privileges) from this user on the condition this user has also all required access granted with [GRANT OPTION](../../../sql-reference/statements/grant.md#grant-privigele-syntax). Options of the `GRANTEES` clause:

-   `user` — Specifies a user this user can grant privileges to.
-   `role` — Specifies a role this user can grant privileges to.
-   `ANY` — This user can grant privileges to anyone. It's the default setting.
-   `NONE` — This user can grant privileges to none.

You can exclude any user or role by using the `EXCEPT` expression. For example, `ALTER USER user1 GRANTEES ANY EXCEPT user2`. It means if `user1` has some privileges granted with `GRANT OPTION` it will be able to grant those privileges to anyone except `user2`.

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

Allows the user with `john` account to grant his privileges to the user with `jack` account:

``` sql
ALTER USER john GRANTEES jack;
```
