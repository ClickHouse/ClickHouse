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

Specifies users or roles which are allowed to receive [grants](../../../sql-reference/statements/grant.md) from this user on the condition this user has also all required access granted with grant option. Values of the `GRANTEES` clause:

-   `user` — Specifies user which is allowed to receive grants from this user.
-   `role` — Specifies role which is allowed to receive grants from this user.
-   `ANY` — User with grant option can grant to anyone, used by default.
-   `NONE` — User with grant option provides to nobody.

You can exclude any user or role by using the `EXCEPT` expression. For example, `ALTER USER user1 GRANTEES ANY EXCEPT user2`.

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

Assigns grants from a user with a `john` account to a user with a `jack` account:

``` sql
ALTER USER john GRANTEES jack;
```
