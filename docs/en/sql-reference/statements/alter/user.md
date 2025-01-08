---
slug: /en/sql-reference/statements/alter/user
sidebar_position: 45
sidebar_label: USER
title: "ALTER USER"
---

Changes ClickHouse user accounts.

Syntax:

``` sql
ALTER USER [IF EXISTS] name1 [RENAME TO new_name |, name2 [,...]] 
    [ON CLUSTER cluster_name]
    [NOT IDENTIFIED | RESET AUTHENTICATION METHODS TO NEW | {IDENTIFIED | ADD IDENTIFIED} {[WITH {plaintext_password | sha256_password | sha256_hash | double_sha1_password | double_sha1_hash}] BY {'password' | 'hash'}} | WITH NO_PASSWORD | {WITH ldap SERVER 'server_name'} | {WITH kerberos [REALM 'realm']} | {WITH ssl_certificate CN 'common_name' | SAN 'TYPE:subject_alt_name'} | {WITH ssh_key BY KEY 'public_key' TYPE 'ssh-rsa|...'} | {WITH http SERVER 'server_name' [SCHEME 'Basic']} 
    [, {[{plaintext_password | sha256_password | sha256_hash | ...}] BY {'password' | 'hash'}} | {ldap SERVER 'server_name'} | {...} | ... [,...]]]
    [[ADD | DROP] HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [VALID UNTIL datetime]
    [DEFAULT ROLE role [,...] | ALL | ALL EXCEPT role [,...] ]
    [GRANTEES {user | role | ANY | NONE} [,...] [EXCEPT {user | role} [,...]]]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY | WRITABLE] | PROFILE 'profile_name'] [,...]
```

To use `ALTER USER` you must have the [ALTER USER](../../../sql-reference/statements/grant.md#access-management) privilege.

## GRANTEES Clause

Specifies users or roles which are allowed to receive [privileges](../../../sql-reference/statements/grant.md#privileges) from this user on the condition this user has also all required access granted with [GRANT OPTION](../../../sql-reference/statements/grant.md#granting-privilege-syntax). Options of the `GRANTEES` clause:

- `user` — Specifies a user this user can grant privileges to.
- `role` — Specifies a role this user can grant privileges to.
- `ANY` — This user can grant privileges to anyone. It's the default setting.
- `NONE` — This user can grant privileges to none.

You can exclude any user or role by using the `EXCEPT` expression. For example, `ALTER USER user1 GRANTEES ANY EXCEPT user2`. It means if `user1` has some privileges granted with `GRANT OPTION` it will be able to grant those privileges to anyone except `user2`.

## Examples

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

Adds new authentication methods to the user while keeping the existing ones:

``` sql
ALTER USER user1 ADD IDENTIFIED WITH plaintext_password by '1', bcrypt_password by '2', plaintext_password by '3'
```

Notes:
1. Older versions of ClickHouse might not support the syntax of multiple authentication methods. Therefore, if the ClickHouse server contains such users and is downgraded to a version that does not support it, such users will become unusable and some user related operations will be broken. In order to downgrade gracefully, one must set all users to contain a single authentication method prior to downgrading. Alternatively, if the server was downgraded without the proper procedure, the faulty users should be dropped.
2. `no_password` can not co-exist with other authentication methods for security reasons.
Because of that, it is not possible to `ADD` a `no_password` authentication method. The below query will throw an error:

``` sql
ALTER USER user1 ADD IDENTIFIED WITH no_password
```

If you want to drop authentication methods for a user and rely on `no_password`, you must specify in the below replacing form.

Reset authentication methods and adds the ones specified in the query (effect of leading IDENTIFIED without the ADD keyword):

``` sql
ALTER USER user1 IDENTIFIED WITH plaintext_password by '1', bcrypt_password by '2', plaintext_password by '3'
```

Reset authentication methods and keep the most recent added one:
``` sql
ALTER USER user1 RESET AUTHENTICATION METHODS TO NEW
```
