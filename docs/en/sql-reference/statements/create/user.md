---
sidebar_position: 39
sidebar_label: USER
---

# CREATE USER

Creates [user accounts](../../../operations/access-rights.md#user-account-management).

Syntax:

``` sql
CREATE USER [IF NOT EXISTS | OR REPLACE] name1 [ON CLUSTER cluster_name1]
        [, name2 [ON CLUSTER cluster_name2] ...]
    [NOT IDENTIFIED | IDENTIFIED {[WITH {no_password | plaintext_password | sha256_password | sha256_hash | double_sha1_password | double_sha1_hash}] BY {'password' | 'hash'}} | {WITH ldap SERVER 'server_name'} | {WITH kerberos [REALM 'realm']}]
    [HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [DEFAULT ROLE role [,...]]
    [DEFAULT DATABASE database | NONE]
    [GRANTEES {user | role | ANY | NONE} [,...] [EXCEPT {user | role} [,...]]]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY | WRITABLE] | PROFILE 'profile_name'] [,...]
```

`ON CLUSTER` clause allows creating users on a cluster, see [Distributed DDL](../../../sql-reference/distributed-ddl.md).

## Identification

There are multiple ways of user identification:

-   `IDENTIFIED WITH no_password`
-   `IDENTIFIED WITH plaintext_password BY 'qwerty'`
-   `IDENTIFIED WITH sha256_password BY 'qwerty'` or `IDENTIFIED BY 'password'`
-   `IDENTIFIED WITH sha256_hash BY 'hash'` or `IDENTIFIED WITH sha256_hash BY 'hash' SALT 'salt'`
-   `IDENTIFIED WITH double_sha1_password BY 'qwerty'`
-   `IDENTIFIED WITH double_sha1_hash BY 'hash'`
-   `IDENTIFIED WITH ldap SERVER 'server_name'`
-   `IDENTIFIED WITH kerberos` or `IDENTIFIED WITH kerberos REALM 'realm'`

For identification with sha256_hash using `SALT` - hash must be calculated from concatination of 'password' and 'salt'.

## User Host

User host is a host from which a connection to ClickHouse server could be established. The host can be specified in the `HOST` query section in the following ways:

-   `HOST IP 'ip_address_or_subnetwork'` — User can connect to ClickHouse server only from the specified IP address or a [subnetwork](https://en.wikipedia.org/wiki/Subnetwork). Examples: `HOST IP '192.168.0.0/16'`, `HOST IP '2001:DB8::/32'`. For use in production, only specify `HOST IP` elements (IP addresses and their masks), since using `host` and `host_regexp` might cause extra latency.
-   `HOST ANY` — User can connect from any location. This is a default option.
-   `HOST LOCAL` — User can connect only locally.
-   `HOST NAME 'fqdn'` — User host can be specified as FQDN. For example, `HOST NAME 'mysite.com'`.
-   `HOST REGEXP 'regexp'` — You can use [pcre](http://www.pcre.org/) regular expressions when specifying user hosts. For example, `HOST REGEXP '.*\.mysite\.com'`.
-   `HOST LIKE 'template'` — Allows you to use the [LIKE](../../../sql-reference/functions/string-search-functions.md#function-like) operator to filter the user hosts. For example, `HOST LIKE '%'` is equivalent to `HOST ANY`, `HOST LIKE '%.mysite.com'` filters all the hosts in the `mysite.com` domain.

Another way of specifying host is to use `@` syntax following the username. Examples:

-   `CREATE USER mira@'127.0.0.1'` — Equivalent to the `HOST IP` syntax.
-   `CREATE USER mira@'localhost'` — Equivalent to the `HOST LOCAL` syntax.
-   `CREATE USER mira@'192.168.%.%'` — Equivalent to the `HOST LIKE` syntax.

:::warning    
ClickHouse treats `user_name@'address'` as a username as a whole. Thus, technically you can create multiple users with the same `user_name` and different constructions after `@`. However, we do not recommend to do so.
:::

## GRANTEES Clause

Specifies users or roles which are allowed to receive [privileges](../../../sql-reference/statements/grant.md#grant-privileges) from this user on the condition this user has also all required access granted with [GRANT OPTION](../../../sql-reference/statements/grant.md#grant-privigele-syntax). Options of the `GRANTEES` clause:

-   `user` — Specifies a user this user can grant privileges to.
-   `role` — Specifies a role this user can grant privileges to.
-   `ANY` — This user can grant privileges to anyone. It's the default setting.
-   `NONE` — This user can grant privileges to none.

You can exclude any user or role by using the `EXCEPT` expression. For example, `CREATE USER user1 GRANTEES ANY EXCEPT user2`. It means if `user1` has some privileges granted with `GRANT OPTION` it will be able to grant those privileges to anyone except `user2`.

## Examples

Create the user account `mira` protected by the password `qwerty`:

``` sql
CREATE USER mira HOST IP '127.0.0.1' IDENTIFIED WITH sha256_password BY 'qwerty';
```

`mira` should start client app at the host where the ClickHouse server runs.

Create the user account `john`, assign roles to it and make this roles default:

``` sql
CREATE USER john DEFAULT ROLE role1, role2;
```

Create the user account `john` and make all his future roles default:

``` sql
CREATE USER john DEFAULT ROLE ALL;
```

When some role is assigned to `john` in the future, it will become default automatically.

Create the user account `john` and make all his future roles default excepting `role1` and `role2`:

``` sql
CREATE USER john DEFAULT ROLE ALL EXCEPT role1, role2;
```

Create the user account `john` and allow him to grant his privileges to the user with `jack` account:

``` sql
CREATE USER john GRANTEES jack;
```
