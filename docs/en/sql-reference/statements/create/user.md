---
toc_priority: 5
toc_title: USER
---

# CREATE USER {#create-user-statement}

Creates a [user account](../../../operations/access-rights.md#user-account-management).

Syntax:

``` sql
CREATE USER [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [IDENTIFIED [WITH {NO_PASSWORD|PLAINTEXT_PASSWORD|SHA256_PASSWORD|SHA256_HASH|DOUBLE_SHA1_PASSWORD|DOUBLE_SHA1_HASH}] BY {'password'|'hash'}]
    [HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [DEFAULT ROLE role [,...]]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
```

`ON CLUSTER` clause allows creating users on a cluster, see [Distributed DDL](../../../sql-reference/distributed-ddl.md).

## Identification {#identification}

There are multiple ways of user identification:

-   `IDENTIFIED WITH no_password`
-   `IDENTIFIED WITH plaintext_password BY 'qwerty'`
-   `IDENTIFIED WITH sha256_password BY 'qwerty'` or `IDENTIFIED BY 'password'`
-   `IDENTIFIED WITH sha256_hash BY 'hash'`
-   `IDENTIFIED WITH double_sha1_password BY 'qwerty'`
-   `IDENTIFIED WITH double_sha1_hash BY 'hash'`

## User Host {#user-host}

User host is a host from which a connection to ClickHouse server could be established. The host can be specified in the `HOST` query section in the following ways:

-   `HOST IP 'ip_address_or_subnetwork'` — User can connect to ClickHouse server only from the specified IP address or a [subnetwork](https://en.wikipedia.org/wiki/Subnetwork). Examples: `HOST IP '192.168.0.0/16'`, `HOST IP '2001:DB8::/32'`. For use in production, only specify `HOST IP` elements (IP addresses and their masks), since using `host` and `host_regexp` might cause extra latency.
-   `HOST ANY` — User can connect from any location. This is a default option.
-   `HOST LOCAL` — User can connect only locally.
-   `HOST NAME 'fqdn'` — User host can be specified as FQDN. For example, `HOST NAME 'mysite.com'`.
-   `HOST NAME REGEXP 'regexp'` — You can use [pcre](http://www.pcre.org/) regular expressions when specifying user hosts. For example, `HOST NAME REGEXP '.*\.mysite\.com'`.
-   `HOST LIKE 'template'` — Allows you to use the [LIKE](../../../sql-reference/functions/string-search-functions.md#function-like) operator to filter the user hosts. For example, `HOST LIKE '%'` is equivalent to `HOST ANY`, `HOST LIKE '%.mysite.com'` filters all the hosts in the `mysite.com` domain.

Another way of specifying host is to use `@` syntax following the username. Examples:

-   `CREATE USER mira@'127.0.0.1'` — Equivalent to the `HOST IP` syntax.
-   `CREATE USER mira@'localhost'` — Equivalent to the `HOST LOCAL` syntax.
-   `CREATE USER mira@'192.168.%.%'` — Equivalent to the `HOST LIKE` syntax.

!!! info "Warning"
    ClickHouse treats `user_name@'address'` as a username as a whole. Thus, technically you can create multiple users with the same `user_name` and different constructions after `@`. However, we don’t recommend to do so.

## Examples {#create-user-examples}

Create the user account `mira` protected by the password `qwerty`:

``` sql
CREATE USER mira HOST IP '127.0.0.1' IDENTIFIED WITH sha256_password BY 'qwerty'
```

`mira` should start client app at the host where the ClickHouse server runs.

Create the user account `john`, assign roles to it and make this roles default:

``` sql
CREATE USER john DEFAULT ROLE role1, role2
```

Create the user account `john` and make all his future roles default:

``` sql
ALTER USER user DEFAULT ROLE ALL
```

When some role is assigned to `john` in the future, it will become default automatically.

Create the user account `john` and make all his future roles default excepting `role1` and `role2`:

``` sql
ALTER USER john DEFAULT ROLE ALL EXCEPT role1, role2
```
