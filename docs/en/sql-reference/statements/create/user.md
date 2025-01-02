---
slug: /en/sql-reference/statements/create/user
sidebar_position: 39
sidebar_label: USER
title: "CREATE USER"
---

Creates [user accounts](../../../guides/sre/user-management/index.md#user-account-management).

Syntax:

``` sql
CREATE USER [IF NOT EXISTS | OR REPLACE] name1 [, name2 [,...]] [ON CLUSTER cluster_name]
    [NOT IDENTIFIED | IDENTIFIED {[WITH {plaintext_password | sha256_password | sha256_hash | double_sha1_password | double_sha1_hash}] BY {'password' | 'hash'}} | WITH NO_PASSWORD | {WITH ldap SERVER 'server_name'} | {WITH kerberos [REALM 'realm']} | {WITH ssl_certificate CN 'common_name' | SAN 'TYPE:subject_alt_name'} | {WITH ssh_key BY KEY 'public_key' TYPE 'ssh-rsa|...'} | {WITH http SERVER 'server_name' [SCHEME 'Basic']} 
    [, {[{plaintext_password | sha256_password | sha256_hash | ...}] BY {'password' | 'hash'}} | {ldap SERVER 'server_name'} | {...} | ... [,...]]]
    [HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [VALID UNTIL datetime]
    [IN access_storage_type]
    [DEFAULT ROLE role [,...]]
    [DEFAULT DATABASE database | NONE]
    [GRANTEES {user | role | ANY | NONE} [,...] [EXCEPT {user | role} [,...]]]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY | WRITABLE] | PROFILE 'profile_name'] [,...]
```

`ON CLUSTER` clause allows creating users on a cluster, see [Distributed DDL](../../../sql-reference/distributed-ddl.md).

## Identification

There are multiple ways of user identification:

- `IDENTIFIED WITH no_password`
- `IDENTIFIED WITH plaintext_password BY 'qwerty'`
- `IDENTIFIED WITH sha256_password BY 'qwerty'` or `IDENTIFIED BY 'password'`
- `IDENTIFIED WITH sha256_hash BY 'hash'` or `IDENTIFIED WITH sha256_hash BY 'hash' SALT 'salt'`
- `IDENTIFIED WITH double_sha1_password BY 'qwerty'`
- `IDENTIFIED WITH double_sha1_hash BY 'hash'`
- `IDENTIFIED WITH bcrypt_password BY 'qwerty'`
- `IDENTIFIED WITH bcrypt_hash BY 'hash'`
- `IDENTIFIED WITH ldap SERVER 'server_name'`
- `IDENTIFIED WITH kerberos` or `IDENTIFIED WITH kerberos REALM 'realm'`
- `IDENTIFIED WITH ssl_certificate CN 'mysite.com:user'`
- `IDENTIFIED WITH ssh_key BY KEY 'public_key' TYPE 'ssh-rsa', KEY 'another_public_key' TYPE 'ssh-ed25519'`
- `IDENTIFIED WITH http SERVER 'http_server'` or `IDENTIFIED WITH http SERVER 'http_server' SCHEME 'basic'`
- `IDENTIFIED BY 'qwerty'`

Password complexity requirements can be edited in [config.xml](/docs/en/operations/configuration-files). Below is an example configuration that requires passwords to be at least 12 characters long and contain 1 number. Each password complexity rule requires a regex to match against passwords and a description of the rule.

```xml
<clickhouse>
    <password_complexity>
        <rule>
            <pattern>.{12}</pattern>
            <message>be at least 12 characters long</message>
        </rule>
        <rule>
            <pattern>\p{N}</pattern>
            <message>contain at least 1 numeric character</message>
        </rule>
    </password_complexity>
</clickhouse>
```

:::note
In ClickHouse Cloud, by default, passwords must meet the following complexity requirements:
- Be at least 12 characters long
- Contain at least 1 numeric character
- Contain at least 1 uppercase character
- Contain at least 1 lowercase character
- Contain at least 1 special character
:::

## Examples

1. The following username is `name1` and does not require a password - which obviously doesn't provide much security:

    ```sql
    CREATE USER name1 NOT IDENTIFIED
    ```

2. To specify a plaintext password:

    ```sql
    CREATE USER name2 IDENTIFIED WITH plaintext_password BY 'my_password'
    ```

    :::tip
    The password is stored in a SQL text file in `/var/lib/clickhouse/access`, so it's not a good idea to use `plaintext_password`. Try `sha256_password` instead, as demonstrated next...
    :::

3. The most common option is to use a password that is hashed using SHA-256. ClickHouse will hash the password for you when you specify `IDENTIFIED WITH sha256_password`. For example:

    ```sql
    CREATE USER name3 IDENTIFIED WITH sha256_password BY 'my_password'
    ```

    The `name3` user can now login using `my_password`, but the password is stored as the hashed value above. The following SQL file was created in `/var/lib/clickhouse/access` and gets executed at server startup:

    ```bash
    /var/lib/clickhouse/access $ cat 3843f510-6ebd-a52d-72ac-e021686d8a93.sql
    ATTACH USER name3 IDENTIFIED WITH sha256_hash BY '0C268556C1680BEF0640AAC1E7187566704208398DA31F03D18C74F5C5BE5053' SALT '4FB16307F5E10048196966DD7E6876AE53DE6A1D1F625488482C75F14A5097C7';
    ```

    :::tip
    If you have already created a hash value and corresponding salt value for a username, then you can use `IDENTIFIED WITH sha256_hash BY 'hash'` or `IDENTIFIED WITH sha256_hash BY 'hash' SALT 'salt'`. For identification with `sha256_hash` using `SALT` - hash must be calculated from concatenation of 'password' and 'salt'.
    :::

4. The `double_sha1_password` is not typically needed, but comes in handy when working with clients that require it (like the MySQL interface):

    ```sql
    CREATE USER name4 IDENTIFIED WITH double_sha1_password BY 'my_password'
    ```

    ClickHouse generates and runs the following query:

    ```response
    CREATE USER name4 IDENTIFIED WITH double_sha1_hash BY 'CCD3A959D6A004B9C3807B728BC2E55B67E10518'
    ```

5. The `bcrypt_password` is the most secure option for storing passwords. It uses the [bcrypt](https://en.wikipedia.org/wiki/Bcrypt) algorithm, which is resilient against brute force attacks even if the password hash is compromised.

    ```sql
    CREATE USER name5 IDENTIFIED WITH bcrypt_password BY 'my_password'
    ```

    The length of the password is limited to 72 characters with this method. The bcrypt work factor parameter, which defines the amount of computations and time needed to compute the hash and verify the password, can be modified in the server configuration:

    ```xml
    <bcrypt_workfactor>12</bcrypt_workfactor>
    ```

    The work factor must be between 4 and 31, with a default value of 12.

6. The type of the password can also be omitted:

    ```sql
    CREATE USER name6 IDENTIFIED BY 'my_password'
    ```

    In this case, ClickHouse will use the default password type specified in the server configuration:

    ```xml
    <default_password_type>sha256_password</default_password_type>
    ```

    The available password types are: `plaintext_password`, `sha256_password`, `double_sha1_password`.

7. Multiple authentication methods can be specified: 

   ```sql
   CREATE USER user1 IDENTIFIED WITH plaintext_password by '1', bcrypt_password by '2', plaintext_password by '3''
   ```

Notes:
1. Older versions of ClickHouse might not support the syntax of multiple authentication methods. Therefore, if the ClickHouse server contains such users and is downgraded to a version that does not support it, such users will become unusable and some user related operations will be broken. In order to downgrade gracefully, one must set all users to contain a single authentication method prior to downgrading. Alternatively, if the server was downgraded without the proper procedure, the faulty users should be dropped.
2. `no_password` can not co-exist with other authentication methods for security reasons. Therefore, you can only specify
`no_password` if it is the only authentication method in the query. 

## User Host

User host is a host from which a connection to ClickHouse server could be established. The host can be specified in the `HOST` query section in the following ways:

- `HOST IP 'ip_address_or_subnetwork'` — User can connect to ClickHouse server only from the specified IP address or a [subnetwork](https://en.wikipedia.org/wiki/Subnetwork). Examples: `HOST IP '192.168.0.0/16'`, `HOST IP '2001:DB8::/32'`. For use in production, only specify `HOST IP` elements (IP addresses and their masks), since using `host` and `host_regexp` might cause extra latency.
- `HOST ANY` — User can connect from any location. This is a default option.
- `HOST LOCAL` — User can connect only locally.
- `HOST NAME 'fqdn'` — User host can be specified as FQDN. For example, `HOST NAME 'mysite.com'`.
- `HOST REGEXP 'regexp'` — You can use [pcre](http://www.pcre.org/) regular expressions when specifying user hosts. For example, `HOST REGEXP '.*\.mysite\.com'`.
- `HOST LIKE 'template'` — Allows you to use the [LIKE](../../../sql-reference/functions/string-search-functions.md#function-like) operator to filter the user hosts. For example, `HOST LIKE '%'` is equivalent to `HOST ANY`, `HOST LIKE '%.mysite.com'` filters all the hosts in the `mysite.com` domain.

Another way of specifying host is to use `@` syntax following the username. Examples:

- `CREATE USER mira@'127.0.0.1'` — Equivalent to the `HOST IP` syntax.
- `CREATE USER mira@'localhost'` — Equivalent to the `HOST LOCAL` syntax.
- `CREATE USER mira@'192.168.%.%'` — Equivalent to the `HOST LIKE` syntax.

:::tip
ClickHouse treats `user_name@'address'` as a username as a whole. Thus, technically you can create multiple users with the same `user_name` and different constructions after `@`. However, we do not recommend to do so.
:::

## VALID UNTIL Clause

Allows you to specify the expiration date and, optionally, the time for user credentials. It accepts a string as a parameter. It is recommended to use the `YYYY-MM-DD [hh:mm:ss] [timezone]` format for datetime. By default, this parameter equals `'infinity'`.

Examples:

- `CREATE USER name1 VALID UNTIL '2025-01-01'`
- `CREATE USER name1 VALID UNTIL '2025-01-01 12:00:00 UTC'`
- `CREATE USER name1 VALID UNTIL 'infinity'`

## GRANTEES Clause

Specifies users or roles which are allowed to receive [privileges](../../../sql-reference/statements/grant.md#privileges) from this user on the condition this user has also all required access granted with [GRANT OPTION](../../../sql-reference/statements/grant.md#granting-privilege-syntax). Options of the `GRANTEES` clause:

- `user` — Specifies a user this user can grant privileges to.
- `role` — Specifies a role this user can grant privileges to.
- `ANY` — This user can grant privileges to anyone. It's the default setting.
- `NONE` — This user can grant privileges to none.

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
