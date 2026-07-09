---
description: 'USER 文档'
sidebar_label: 'USER'
sidebar_position: 39
slug: /sql-reference/statements/create/user
title: 'CREATE USER'
doc_type: 'reference'
---

创建[用户账户](../../../guides/sre/user-management/index.md#user-account-management)。

语法：

```sql
CREATE USER [IF NOT EXISTS | OR REPLACE] name1 [, name2 [,...]] [ON CLUSTER cluster_name]
    [NOT IDENTIFIED | IDENTIFIED {[WITH {plaintext_password | sha256_password | sha256_hash | double_sha1_password | double_sha1_hash}] BY {'password' | 'hash'}} | WITH NO_PASSWORD | {WITH ldap SERVER 'server_name'} | {WITH kerberos [REALM 'realm']} | {WITH ssl_certificate CN 'common_name' | SAN 'TYPE:subject_alt_name'} | {WITH ssh_key BY KEY 'public_key' TYPE 'ssh-rsa|...'} | {WITH http SERVER 'server_name' [SCHEME 'Basic']} [VALID UNTIL datetime] 
    [, {[{plaintext_password | sha256_password | sha256_hash | ...}] BY {'password' | 'hash'}} | {ldap SERVER 'server_name'} | {...} | ... [,...]]]
    [HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [VALID UNTIL datetime]
    [IN access_storage_type]
    [ROLE role [,...]]
    [DEFAULT ROLE role [,...]]
    [DEFAULT DATABASE database | NONE]
    [GRANTEES {user | role | ANY | NONE} [,...] [EXCEPT {user | role} [,...]]]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY | WRITABLE] | PROFILE 'profile_name'] [,...]
```

`ON CLUSTER` 子句允许在集群中创建用户，参见 [Distributed DDL](../../../sql-reference/distributed-ddl.md)。

<div id="identification">
  ## 身份识别
</div>

用户可通过多种方式进行身份识别：

* `IDENTIFIED WITH no_password`
* `IDENTIFIED WITH plaintext_password BY 'qwerty'`
* `IDENTIFIED WITH sha256_password BY 'qwerty'` or `IDENTIFIED BY 'password'`
* `IDENTIFIED WITH sha256_hash BY 'hash'` or `IDENTIFIED WITH sha256_hash BY 'hash' SALT 'salt'`
* `IDENTIFIED WITH double_sha1_password BY 'qwerty'`
* `IDENTIFIED WITH double_sha1_hash BY 'hash'`
* `IDENTIFIED WITH bcrypt_password BY 'qwerty'`
* `IDENTIFIED WITH bcrypt_hash BY 'hash'`
* `IDENTIFIED WITH ldap SERVER 'server_name'`
* `IDENTIFIED WITH kerberos` or `IDENTIFIED WITH kerberos REALM 'realm'`
* `IDENTIFIED WITH ssl_certificate CN 'mysite.com:user'`
* `IDENTIFIED WITH ssh_key BY KEY 'public_key' TYPE 'ssh-rsa', KEY 'another_public_key' TYPE 'ssh-ed25519'`
* `IDENTIFIED WITH http SERVER 'http_server'` or `IDENTIFIED WITH http SERVER 'http_server' SCHEME 'basic'`
* `IDENTIFIED BY 'qwerty'`

密码复杂度要求可在 [config.xml](/zh/operations/configuration-files) 中修改。下面是一个示例配置：要求密码长度至少为 12 个字符，并包含 1 个数字。每条密码复杂度规则都需要一个用于匹配密码的正则表达式，以及该规则的说明。

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
在 ClickHouse Cloud 中，默认情况下，密码必须满足以下复杂度要求：

* 长度至少为 12 个字符
* 至少包含 1 个数字
* 至少包含 1 个大写字母
* 至少包含 1 个小写字母
* 至少包含 1 个特殊字符
  :::

<div id="examples">
  ## 示例
</div>

1. 以下用户名为 `name1`，且无需密码——这显然几乎没有安全性可言：

   ```sql
   CREATE USER name1 NOT IDENTIFIED
   ```

2. 要指定明文密码：

   ```sql
   CREATE USER name2 IDENTIFIED WITH plaintext_password BY 'my_password'
   ```

   :::tip
   密码会以 SQL 文本文件的形式存储在 `/var/lib/clickhouse/access` 中，因此使用 `plaintext_password` 并不是个好主意。请改用 `sha256_password`，如下所示...
   :::

3. 最常见的做法是使用经过 SHA-256 哈希处理的密码。当你指定 `IDENTIFIED WITH sha256_password` 时，ClickHouse 会自动为你对密码进行哈希处理。例如：

   ```sql
   CREATE USER name3 IDENTIFIED WITH sha256_password BY 'my_password'
   ```

   `name3` 用户现在可以使用 `my_password` 登录，但密码会以上述哈希值的形式存储。系统会在 `/var/lib/clickhouse/access` 中创建如下 SQL 文件，并在服务器启动时执行：

   ```bash
   /var/lib/clickhouse/access $ cat 3843f510-6ebd-a52d-72ac-e021686d8a93.sql
   ATTACH USER name3 IDENTIFIED WITH sha256_hash BY '0C268556C1680BEF0640AAC1E7187566704208398DA31F03D18C74F5C5BE5053' SALT '4FB16307F5E10048196966DD7E6876AE53DE6A1D1F625488482C75F14A5097C7';
   ```

   :::tip
   如果你已经为某个用户名生成了哈希值及对应的盐值，那么可以使用 `IDENTIFIED WITH sha256_hash BY 'hash'` 或 `IDENTIFIED WITH sha256_hash BY 'hash' SALT 'salt'`。对于使用 `SALT` 的 `sha256_hash` 身份验证，哈希值必须通过拼接 &#39;password&#39; 和 &#39;salt&#39; 后计算得出。
   :::

4. `double_sha1_password` 通常并非必需，但在与要求使用它的客户端配合时会很有用 (例如 MySQL 接口) ：

   ```sql
   CREATE USER name4 IDENTIFIED WITH double_sha1_password BY 'my_password'
   ```

   ClickHouse 会生成并运行以下查询：

   ```response
   CREATE USER name4 IDENTIFIED WITH double_sha1_hash BY 'CCD3A959D6A004B9C3807B728BC2E55B67E10518'
   ```

5. `bcrypt_password` 是存储密码时最安全的选项。它使用 [bcrypt](https://en.wikipedia.org/wiki/Bcrypt) 算法，即使密码哈希已泄露，也能有效抵御暴力破解攻击。

   ```sql
   CREATE USER name5 IDENTIFIED WITH bcrypt_password BY 'my_password'
   ```

   使用这种方法时，密码长度限制为 72 个字符。
   bcrypt work factor 参数用于定义计算哈希值和验证密码所需的计算量与时间，可以在服务器配置中进行修改：

   ```xml
   <bcrypt_workfactor>12</bcrypt_workfactor>
   ```

   work factor 必须介于 4 到 31 之间，默认值为 12。

   :::warning
   对于高频身份验证的应用，
   由于 bcrypt 在较高 work factor 下
   会带来较大的计算开销，
   请考虑使用其他身份验证方法。
   :::

6. 密码类型也可以省略：

   ```sql
   CREATE USER name6 IDENTIFIED BY 'my_password'
   ```

   在这种情况下，ClickHouse 将使用服务器配置中指定的默认密码类型：

   ```xml
   <default_password_type>sha256_password</default_password_type>
   ```

   可用的密码类型有：`plaintext_password`、`sha256_password`、`double_sha1_password`。

7. 可以指定多种身份验证方法：

   ```sql
   CREATE USER user1 IDENTIFIED WITH plaintext_password by '1', bcrypt_password by '2', plaintext_password by '3''
   ```

说明：

1. 较旧版本的 ClickHouse 可能不支持多个身份验证方法的语法。因此，如果 ClickHouse server 中存在此类用户，并且被降级到不支持该语法的版本，这些用户将无法使用，某些与用户相关的操作也会失效。为了平稳降级，必须在降级前将所有用户设置为仅包含一种身份验证方法。或者，如果 server 在未遵循正确流程的情况下已被降级，则应删除这些有问题的用户。
2. 出于安全原因，`no_password` 不能与其他身份验证方法并存。因此，只有当 `no_password` 是查询中唯一的身份验证方法时，才能指定
   `no_password`。

<div id="user-host">
  ## 用户主机
</div>

用户主机是指可用于建立到 ClickHouse server 的连接的主机。可以在 `HOST` 查询部分中按以下方式指定主机：

* `HOST IP 'ip_address_or_subnetwork'` — 用户只能从指定的 IP 地址或某个[子网](https://en.wikipedia.org/wiki/Subnetwork)连接到 ClickHouse server。示例：`HOST IP '192.168.0.0/16'`、`HOST IP '2001:DB8::/32'`。在生产环境中，建议仅指定 `HOST IP` 元素 (IP 地址及其掩码) ，因为使用 `host` 和 `host_regexp` 可能会带来额外的延迟。
* `HOST ANY` — 用户可以从任何位置连接。这是默认选项。
* `HOST LOCAL` — 用户只能从本地连接。
* `HOST NAME 'fqdn'` — 可以将用户主机指定为 FQDN。例如：`HOST NAME 'mysite.com'`。
* `HOST REGEXP 'regexp'` — 指定用户主机时，可以使用 [pcre](http://www.pcre.org/) 正则表达式。例如：`HOST REGEXP '.*\.mysite\.com'`。
* `HOST LIKE 'template'` — 允许你使用 [LIKE](/zh/sql-reference/functions/string-search-functions#like) 运算符来过滤用户主机。例如，`HOST LIKE '%'` 等同于 `HOST ANY`，`HOST LIKE '%.mysite.com'` 会过滤 `mysite.com` 域下的所有主机。

指定主机的另一种方式是使用跟在用户名后面的 `@` 语法。示例：

* `CREATE USER mira@'127.0.0.1'` — 等同于 `HOST IP` 语法。
* `CREATE USER mira@'localhost'` — 等同于 `HOST LOCAL` 语法。
* `CREATE USER mira@'192.168.%.%'` — 等同于 `HOST LIKE` 语法。

:::tip
ClickHouse 将 `user_name@'address'` 视为一个完整的用户名。因此，从技术上讲，你可以创建多个具有相同 `user_name`、但 `@` 后部分不同的用户。不过，我们不建议这样做。
:::

<div id="valid-until-clause">
  ## VALID UNTIL 子句
</div>

允许为身份验证方法指定过期日期，并可选择指定过期时间。它接受一个字符串参数。建议日期时间使用 `YYYY-MM-DD [hh:mm:ss] [timezone]` 格式，其中 `[timezone]` 必须为数值偏移量，例如 `+09:00`，或 `UTC`、`GMT`、`Z`、`MSK`、`MSD` 之一；像 `Asia/Tokyo` 这样的具名 IANA 时区不会被识别 (见下方说明) 。默认情况下，该参数等于 `'infinity'`。
`VALID UNTIL` 子句只能与某种身份验证方法一同指定，唯一的例外是查询中未指定任何身份验证方法的情况。在这种情况下，`VALID UNTIL` 子句将应用于所有现有的身份验证方法。

示例：

* `CREATE USER name1 VALID UNTIL '2025-01-01'`
* `CREATE USER name1 VALID UNTIL '2025-01-01 12:00:00 UTC'`
* `CREATE USER name1 VALID UNTIL '2025-01-01 12:00:00 +09:00'`
* `CREATE USER name1 VALID UNTIL 'infinity'`
* `CREATE USER name1 IDENTIFIED WITH plaintext_password BY 'no_expiration', bcrypt_password BY 'expiration_set' VALID UNTIL '2025-01-01'`

:::note
该日期时间字符串由 `parseDateTimeBestEffort` 解析，它仅识别时区标记 `UTC`、`GMT`、`Z`、`MSK`、`MSD` 以及 `+09:00` 或 `-05:00` 这类数值偏移量。像 `Asia/Tokyo` 或 `Europe/London` 这样的具名 IANA 时区不受支持；此外，对于实行夏令时的区域，固定偏移量并不等同于 IANA 时区，因此你必须根据所编码的具体日期计算正确的偏移量。
:::

<div id="grantees-clause">
  ## GRANTEES 子句
</div>

指定哪些用户或角色可以从该用户接收[特权](../../../sql-reference/statements/grant.md#privileges)，前提是该用户本身也已通过 [GRANT OPTION](../../../sql-reference/statements/grant.md#granting-privilege-syntax) 获得所有必需的访问权限。`GRANTEES` 子句的选项包括：

* `user` — 指定该用户可以向其授予特权的用户。
* `role` — 指定该用户可以向其授予特权的角色。
* `ANY` — 该用户可以向任何人授予特权。这是默认设置。
* `NONE` — 该用户不能向任何人授予特权。

你可以使用 `EXCEPT` 表达式排除任意用户或角色。例如，`CREATE USER user1 GRANTEES ANY EXCEPT user2`。这意味着，如果 `user1` 拥有一些通过 `GRANT OPTION` 授予的特权，则可以将这些特权授予除 `user2` 之外的任何人。

<div id="examples">
  ## 示例
</div>

创建用户账户 `mira`，并使用密码 `qwerty` 进行保护：

```sql
CREATE USER mira HOST IP '127.0.0.1' IDENTIFIED WITH sha256_password BY 'qwerty';
```

`mira` 应在运行 ClickHouse server 的主机上启动客户端应用程序。

创建用户账户 `john` 并为其分配角色：

```sql
CREATE USER john ROLE role1, role2;
```

创建用户账户 `john`，分配角色，并将其中一部分设为默认角色：

```sql
CREATE USER john ROLE role1, role2 DEFAULT ROLE role1;
```

或

```sql
CREATE USER john ROLE role1, role2 DEFAULT ROLE ALL EXCEPT role2;
```

创建用户账户 `john`，并允许他将自己的特权授予 `jack` 用户账户：

```sql
CREATE USER john GRANTEES jack;
```

使用查询参数创建用户账户 `john`：

```sql
SET param_user=john;
CREATE USER {user:Identifier};
```