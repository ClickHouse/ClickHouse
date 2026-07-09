---
description: 'USER 相关文档'
sidebar_label: 'USER'
sidebar_position: 45
slug: /sql-reference/statements/alter/user
title: 'ALTER USER'
doc_type: 'reference'
---

修改 ClickHouse 用户账户。

语法：

```sql
ALTER USER [IF EXISTS] name1 [RENAME TO new_name |, name2 [,...]] 
    [ON CLUSTER cluster_name]
    [NOT IDENTIFIED | RESET AUTHENTICATION METHODS TO NEW | {IDENTIFIED | ADD IDENTIFIED} {[WITH {plaintext_password | sha256_password | sha256_hash | double_sha1_password | double_sha1_hash}] BY {'password' | 'hash'}} | WITH NO_PASSWORD | {WITH ldap SERVER 'server_name'} | {WITH kerberos [REALM 'realm']} | {WITH ssl_certificate CN 'common_name' | SAN 'TYPE:subject_alt_name'} | {WITH ssh_key BY KEY 'public_key' TYPE 'ssh-rsa|...'} | {WITH http SERVER 'server_name' [SCHEME 'Basic']} [VALID UNTIL datetime]
    [, {[{plaintext_password | sha256_password | sha256_hash | ...}] BY {'password' | 'hash'}} | {ldap SERVER 'server_name'} | {...} | ... [,...]]]
    [[ADD | DROP] HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [VALID UNTIL datetime]
    [DEFAULT ROLE role [,...] | ALL | ALL EXCEPT role [,...] ]
    [GRANTEES {user | role | ANY | NONE} [,...] [EXCEPT {user | role} [,...]]]
    [DROP ALL PROFILES]
    [DROP ALL SETTINGS]
    [DROP SETTINGS variable [,...] ]
    [DROP PROFILES 'profile_name' [,...] ]
    [ADD|MODIFY SETTINGS variable [=value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE|CONST|CHANGEABLE_IN_READONLY] [,...] ]
    [SET variable [=value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE|CONST|CHANGEABLE_IN_READONLY] [,...] ]
    [ADD PROFILES 'profile_name' [,...] ]
```

要使用 `ALTER USER`，你必须拥有 [ALTER USER](../../../sql-reference/statements/grant.md#access-management) 权限。

`SET variable = value` 是 `MODIFY SETTING variable = value` 的别名：它会就地修改单个设置，同时保留其余设置。相比单独使用 `SETTINGS` 子句，应优先使用它 (或 `MODIFY SETTING`) ；后者会替换整个 settings 列表，并且还会移除所有继承自父级的 profile。

<div id="grantees-clause">
  ## GRANTEES 子句
</div>

指定允许从该用户接收[特权](../../../sql-reference/statements/grant.md#privileges)的用户或角色，前提是该用户本身也已通过 [GRANT OPTION](../../../sql-reference/statements/grant.md#granting-privilege-syntax) 获得所有必需的访问权限。`GRANTEES` 子句的选项包括：

* `user` — 指定该用户可以向其授予特权的用户。
* `role` — 指定该用户可以向其授予特权的角色。
* `ANY` — 该用户可以向任何人授予特权。这是默认设置。
* `NONE` — 该用户不能向任何人授予特权。

你可以使用 `EXCEPT` 表达式排除任意用户或角色。例如，`ALTER USER user1 GRANTEES ANY EXCEPT user2`。这表示，如果 `user1` 拥有一些通过 `GRANT OPTION` 授予的特权，它就可以将这些特权授予除 `user2` 之外的任何人。

<div id="examples">
  ## 示例
</div>

将已分配的角色设置为默认角色：

```sql
ALTER USER user DEFAULT ROLE role1, role2
```

如果先前未向用户分配任何角色，ClickHouse 会抛出异常。

将所有已分配角色设为默认：

```sql
ALTER USER user DEFAULT ROLE ALL
```

如果今后为某个用户分配了角色，该角色将自动成为默认角色。

将除 `role1` 和 `role2` 之外的所有已分配角色设为默认角色：

```sql
ALTER USER user DEFAULT ROLE ALL EXCEPT role1, role2
```

允许 `john` 账户对应的用户将其特权授予 `jack` 账户对应的用户：

```sql
ALTER USER john GRANTEES jack;
```

向该用户添加新的身份验证方法，同时保留现有方法：

```sql
ALTER USER user1 ADD IDENTIFIED WITH plaintext_password by '1', bcrypt_password by '2', plaintext_password by '3'
```

注意：

1. 较旧版本的 ClickHouse 可能不支持多种身份验证方法的语法。因此，如果 ClickHouse server 中存在此类用户，并且被降级到不支持该功能的版本，这些用户将无法再使用，某些与用户相关的操作也会失效。为了平稳降级，必须在降级前将所有用户都设置为仅包含一种身份验证方法。或者，如果 server 未按正确流程就已降级，则应删除这些有问题的用户。
2. 出于安全原因，`no_password` 不能与其他身份验证方法共存。
   因此，无法 `ADD` `no_password` 身份验证方法。下面的查询将抛出错误：

```sql
ALTER USER user1 ADD IDENTIFIED WITH no_password
```

如果要删除某个用户的身份验证方法并改为使用 `no_password`，则必须采用下面这种替换形式来指定。

重置身份验证方法，并添加查询中指定的方法 (即前置使用 IDENTIFIED 且不带 ADD 关键字时的效果) ：

```sql
ALTER USER user1 IDENTIFIED WITH plaintext_password by '1', bcrypt_password by '2', plaintext_password by '3'
```

重置身份验证方法，并保留最新添加的一种：

```sql
ALTER USER user1 RESET AUTHENTICATION METHODS TO NEW
```

<div id="valid-until-clause">
  ## VALID UNTIL 子句
</div>

用于为身份验证方法指定过期日期，以及可选的过期时间。它接受一个字符串参数。对于日期时间，建议使用 `YYYY-MM-DD [hh:mm:ss] [timezone]` 格式。默认情况下，该参数等于 `'infinity'`。
`VALID UNTIL` 子句只能与某种身份验证方法一起指定，唯一的例外是查询中未指定任何身份验证方法的情况。在这种情况下，`VALID UNTIL` 子句将应用于所有现有的身份验证方法。

示例：

* `ALTER USER name1 VALID UNTIL '2025-01-01'`
* `ALTER USER name1 VALID UNTIL '2025-01-01 12:00:00 UTC'`
* `ALTER USER name1 VALID UNTIL 'infinity'`
* `ALTER USER name1 IDENTIFIED WITH plaintext_password BY 'no_expiration', bcrypt_password BY 'expiration_set' VALID UNTIL'2025-01-01''`