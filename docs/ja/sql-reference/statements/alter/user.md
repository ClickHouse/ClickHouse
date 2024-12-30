---
slug: /ja/sql-reference/statements/alter/user
sidebar_position: 45
sidebar_label: USER
title: "ALTER USER"
---

ClickHouseユーザーアカウントを変更します。

構文：

``` sql
ALTER USER [IF EXISTS] name1 [RENAME TO new_name |, name2 [,...]] 
    [ON CLUSTER cluster_name]
    [NOT IDENTIFIED | RESET AUTHENTICATION METHODS TO NEW | {IDENTIFIED | ADD IDENTIFIED} {[WITH {plaintext_password | sha256_password | sha256_hash | double_sha1_password | double_sha1_hash}] BY {'password' | 'hash'}} | WITH NO_PASSWORD | {WITH ldap SERVER 'server_name'} | {WITH kerberos [REALM 'realm']} | {WITH ssl_certificate CN 'common_name' | SAN 'TYPE:subject_alt_name'} | {WITH ssh_key BY KEY 'public_key' TYPE 'ssh-rsa|...'} | {WITH http SERVER 'server_name' [SCHEME 'Basic']} [VALID UNTIL datetime]
    [, {[{plaintext_password | sha256_password | sha256_hash | ...}] BY {'password' | 'hash'}} | {ldap SERVER 'server_name'} | {...} | ... [,...]]]
    [[ADD | DROP] HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [VALID UNTIL datetime]
    [DEFAULT ROLE role [,...] | ALL | ALL EXCEPT role [,...] ]
    [GRANTEES {user | role | ANY | NONE} [,...] [EXCEPT {user | role} [,...]]]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY | WRITABLE] | PROFILE 'profile_name'] [,...]
```

`ALTER USER`を使用するには、[ALTER USER](../../../sql-reference/statements/grant.md#access-management)権限が必要です。

## GRANTEES句

このユーザーが必要なすべてのアクセスを[GRANT OPTION](../../../sql-reference/statements/grant.md#granting-privilege-syntax)で付与された条件下で、このユーザーから[特権](../../../sql-reference/statements/grant.md#privileges)を受け取ることを許可されているユーザーまたはロールを指定します。`GRANTEES`句のオプションは次のとおりです：

- `user` — このユーザーが特権を付与できるユーザーを指定します。
- `role` — このユーザーが特権を付与できるロールを指定します。
- `ANY` — このユーザーは誰にでも特権を付与できます。これはデフォルト設定です。
- `NONE` — このユーザーは誰にも特権を付与できません。

`EXCEPT`式を使用することで、任意のユーザーまたはロールを除外することができます。例：`ALTER USER user1 GRANTEES ANY EXCEPT user2`。これは、`user1`が`GRANT OPTION`で付与された特権を持っている場合、その特権を`user2`を除く誰にでも付与できることを意味します。

## 例

割り当てられたロールをデフォルトに設定:

``` sql
ALTER USER user DEFAULT ROLE role1, role2
```

ユーザーにロールが以前に割り当てられていない場合、ClickHouseは例外をスローします。

割り当てられたすべてのロールをデフォルトに設定:

``` sql
ALTER USER user DEFAULT ROLE ALL
```

将来、ユーザーにロールが割り当てられる場合、それは自動的にデフォルトになります。

`role1`および`role2`を除くすべての割り当てられたロールをデフォルトに設定:

``` sql
ALTER USER user DEFAULT ROLE ALL EXCEPT role1, role2
```

`john`アカウントを持つユーザーが`jack`アカウントを持つユーザーに特権を付与することを許可:

``` sql
ALTER USER john GRANTEES jack;
```

既存の認証方法を保持しながら、新しい認証方法をユーザーに追加:

``` sql
ALTER USER user1 ADD IDENTIFIED WITH plaintext_password by '1', bcrypt_password by '2', plaintext_password by '3'
```

注意事項:
1. 古いバージョンのClickHouseは複数の認証方法の構文をサポートしていない可能性があります。そのため、ClickHouseサーバーにそのようなユーザーが含まれていて、サポートされていないバージョンにダウングレードされた場合、そのユーザーは使用不能になり、ユーザー関連の操作が一部失敗します。スムーズにダウングレードするためには、ダウングレード前にすべてのユーザーを単一の認証方法に設定する必要があります。あるいは、適切な手続きを経ずにサーバーがダウングレードされた場合、問題のあるユーザーを削除する必要があります。
2. セキュリティの理由から、`no_password`は他の認証方法と共存できません。そのため、`no_password`認証方法を`ADD`することはできません。以下のクエリはエラーをスローします：

``` sql
ALTER USER user1 ADD IDENTIFIED WITH no_password
```

ユーザーの認証方法を削除して`no_password`に依存する場合は、下記の置換形式で指定する必要があります。

認証方法をリセットし、クエリで指定されたものを追加（ADDキーワードなしでのIDENTIFIEDの効果）:

``` sql
ALTER USER user1 IDENTIFIED WITH plaintext_password by '1', bcrypt_password by '2', plaintext_password by '3'
```

認証方法をリセットし、最新の追加を保持:

``` sql
ALTER USER user1 RESET AUTHENTICATION METHODS TO NEW
```

## VALID UNTIL句

認証方法に有効期限の日付、およびオプションで時刻を指定できます。パラメータとして文字列を受け入れます。日時の形式には`YYYY-MM-DD [hh:mm:ss] [timezone]`の使用を推奨します。デフォルトでは、このパラメータは`'infinity'`です。`VALID UNTIL`句は、クエリで認証方法が指定されていない場合を除いて、認証方法とともにのみ指定できます。このシナリオでは、`VALID UNTIL`句は既存のすべての認証方法に適用されます。

例：

- `ALTER USER name1 VALID UNTIL '2025-01-01'`
- `ALTER USER name1 VALID UNTIL '2025-01-01 12:00:00 UTC'`
- `ALTER USER name1 VALID UNTIL 'infinity'`
- `ALTER USER name1 IDENTIFIED WITH plaintext_password BY 'no_expiration', bcrypt_password BY 'expiration_set' VALID UNTIL '2025-01-01'`
