---
slug: /ja/sql-reference/statements/create/user
sidebar_position: 39
sidebar_label: USER
title: "CREATE USER"
---

[ユーザーアカウント](../../../guides/sre/user-management/index.md#user-account-management)を作成します。

構文:

``` sql
CREATE USER [IF NOT EXISTS | OR REPLACE] name1 [, name2 [,...]] [ON CLUSTER cluster_name]
    [NOT IDENTIFIED | IDENTIFIED {[WITH {plaintext_password | sha256_password | sha256_hash | double_sha1_password | double_sha1_hash}] BY {'password' | 'hash'}} | WITH NO_PASSWORD | {WITH ldap SERVER 'server_name'} | {WITH kerberos [REALM 'realm']} | {WITH ssl_certificate CN 'common_name' | SAN 'TYPE:subject_alt_name'} | {WITH ssh_key BY KEY 'public_key' TYPE 'ssh-rsa|...'} | {WITH http SERVER 'server_name' [SCHEME 'Basic']} [VALID UNTIL datetime] 
    [, {[{plaintext_password | sha256_password | sha256_hash | ...}] BY {'password' | 'hash'}} | {ldap SERVER 'server_name'} | {...} | ... [,...]]]
    [HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [VALID UNTIL datetime]
    [IN access_storage_type]
    [DEFAULT ROLE role [,...]]
    [DEFAULT DATABASE database | NONE]
    [GRANTEES {user | role | ANY | NONE} [,...] [EXCEPT {user | role} [,...]]]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY | WRITABLE] | PROFILE 'profile_name'] [,...]
```

`ON CLUSTER`句はクラスター上でユーザーを作成することを可能にします。[分散DDL](../../../sql-reference/distributed-ddl.md)を参照してください。

## 識別

ユーザーの識別には複数の方法があります:

- `IDENTIFIED WITH no_password`
- `IDENTIFIED WITH plaintext_password BY 'qwerty'`
- `IDENTIFIED WITH sha256_password BY 'qwerty'` または `IDENTIFIED BY 'password'`
- `IDENTIFIED WITH sha256_hash BY 'hash'` または `IDENTIFIED WITH sha256_hash BY 'hash' SALT 'salt'`
- `IDENTIFIED WITH double_sha1_password BY 'qwerty'`
- `IDENTIFIED WITH double_sha1_hash BY 'hash'`
- `IDENTIFIED WITH bcrypt_password BY 'qwerty'`
- `IDENTIFIED WITH bcrypt_hash BY 'hash'`
- `IDENTIFIED WITH ldap SERVER 'server_name'`
- `IDENTIFIED WITH kerberos` または `IDENTIFIED WITH kerberos REALM 'realm'`
- `IDENTIFIED WITH ssl_certificate CN 'mysite.com:user'`
- `IDENTIFIED WITH ssh_key BY KEY 'public_key' TYPE 'ssh-rsa', KEY 'another_public_key' TYPE 'ssh-ed25519'`
- `IDENTIFIED WITH http SERVER 'http_server'` または `IDENTIFIED WITH http SERVER 'http_server' SCHEME 'basic'`
- `IDENTIFIED BY 'qwerty'`

パスワードの複雑さの要件は[config.xml](/docs/ja/operations/configuration-files)で編集することができます。以下は、パスワードに少なくとも12文字を要求し、1つの数字を含む必要がある例です。各パスワードの複雑さルールは、パスワードと一致する正規表現とルールの説明が必要です。

```xml
<clickhouse>
    <password_complexity>
        <rule>
            <pattern>.{12}</pattern>
            <message>少なくとも12文字である必要があります</message>
        </rule>
        <rule>
            <pattern>\p{N}</pattern>
            <message>少なくとも1つの数字を含む必要があります</message>
        </rule>
    </password_complexity>
</clickhouse>
```

:::note
ClickHouse Cloudでは、デフォルトでパスワードは次の複雑さ要件を満たす必要があります:
- 少なくとも12文字である
- 少なくとも1つの数字を含む
- 少なくとも1つの大文字を含む
- 少なくとも1つの小文字を含む
- 少なくとも1つの特殊文字を含む
:::

## 例

1. 次のユーザー名は`name1`で、パスワードは不要です - これは明らかにセキュリティを提供しません:

    ```sql
    CREATE USER name1 NOT IDENTIFIED
    ```

2. 平文パスワードを指定するには:

    ```sql
    CREATE USER name2 IDENTIFIED WITH plaintext_password BY 'my_password'
    ```

    :::tip
    パスワードはSQLテキストファイルに`/var/lib/clickhouse/access`に保存されるので、`plaintext_password`を使用するのは良い考えではありません。代わりに次の例で示すように`sha256_password`を試してください...
    :::

3. 最も一般的なオプションは、SHA-256を使用してハッシュされたパスワードを使用することです。`IDENTIFIED WITH sha256_password`を指定すると、ClickHouseはパスワードをハッシュしてくれます。たとえば:

    ```sql
    CREATE USER name3 IDENTIFIED WITH sha256_password BY 'my_password'
    ```

    `name3`ユーザーは今 `my_password`を使ってログインでき、パスワードは上記のハッシュされた値として保存されます。次のSQLファイルが`/var/lib/clickhouse/access`に作成され、サーバー起動時に実行されます:

    ```bash
    /var/lib/clickhouse/access $ cat 3843f510-6ebd-a52d-72ac-e021686d8a93.sql
    ATTACH USER name3 IDENTIFIED WITH sha256_hash BY '0C268556C1680BEF0640AAC1E7187566704208398DA31F03D18C74F5C5BE5053' SALT '4FB16307F5E10048196966DD7E6876AE53DE6A1D1F625488482C75F14A5097C7';
    ```

    :::tip
    ユーザー名に対して既にハッシュ値と対応するソルト値を作成している場合、`IDENTIFIED WITH sha256_hash BY 'hash'`や`IDENTIFIED WITH sha256_hash BY 'hash' SALT 'salt'`を使用できます。`sha256_hash`の識別に`SALT`を使用する場合 - ハッシュは'password'と'salt'の結合から計算される必要があります。
    :::

4. `double_sha1_password`は通常必要ありませんが、MySQLインターフェースのようにそれを必要とするクライアントで作業する場合に便利です:

    ```sql
    CREATE USER name4 IDENTIFIED WITH double_sha1_password BY 'my_password'
    ```

    ClickHouseは次のクエリを生成し実行します:

    ```response
    CREATE USER name4 IDENTIFIED WITH double_sha1_hash BY 'CCD3A959D6A004B9C3807B728BC2E55B67E10518'
    ```

5. `bcrypt_password`はパスワードを保存する最も安全なオプションです。これは、[bcrypt](https://en.wikipedia.org/wiki/Bcrypt)アルゴリズムを使用しており、パスワードハッシュが侵害された場合でもブルートフォース攻撃に対して耐性があります。

    ```sql
    CREATE USER name5 IDENTIFIED WITH bcrypt_password BY 'my_password'
    ```

    この方法ではパスワードの長さは72文字に制限されています。bcryptのワークファクターパラメーターは、ハッシュの計算とパスワードの検証に必要な計算と時間の量を定義しており、サーバー設定で変更できます:

    ```xml
    <bcrypt_workfactor>12</bcrypt_workfactor>
    ```

    ワークファクターは4から31の間でなければならず、デフォルト値は12です。

6. パスワードの種類を省略することもできます:

    ```sql
    CREATE USER name6 IDENTIFIED BY 'my_password'
    ```

    この場合、ClickHouseはサーバー設定で指定されたデフォルトのパスワードタイプを使用します:

    ```xml
    <default_password_type>sha256_password</default_password_type>
    ```

    利用可能なパスワードタイプは、`plaintext_password`、`sha256_password`、`double_sha1_password`です。

7. 複数の認証方法を指定できます:

   ```sql
   CREATE USER user1 IDENTIFIED WITH plaintext_password by '1', bcrypt_password by '2', plaintext_password by '3''
   ```

注意事項:
1. 古いバージョンのClickHouseでは、複数の認証方法の構文がサポートされていない可能性があります。したがって、ClickHouseサーバーにそのようなユーザーが存在し、サポートされていないバージョンにダウングレードされた場合、そのようなユーザーは使用不能になり、一部のユーザー関連操作が壊れる可能性があります。ダウングレードするには、事前にすべてのユーザーを1つの認証方法に設定する必要があります。代替案として、サーバーが適切な手順なしでダウングレードされた場合、問題のあるユーザーを削除する必要があります。
2. セキュリティ上の理由から、`no_password`は他の認証方法と共存できません。したがって、クエリで認証方法が唯一の場合にのみ`no_password`を指定できます。

## ユーザーホスト

ユーザーホストは、ClickHouseサーバーに接続を確立できるホストです。ホストは`HOST`クエリセクションで次の方法で指定できます:

- `HOST IP 'ip_address_or_subnetwork'` — 指定されたIPアドレスまたは[サブネットワーク](https://en.wikipedia.org/wiki/Subnetwork)からのみユーザーはClickHouseサーバーに接続できます。例: `HOST IP '192.168.0.0/16'`, `HOST IP '2001:DB8::/32'`。本番環境での使用には、`HOST IP`要素（IPアドレスとそのマスク）のみを指定してください。`host`や`host_regexp`を使用すると余計な遅延が生じる可能性があるためです。
- `HOST ANY` — ユーザーはどこからでも接続できます。これはデフォルトのオプションです。
- `HOST LOCAL` — ユーザーはローカルのみ接続できます。
- `HOST NAME 'fqdn'` — ユーザーホストはFQDNとして指定できます。たとえば、`HOST NAME 'mysite.com'`。
- `HOST REGEXP 'regexp'` — ユーザーホストを指定する際に[pcre](http://www.pcre.org/)正規表現を使用できます。たとえば、`HOST REGEXP '.*\.mysite\.com'`。
- `HOST LIKE 'template'` — ユーザーホストをフィルターするために[LIKE](../../../sql-reference/functions/string-search-functions.md#function-like)演算子を使用することができます。たとえば、`HOST LIKE '%'`は`HOST ANY`と同じ意味になります。`HOST LIKE '%.mysite.com'`は`mysite.com`ドメイン内のすべてのホストをフィルタリングします。

ホストを指定する別の方法は、ユーザー名の後に`@`構文を使用することです。例:

- `CREATE USER mira@'127.0.0.1'` — `HOST IP`構文と同等です。
- `CREATE USER mira@'localhost'` — `HOST LOCAL`構文と同等です。
- `CREATE USER mira@'192.168.%.%'` — `HOST LIKE`構文と同等です。

:::tip
ClickHouseは`user_name@'address'`を一つのユーザー名として扱います。そのため、技術的には、同じ`user_name`と`@`の後の異なる構成を持つ複数のユーザーを作成することができます。しかし、それを行うことはお勧めしません。
:::

## VALID UNTIL句

認証方法の有効期限と、オプションでその時刻を指定することができます。文字列をパラメータとして受け取ります。日付と時刻には`YYYY-MM-DD [hh:mm:ss] [timezone]`形式を使用することを推奨します。既定では、このパラメータは`'infinity'`に設定されています。
`VALID UNTIL`句は、クエリで認証方法が指定されている場合に限り指定可能です。認証方法が指定されていない場合、`VALID UNTIL`句は既存のすべての認証方法に適用されます。

例:

- `CREATE USER name1 VALID UNTIL '2025-01-01'`
- `CREATE USER name1 VALID UNTIL '2025-01-01 12:00:00 UTC'`
- `CREATE USER name1 VALID UNTIL 'infinity'`
- ```CREATE USER name1 VALID UNTIL '2025-01-01 12:00:00 `Asia/Tokyo`'```
- `CREATE USER name1 IDENTIFIED WITH plaintext_password BY 'no_expiration', bcrypt_password BY 'expiration_set' VALID UNTIL '2025-01-01''`

## GRANTEES句

このユーザーが持つ[権限](../../../sql-reference/statements/grant.md#privileges)を付与できるユーザーまたはロールを指定します。このユーザーもまた、[GRANT OPTION](../../../sql-reference/statements/grant.md#granting-privilege-syntax)で付与されたすべての必要なアクセスを持っている場合にのみです。`GRANTEES`句のオプション:

- `user` — このユーザーが権限を付与できるユーザーを指定します。
- `role` — このユーザーが権限を付与できるロールを指定します。
- `ANY` — このユーザーは誰にでも権限を付与することができます。これはデフォルト設定です。
- `NONE` — このユーザーは誰にも権限を付与できません。

`EXCEPT`式を使用して、任意のユーザーまたはロールを除外できます。たとえば、`CREATE USER user1 GRANTEES ANY EXCEPT user2`と言えば、`user1`が`GRANT OPTION`付きでいくつかの権限を持っている場合、それらの権限を`user2`を除く誰にでも付与できるという意味です。

## 例

`qwerty`というパスワードで保護されたユーザーアカウント`mira`を作成します:

``` sql
CREATE USER mira HOST IP '127.0.0.1' IDENTIFIED WITH sha256_password BY 'qwerty';
```

`mira`はClickHouseサーバーが稼働しているホストでクライアントアプリケーションを起動する必要があります。

ユーザーアカウント`john`を作成し、ロールを割り当て、それらをデフォルトにします:

``` sql
CREATE USER john DEFAULT ROLE role1, role2;
```

将来のすべてのロールをデフォルトにするユーザーアカウント`john`を作成します:

``` sql
CREATE USER john DEFAULT ROLE ALL;
```

将来的に`john`にロールが割り当てられると、それが自動的にデフォルトになります。

ユーザーアカウント`john`を作成し、`role1`と`role2`を除く将来のすべてのロールをデフォルトにし:

``` sql
CREATE USER john DEFAULT ROLE ALL EXCEPT role1, role2;
```

ユーザーアカウント`john`を作成し、自分の権限を`jack`アカウントを持つユーザーに付与できるようにします:

``` sql
CREATE USER john GRANTEES jack;
```
