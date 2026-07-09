---
description: 'USER に関するドキュメント'
sidebar_label: 'USER'
sidebar_position: 39
slug: /sql-reference/statements/create/user
title: 'CREATE USER'
doc_type: 'reference'
---

[ユーザーアカウント](../../../guides/sre/user-management/index.md#user-account-management)を作成します。

構文:

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

`ON CLUSTER`句を使用すると、クラスター上でユーザーを作成できます。詳しくは、[Distributed DDL](../../../sql-reference/distributed-ddl.md)を参照してください。

<div id="identification">
  ## 識別
</div>

ユーザーを識別する方法は複数あります。

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

パスワードの複雑性要件は [config.xml](/ja/operations/configuration-files) で編集できます。以下は、パスワードを 12 文字以上とし、数字を 1 文字以上含めることを必須とする設定例です。各パスワード複雑性ルールでは、パスワードに対して照合する正規表現と、そのルールの説明を指定する必要があります。

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
ClickHouse Cloud では、デフォルトでパスワードに次の複雑さの要件が適用されます。

* 12 文字以上であること
* 数字を少なくとも 1 文字含むこと
* 英大文字を少なくとも 1 文字含むこと
* 英小文字を少なくとも 1 文字含むこと
* 特殊文字を少なくとも 1 文字含むこと
  :::

<div id="examples">
  ## 例
</div>

1. 次のユーザー名は `name1` で、パスワードは不要です。言うまでもなく、これはほとんどセキュリティを提供しません。

   ```sql
   CREATE USER name1 NOT IDENTIFIED
   ```

2. 平文パスワードを指定するには、次のようにします:

   ```sql
   CREATE USER name2 IDENTIFIED WITH plaintext_password BY 'my_password'
   ```

   :::tip
   パスワードは `/var/lib/clickhouse/access` 内の SQL テキストファイルに保存されるため、`plaintext_password` の使用は推奨されません。代わりに、次に示す `sha256_password` の使用を検討してください。
   :::

3. 最も一般的なのは、SHA-256 でハッシュ化したパスワードを使用する方法です。`IDENTIFIED WITH sha256_password` を指定すると、ClickHouse がパスワードを自動的にハッシュ化します。例えば:

   ```sql
   CREATE USER name3 IDENTIFIED WITH sha256_password BY 'my_password'
   ```

   `name3` ユーザーは `my_password` でログインできますが、保存されるのは上記のハッシュ値です。次の SQL ファイルが `/var/lib/clickhouse/access` に作成され、サーバーの起動時に実行されます:

   ```bash
   /var/lib/clickhouse/access $ cat 3843f510-6ebd-a52d-72ac-e021686d8a93.sql
   ATTACH USER name3 IDENTIFIED WITH sha256_hash BY '0C268556C1680BEF0640AAC1E7187566704208398DA31F03D18C74F5C5BE5053' SALT '4FB16307F5E10048196966DD7E6876AE53DE6A1D1F625488482C75F14A5097C7';
   ```

   :::tip
   ユーザー名に対応するハッシュ値と salt 値をすでに作成済みであれば、`IDENTIFIED WITH sha256_hash BY 'hash'` または `IDENTIFIED WITH sha256_hash BY 'hash' SALT 'salt'` を使用できます。`SALT` を指定して `sha256_hash` を使う場合、ハッシュは &#39;password&#39; と &#39;salt&#39; を連結した値から計算する必要があります。
   :::

4. `double_sha1_password` は通常あまり必要ありませんが、これを必要とするクライアント (MySQL インターフェイス など) を扱う場合に便利です:

   ```sql
   CREATE USER name4 IDENTIFIED WITH double_sha1_password BY 'my_password'
   ```

   ClickHouse は次のクエリを生成して実行します:

   ```response
   CREATE USER name4 IDENTIFIED WITH double_sha1_hash BY 'CCD3A959D6A004B9C3807B728BC2E55B67E10518'
   ```

5. `bcrypt_password` は、パスワード保存において最も安全な選択肢です。[bcrypt](https://en.wikipedia.org/wiki/Bcrypt) アルゴリズムを使用しており、パスワードハッシュが侵害された場合でも総当たり攻撃への耐性があります。

   ```sql
   CREATE USER name5 IDENTIFIED WITH bcrypt_password BY 'my_password'
   ```

   この方式では、パスワードの長さは 72 文字までに制限されます。
   ハッシュの計算とパスワード検証に必要な計算量と時間を定義する bcrypt の work factor パラメーターは、サーバー設定で変更できます:

   ```xml
   <bcrypt_workfactor>12</bcrypt_workfactor>
   ```

   work factor は 4 から 31 の範囲で指定する必要があり、デフォルト値は 12 です。

   :::warning
   認証頻度の高いアプリケーションでは、
   work factor が高い場合の
   bcrypt の計算オーバーヘッドを考慮し、
   別の認証方式の利用を検討してください。
   :::

6. パスワードの種類は省略することもできます:

   ```sql
   CREATE USER name6 IDENTIFIED BY 'my_password'
   ```

   この場合、ClickHouse はサーバー設定で指定されたデフォルトのパスワード種別を使用します:

   ```xml
   <default_password_type>sha256_password</default_password_type>
   ```

   使用可能なパスワード種別は次のとおりです: `plaintext_password`, `sha256_password`, `double_sha1_password`.

7. 複数の認証方式を指定できます:

   ```sql
   CREATE USER user1 IDENTIFIED WITH plaintext_password by '1', bcrypt_password by '2', plaintext_password by '3''
   ```

注記:

1. 古いバージョンのClickHouseでは、複数の認証方式を指定する構文がサポートされていない場合があります。そのため、ClickHouse serverにそのようなユーザーが存在する状態で、この構文をサポートしていないバージョンへダウングレードすると、それらのユーザーは使用できなくなり、ユーザー関連の一部の操作も正常に機能しなくなります。問題なくダウングレードするには、事前にすべてのユーザーが単一の認証方式のみを持つように設定しておく必要があります。また、適切な手順を踏まずにserverをダウングレードしてしまった場合は、問題のあるユーザーを削除してください。
2. セキュリティ上の理由から、`no_password` は他の認証方式と併用できません。したがって、`no_password` を指定できるのは、それがクエリ内で唯一の認証方式である場合に限られます。

<div id="user-host">
  ## ユーザーホスト
</div>

ユーザーホストとは、ClickHouse server への接続を確立できるホストのことです。ホストは、`HOST` クエリセクションで次の方法により指定できます。

* `HOST IP 'ip_address_or_subnetwork'` — ユーザーは、指定された IP アドレスまたは [サブネットワーク](https://en.wikipedia.org/wiki/Subnetwork) からのみ ClickHouse server に接続できます。例: `HOST IP '192.168.0.0/16'`、`HOST IP '2001:DB8::/32'`。本番環境で使用する場合は、`host` や `host_regexp` を使用すると余分なレイテンシが発生する可能性があるため、`HOST IP` 要素 (IP アドレスとそのマスク) のみを指定してください。
* `HOST ANY` — ユーザーは任意の場所から接続できます。これはデフォルトのオプションです。
* `HOST LOCAL` — ユーザーはローカルからのみ接続できます。
* `HOST NAME 'fqdn'` — ユーザーホストは FQDN として指定できます。たとえば、`HOST NAME 'mysite.com'` です。
* `HOST REGEXP 'regexp'` — ユーザーホストの指定時には [pcre](http://www.pcre.org/) 正規表現を使用できます。たとえば、`HOST REGEXP '.*\.mysite\.com'` です。
* `HOST LIKE 'template'` — [LIKE](/ja/sql-reference/functions/string-search-functions#like) 演算子を使用してユーザーホストを絞り込めます。たとえば、`HOST LIKE '%'` は `HOST ANY` と同等であり、`HOST LIKE '%.mysite.com'` は `mysite.com` ドメイン内のすべてのホストを絞り込みます。

ホストを指定する別の方法として、ユーザー名の後に続けて `@` 構文を使用する方法があります。例:

* `CREATE USER mira@'127.0.0.1'` — `HOST IP` 構文と同等です。
* `CREATE USER mira@'localhost'` — `HOST LOCAL` 構文と同等です。
* `CREATE USER mira@'192.168.%.%'` — `HOST LIKE` 構文と同等です。

:::tip
ClickHouse は `user_name@'address'` をユーザー名全体として扱います。したがって、技術的には、同じ `user_name` で `@` の後の部分だけが異なる複数のユーザーを作成できます。ただし、そのような使い方は推奨しません。
:::

<div id="valid-until-clause">
  ## VALID UNTIL 句
</div>

認証方式の有効期限の日付と、必要に応じて時刻を指定できます。パラメータには文字列を指定します。datetime には `YYYY-MM-DD [hh:mm:ss] [timezone]` フォーマットを使用することを推奨します。ここで `[timezone]` は `+09:00` のような数値オフセット、または `UTC`、`GMT`、`Z`、`MSK`、`MSD` のいずれかである必要があります。`Asia/Tokyo` のような名前付き IANA ゾーンは認識されません (以下の注記を参照) 。デフォルトでは、このパラメータは `'infinity'` です。
`VALID UNTIL` 句は、クエリ内で認証方式がまったく指定されていない場合を除き、認証方式とあわせてのみ指定できます。この場合、`VALID UNTIL` 句は既存のすべての認証方式に適用されます。

例:

* `CREATE USER name1 VALID UNTIL '2025-01-01'`
* `CREATE USER name1 VALID UNTIL '2025-01-01 12:00:00 UTC'`
* `CREATE USER name1 VALID UNTIL '2025-01-01 12:00:00 +09:00'`
* `CREATE USER name1 VALID UNTIL 'infinity'`
* `CREATE USER name1 IDENTIFIED WITH plaintext_password BY 'no_expiration', bcrypt_password BY 'expiration_set' VALID UNTIL '2025-01-01'`

:::note
datetime 文字列は `parseDateTimeBestEffort` によって解析されます。この関数が認識するタイムゾーンのトークンは `UTC`、`GMT`、`Z`、`MSK`、`MSD`、および `+09:00` や `-05:00` のような数値オフセットのみです。`Asia/Tokyo` や `Europe/London` のような名前付き IANA タイムゾーンはサポートされていません。また、夏時間を採用している地域では固定オフセットは IANA ゾーンと同等ではないため、エンコードする対象の日付に対して正しいオフセットを計算する必要があります。
:::

<div id="grantees-clause">
  ## GRANTEES 句
</div>

このユーザーに、必要なすべてのアクセス権が [GRANT OPTION](../../../sql-reference/statements/grant.md#granting-privilege-syntax) 付きで付与されていることを条件として、このユーザーから [権限](../../../sql-reference/statements/grant.md#privileges) を受け取ることができるユーザーまたはロールを指定します。`GRANTEES` 句のオプションは次のとおりです。

* `user` — このユーザーが権限を付与できるユーザーを指定します。
* `role` — このユーザーが権限を付与できるロールを指定します。
* `ANY` — このユーザーは誰にでも権限を付与できます。これはデフォルト設定です。
* `NONE` — このユーザーは誰にも権限を付与できません。

`EXCEPT` 式を使用すると、任意のユーザーまたはロールを除外できます。たとえば、`CREATE USER user1 GRANTEES ANY EXCEPT user2` です。これは、`user1` に `GRANT OPTION` 付きで何らかの権限が付与されている場合、その権限を `user2` を除く誰にでも付与できることを意味します。

<div id="examples">
  ## 例
</div>

パスワード `qwerty` で保護されたユーザーアカウント `mira` を作成します。

```sql
CREATE USER mira HOST IP '127.0.0.1' IDENTIFIED WITH sha256_password BY 'qwerty';
```

`mira` は、ClickHouse server が稼働しているホスト上でクライアントアプリを起動する必要があります。

ユーザーアカウント `john` を作成し、ロールを割り当てます:

```sql
CREATE USER john ROLE role1, role2;
```

ユーザーアカウント `john` を作成し、ロールを割り当て、その一部をデフォルトに設定します:

```sql
CREATE USER john ROLE role1, role2 DEFAULT ROLE role1;
```

OR

```sql
CREATE USER john ROLE role1, role2 DEFAULT ROLE ALL EXCEPT role2;
```

ユーザーアカウント `john` を作成し、`jack` アカウントのユーザーに自身の権限を付与できるようにします:

```sql
CREATE USER john GRANTEES jack;
```

クエリパラメータを使用して、ユーザーアカウント `john` を作成します:

```sql
SET param_user=john;
CREATE USER {user:Identifier};
```