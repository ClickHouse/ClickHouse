---
description: 'ClickHouse の LDAP 認証設定ガイド'
slug: /operations/external-authenticators/ldap
title: 'LDAP'
doc_type: 'reference'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

LDAP サーバーは、ClickHouse ユーザーの認証に使用できます。これを実現するには、主に 2 つの方法があります。

* `users.xml` またはローカルのアクセス制御パスで定義されている既存ユーザーに対して、LDAP を外部認証機構として使用します。
* LDAP を外部ユーザーディレクトリとして使用し、ローカルで定義されていないユーザーでも、LDAP サーバー上に存在すれば認証できるようにします。

どちらの方法でも、設定の他の部分から参照できるように、ClickHouse の設定で内部名を持つ LDAP サーバーを定義する必要があります。

<div id="ldap-server-definition">
  ## LDAP サーバーの定義
</div>

LDAP サーバーを定義するには、`config.xml` に `ldap_servers` セクションを追加する必要があります。

**例**

```xml
<clickhouse>
    <!- ... -->
    <ldap_servers>
        <!- Typical LDAP server. -->
        <my_ldap_server>
            <host>localhost</host>
            <port>636</port>
            <bind_dn>uid={user_name},ou=users,dc=example,dc=com</bind_dn>
            <verification_cooldown>300</verification_cooldown>
            <follow_referrals>false</follow_referrals>
            <enable_tls>yes</enable_tls>
            <tls_minimum_protocol_version>tls1.2</tls_minimum_protocol_version>
            <tls_require_cert>demand</tls_require_cert>
            <tls_cert_file>/path/to/tls_cert_file</tls_cert_file>
            <tls_key_file>/path/to/tls_key_file</tls_key_file>
            <tls_ca_cert_file>/path/to/tls_ca_cert_file</tls_ca_cert_file>
            <tls_ca_cert_dir>/path/to/tls_ca_cert_dir</tls_ca_cert_dir>
            <tls_cipher_suite>ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:AES256-GCM-SHA384</tls_cipher_suite>
        </my_ldap_server>

        <!- Typical Active Directory with configured user DN detection for further role mapping. -->
        <my_ad_server>
            <host>localhost</host>
            <port>389</port>
            <bind_dn>EXAMPLE\{user_name}</bind_dn>
            <user_dn_detection>
                <base_dn>CN=Users,DC=example,DC=com</base_dn>
                <search_filter>(&amp;(objectClass=user)(sAMAccountName={user_name}))</search_filter>
            </user_dn_detection>
            <enable_tls>no</enable_tls>
        </my_ad_server>
    </ldap_servers>
</clickhouse>
```

`ldap_servers` セクションでは、異なる名前を付けて複数の LDAP サーバーを定義できます。

**パラメータ**

| パラメータ                          | デフォルト         | 説明                                                                                                                                                                                                                                                                                         |
| ------------------------------ | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `host`                         | —             | LDAPサーバーのホスト名または IP。 このパラメータは必須で、空にすることはできません。                                                                                                                                                                                                                                             |
| `port`                         | `636` / `389` | LDAPサーバーのポート。`enable_tls` が `yes` に設定されている場合のデフォルトは `636`、それ以外は `389` です。                                                                                                                                                                                                                  |
| `bind_dn`                      | —             | バインドに使用する DN を構築するためのテンプレート。生成される DN は、認証試行のたびに、テンプレート内のすべての `{user_name}` 部分文字列を実際のユーザー名に置き換えて構築されます。                                                                                                                                                                                     |
| `auth_dn_prefix`               | —             | **非推奨。** `bind_dn` の代替です。`bind_dn` と同時には使用できません。指定した場合、バインド DN は `auth_dn_prefix + {user_name} + auth_dn_suffix` として構築されます。たとえば、`auth_dn_prefix` を `uid=` に、`auth_dn_suffix` を `,ou=users,dc=example,dc=com` に設定すると、`bind_dn` を `uid={user_name},ou=users,dc=example,dc=com` に設定した場合と同等です。 |
| `auth_dn_suffix`               | —             | **非推奨。** `auth_dn_prefix` を参照してください。                                                                                                                                                                                                                                                       |
| `verification_cooldown`        | `0`           | バインド成功後、一定期間 (秒単位) は LDAPサーバーに問い合わせることなく、その後のすべての連続するリクエストでユーザーは認証成功済みと見なされます。`0` を指定するとキャッシュを無効にし、認証リクエストごとに LDAPサーバーへ問い合わせるようにします。                                                                                                                                                       |
| `follow_referrals`             | `false`       | サーバーから返された LDAP リファラルを LDAP クライアントライブラリが自動的に追跡することを許可するフラグ。主に Microsoft Active Directory 環境で関係します。この環境では、高いレベルのベース DN (例: `DC=example,DC=com`) に対するサブツリー検索で、リファラルや検索参照 (例: `DC=DomainDnsZones,...`) が返されることがあります。パーティションをまたぐ検索が明示的に必要な場合にのみ `true` を設定してください。                              |
| `enable_tls`                   | `yes`         | LDAPサーバーへのセキュア接続の使用を有効にするためのフラグ。平文の `ldap://` プロトコルには `no` (非推奨) 、SSL/TLS 上の LDAP である `ldaps://` プロトコルには `yes` (推奨) 、従来の StartTLS プロトコルには `starttls` (平文の `ldap://` プロトコルを TLS にアップグレード) を指定します。                                                                                           |
| `tls_minimum_protocol_version` | `tls1.2`      | SSL/TLS の最小プロトコルバージョン。使用できる値: `ssl2`, `ssl3`, `tls1.0`, `tls1.1`, `tls1.2`。                                                                                                                                                                                                                |
| `tls_require_cert`             | `demand`      | SSL/TLS ピア証明書の検証動作。使用できる値: `never`, `allow`, `try`, `demand`。                                                                                                                                                                                                                              |
| `tls_cert_file`                | —             | 証明書ファイルのパス。                                                                                                                                                                                                                                                                                |
| `tls_key_file`                 | —             | 証明書の秘密鍵ファイルのパス。                                                                                                                                                                                                                                                                            |
| `tls_ca_cert_file`             | —             | CA 証明書ファイルのパス。                                                                                                                                                                                                                                                                             |
| `tls_ca_cert_dir`              | —             | CA 証明書を含むディレクトリのパス。                                                                                                                                                                                                                                                                        |
| `tls_cipher_suite`             | —             | 許可する暗号スイート (OpenSSL 表記) 。                                                                                                                                                                                                                                                                  |
| `search_limit`                 | `256`         | このサーバー定義で実行される LDAP 検索クエリ (ユーザー DN の検出およびロールマッピング) で返すことができる最大エントリ数。                                                                                                                                                                                                                       |

**`user_dn_detection` サブパラメータ**

バインド済みユーザーの実際のユーザー DN を検出するための LDAP 検索パラメータのセクションです。これは主に、サーバーが Active Directory の場合に、後続のロールマッピングで使用する検索フィルタに使われます。結果として得られたユーザー DN は、許可されている箇所で `{user_dn}` 部分文字列を置き換える際に使用されます。デフォルトではユーザー DN はバインド DN と同じ値に設定されますが、検索が実行されると、実際に検出されたユーザー DN の値に更新されます。

| パラメータ           | デフォルト     | 説明                                                                                                                                                                                          |
| --------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `base_dn`       | —         | LDAP 検索のベース DN を構築するために使用するテンプレート。生成される DN は、LDAP 検索中にテンプレート内のすべての `{user_name}` および `{bind_dn}` 部分文字列を、実際のユーザー名とバインド DN に置き換えて構築されます。                                                      |
| `scope`         | `subtree` | LDAP 検索のスコープ。使用できる値: `base`, `one_level`, `children`, `subtree`。                                                                                                                            |
| `search_filter` | —         | LDAP 検索の検索フィルタを構築するために使用するテンプレート。生成されるフィルタは、LDAP 検索中にテンプレート内のすべての `{user_name}`、`{bind_dn}`、`{base_dn}` 部分文字列を、実際のユーザー名、バインド DN、ベース DN に置き換えて構築されます。特殊文字は XML 内で適切にエスケープする必要があることに注意してください。 |

<div id="ldap-external-authenticator">
  ## LDAP 外部認証
</div>

リモート LDAP サーバーは、ローカルで定義されたユーザー (`users.xml` またはローカルのアクセス制御パスで定義されたユーザー) のパスワードを検証する方法として使用できます。これを行うには、ユーザー定義で `password` などのセクションの代わりに、あらかじめ定義しておいた LDAP サーバー名を指定します。

ログインのたびに、ClickHouse は、指定された認証情報を使用して、[LDAP サーバーの定義](#ldap-server-definition) の `bind_dn` パラメーターで定義された DN への &quot;bind&quot; を試みます。これが成功すると、そのユーザーは認証されたものと見なされます。これは一般に &quot;simple bind&quot; 方式と呼ばれます。

**例**

```xml
<clickhouse>
    <!- ... -->
    <users>
        <!- ... -->
        <my_user>
            <!- ... -->
            <ldap>
                <server>my_ldap_server</server>
            </ldap>
        </my_user>
    </users>
</clickhouse>
```

`my_user` ユーザーは `my_ldap_server` を参照している点に注意してください。この LDAP サーバーは、前述のとおりメインの `config.xml` ファイルで設定しておく必要があります。

SQL ベースの [Access Control and Account Management](/ja/operations/access-rights#access-control-usage) が有効になっている場合、LDAP サーバーで認証されるユーザーは、[CREATE USER](/ja/sql-reference/statements/create/user) ステートメントを使って作成することもできます。

```sql title="Query"
CREATE USER my_user IDENTIFIED WITH ldap SERVER 'my_ldap_server';
```

<div id="ldap-external-user-directory">
  ## LDAP 外部ユーザーディレクトリ
</div>

ローカルで定義されたユーザーに加えて、リモート LDAP サーバーをユーザー定義のソースとして使用できます。そのためには、`config.xml` ファイルの `users_directories` セクション内の `ldap` セクションで、あらかじめ定義した LDAP サーバー名 ([LDAP サーバーの定義](#ldap-server-definition) を参照) を指定します。

ログインの試行ごとに、ClickHouse はまずローカルでユーザー定義を探し、通常どおり認証を試みます。ユーザーが定義されていない場合、ClickHouse はその定義が外部 LDAP ディレクトリに存在するとみなし、指定された認証情報を使用して LDAP サーバー上の指定された DN に対して &quot;bind&quot; を試みます。成功すると、そのユーザーは存在し、認証済みであると見なされます。ユーザーには、`roles` セクションで指定されたリストのロールが割り当てられます。さらに、`role_mapping` セクションも設定されている場合は、LDAP の &quot;search&quot; を実行し、その結果を変換してロール名として扱い、ユーザーに割り当てることもできます。これらはすべて、SQL ベースの [Access Control and Account Management](/ja/operations/access-rights#access-control-usage) が有効になっており、[CREATE ROLE](/ja/sql-reference/statements/create/role) ステートメントを使用してロールが作成されていることを前提としています。

**例**

`config.xml` に記述します。

```xml
<clickhouse>
    <!- ... -->
    <user_directories>
        <!- Typical LDAP server. -->
        <ldap>
            <server>my_ldap_server</server>
            <roles>
                <my_local_role1 />
                <my_local_role2 />
            </roles>
            <role_mapping>
                <base_dn>ou=groups,dc=example,dc=com</base_dn>
                <scope>subtree</scope>
                <search_filter>(&amp;(objectClass=groupOfNames)(member={bind_dn}))</search_filter>
                <attribute>cn</attribute>
                <prefix>clickhouse_</prefix>
            </role_mapping>
        </ldap>

        <!- Typical Active Directory with role mapping that relies on the detected user DN. -->
        <ldap>
            <server>my_ad_server</server>
            <role_mapping>
                <base_dn>CN=Users,DC=example,DC=com</base_dn>
                <attribute>CN</attribute>
                <scope>subtree</scope>
                <search_filter>(&amp;(objectClass=group)(member={user_dn}))</search_filter>
                <prefix>clickhouse_</prefix>
            </role_mapping>
        </ldap>
    </user_directories>
</clickhouse>
```

`user_directories` セクション内の `ldap` セクションで参照される `my_ldap_server` は、`config.xml` で設定された、事前に定義済みの LDAP サーバーである必要があります ([LDAP サーバーの定義](#ldap-server-definition) を参照) 。

**パラメータ**

| Parameter | Default | Description                                                                                                               |
| --------- | ------- | ------------------------------------------------------------------------------------------------------------------------- |
| `server`  | —       | 上記の `ldap_servers` 設定セクションで定義されている LDAP サーバー名のいずれかです。このパラメータは必須で、空にすることはできません。                                            |
| `roles`   | —       | LDAP サーバーから取得した各ユーザーに割り当てる、ローカルで定義されたロールの一覧を含むセクションです。ここでロールが指定されておらず、かつロールマッピング (後述) でも割り当てられない場合、ユーザーは認証後に何の操作も実行できません。 |

**`role_mapping` サブパラメータ**

LDAP 検索パラメータとマッピングルールを含むセクションです。ユーザーが認証されると、LDAP にバインドされたまま、`search_filter` とログインしたユーザー名を使って LDAP 検索が実行されます。その検索で見つかった各エントリについて、指定された属性の値が抽出されます。指定されたプレフィックスを持つ各属性値については、そのプレフィックスが削除され、残りの値が ClickHouse で定義されたローカルロール名になります。このロールは、あらかじめ [CREATE ROLE](/ja/sql-reference/statements/create/role) ステートメントで作成しておく必要があります。同じ `ldap` セクション内に複数の `role_mapping` セクションを定義でき、それらはすべて適用されます。

| パラメータ           | デフォルト     | 説明                                                                                                                                                                                                                 |
| --------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `base_dn`       | —         | LDAP 検索のベース DN を構築するための Template です。生成される DN は、LDAP 検索のたびに、テンプレート内のすべての `{user_name}`、`{bind_dn}`、`{user_dn}` の部分文字列を実際のユーザー名、バインド DN、ユーザー DN に置き換えて構築されます。                                                        |
| `scope`         | `subtree` | LDAP 検索のスコープです。指定可能な値: `base`, `one_level`, `children`, `subtree`。                                                                                                                                                 |
| `search_filter` | —         | LDAP 検索の検索フィルタを構築するための Template です。生成されるフィルタは、LDAP 検索のたびに、テンプレート内のすべての `{user_name}`、`{bind_dn}`、`{user_dn}`、`{base_dn}` の部分文字列を実際のユーザー名、バインド DN、ユーザー DN、ベース DN に置き換えて構築されます。XML では特殊文字を適切にエスケープする必要がある点に注意してください。 |
| `attribute`     | `cn`      | LDAP 検索で返される値の属性名です。                                                                                                                                                                                               |
| `prefix`        | 空         | LDAP 検索で返される元の文字列リスト内の各文字列の先頭にあることが想定されるプレフィックスです。プレフィックスは元の文字列から削除され、結果の文字列はローカルロール名として扱われます。                                                                                                                     |