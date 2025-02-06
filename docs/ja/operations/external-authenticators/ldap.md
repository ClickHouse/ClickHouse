---
slug: /ja/operations/external-authenticators/ldap
title: "LDAP"
---
import SelfManaged from '@site/docs/ja/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

LDAPサーバーはClickHouseユーザーの認証に使用できます。このために二つの異なるアプローチがあります:

- 既存のユーザーに対して、`users.xml`やローカルアクセスコントロールパスに定義されているユーザーに対して、LDAPを外部認証器として使用する。
- ローカルに定義されていないユーザーを、LDAPサーバー上に存在する場合に認証するため、LDAPを外部ユーザーディレクトリとして使用する。

これらのアプローチのどちらも、ClickHouseの設定内で他の部分が参照可能な内部名称のLDAPサーバーを定義する必要があります。

## LDAP サーバーの定義 {#ldap-server-definition}

LDAPサーバーを定義するには、`config.xml`に`ldap_servers`セクションを追加する必要があります。

**例**

```xml
<clickhouse>
    <!- ... -->
    <ldap_servers>
        <!- 一般的なLDAPサーバー。 -->
        <my_ldap_server>
            <host>localhost</host>
            <port>636</port>
            <bind_dn>uid={user_name},ou=users,dc=example,dc=com</bind_dn>
            <verification_cooldown>300</verification_cooldown>
            <enable_tls>yes</enable_tls>
            <tls_minimum_protocol_version>tls1.2</tls_minimum_protocol_version>
            <tls_require_cert>demand</tls_require_cert>
            <tls_cert_file>/path/to/tls_cert_file</tls_cert_file>
            <tls_key_file>/path/to/tls_key_file</tls_key_file>
            <tls_ca_cert_file>/path/to/tls_ca_cert_file</tls_ca_cert_file>
            <tls_ca_cert_dir>/path/to/tls_ca_cert_dir</tls_ca_cert_dir>
            <tls_cipher_suite>ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:AES256-GCM-SHA384</tls_cipher_suite>
        </my_ldap_server>

        <!- ロールマッピングのためにユーザーDN検出を設定した典型的なActive Directory。 -->
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

注意: `ldap_servers`セクション内で異なる名前を使用して複数のLDAPサーバーを定義できます。

**パラメーター**

- `host` — LDAPサーバーのホスト名またはIP。このパラメーターは必須で空にはできません。
- `port` — LDAPサーバーのポート。`enable_tls`が`true`に設定されている場合のデフォルトは`636`、それ以外の場合は`389`です。
- `bind_dn` — バインドに使用されるDNを構築するためのテンプレート。
    - 各認証試行時に、テンプレート中のすべての`{user_name}`サブストリングは実際のユーザー名に置き換えられ、この結果得られたDNが構築されます。
- `user_dn_detection` — バインドされたユーザーの実際のユーザーDNを検出するためのLDAP検索パラメータのセクション。
    - これは主にActive Directoryサーバーでのロールマッピングをさらに進めるための検索フィルタです。結果のユーザーDNは、許可される場所で`{user_dn}`サブストリングを置き換えるために使用されます。デフォルトでは、ユーザーDNはバインドDNと同等に設定されていますが、検索が行われた場合は、検出された実際のユーザーDNの値で更新されます。
        - `base_dn` — LDAP検索のためのベースDNを構築するためのテンプレート。
            - 実際のユーザー名とバインドDNでテンプレート中のすべての`{user_name}`および`{bind_dn}`サブストリングを置き換えることにより構築されます。
        - `scope` — LDAP検索のスコープ。
            - 許容値は: `base`, `one_level`, `children`, `subtree`（デフォルト）です。
        - `search_filter` — LDAP検索のための検索フィルタを構築するためのテンプレート。
            - LDAP検索中に実際のユーザー名、バインドDN、およびベースDNでテンプレート中の`{user_name}`、`{bind_dn}`、および`{base_dn}`サブストリングをすべて置き換えることにより構築されます。
            - 特殊文字はXMLで適切にエスケープされる必要があります。
- `verification_cooldown` — 成功したバインド試行後、LDAPサーバーに接触せずに、すべての連続したリクエストでユーザーが成功したと見なされる秒数。
    - キャッシュを無効にして各認証要求でLDAPサーバーに接触を強制するには`0`（デフォルト）を指定します。
- `enable_tls` — LDAPサーバーへのセキュアな接続を行うかどうかのフラグ。
    - プレーンテキストの`ldap://`プロトコルを指定するには`no`（推奨されません）。
    - SSL/TLSを介したLDAP `ldaps://`プロトコルを指定するには`yes`（推奨、デフォルト）を指定します。
    - レガシーのStartTLSプロトコル（プレーンテキストの`ldap://`プロトコルをTLSにアップグレード）を指定するには`starttls`を指定します。
- `tls_minimum_protocol_version` — SSL/TLSの最小プロトコルバージョン。
    - 許容値は: `ssl2`, `ssl3`, `tls1.0`, `tls1.1`, `tls1.2`（デフォルト）です。
- `tls_require_cert` — SSL/TLSピア証明書の検証動作。
    - 許容値は: `never`, `allow`, `try`, `demand`（デフォルト）です。
- `tls_cert_file` — 証明書ファイルへのパス。
- `tls_key_file` — 証明書キーのファイルパス。
- `tls_ca_cert_file` — CA証明書ファイルへのパス。
- `tls_ca_cert_dir` — CA証明書が含まれるディレクトリへのパス。
- `tls_cipher_suite` — 許可される暗号スイート（OpenSSL表記で）。

## LDAP 外部認証器 {#ldap-external-authenticator}

リモートLDAPサーバーを使用して、ローカルに定義されたユーザー（`users.xml`やローカルアクセスコントロールパスで定義されたユーザー）に対するパスワードを検証することができます。このためには、ユーザー定義の中の`password`や類似セクションの代わりに、前述のLDAPサーバー名を指定します。

各ログイン試行時に、ClickHouseは[LDAPサーバーの定義](#ldap-server-definition)に定義されている`bind_dn`パラメータで指定されたDNに、提供された資格情報を使用して「バインド」しようとし、成功した場合にはユーザーが認証されたと見なされます。これはしばしば単純バインドメソッドと呼ばれます。

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

注意: ユーザー`my_user`は`my_ldap_server`を参照しています。このLDAPサーバーは、前述のようにメインの`config.xml`ファイルに設定されていなければなりません。

SQL駆動の[アクセスコントロールとアカウント管理](/docs/ja/guides/sre/user-management/index.md#access-control)が有効になっている場合、LDAPサーバーによって認証されたユーザーは[CREATE USER](/docs/ja/sql-reference/statements/create/user.md#create-user-statement)文を使用して作成することもできます。

クエリ:

```sql
CREATE USER my_user IDENTIFIED WITH ldap SERVER 'my_ldap_server';
```

## LDAP 外部ユーザーディレクトリ {#ldap-external-user-directory}

ローカルに定義されたユーザーに加えて、リモートLDAPサーバーをユーザー定義のソースとして使用することもできます。このためには、`config.xml`ファイルの`users_directories`セクション内の`ldap`セクションで、前述のLDAPサーバー名を指定します（[LDAPサーバーの定義](#ldap-server-definition)を参照）。

各ログイン試行時に、ClickHouseはユーザー定義をローカルで見つけ、それを通常どおり認証しようとします。ユーザーが定義されていない場合、ClickHouseは外部LDAPディレクトリにその定義が存在すると仮定し、提供された資格情報でLDAPサーバーの指定されたDNに「バインド」しようとします。これに成功した場合、ユーザーは存在し、認証されたと見なされます。ユーザーには`roles`セクションで指定されたリストからロールが割り当てられます。さらに、LDAP「検索」が実行され、その結果を役割名として変換し、`role_mapping`セクションも設定されている場合にはユーザーに割り当てることもできます。これらはすべて、SQL駆動の[アクセスコントロールとアカウント管理](/docs/ja/guides/sre/user-management/index.md#access-control)が有効になっており、[CREATE ROLE](/docs/ja/sql-reference/statements/create/role.md#create-role-statement)ステートメントを使用してロールが作成されていることを前提としています。

**例**

`config.xml`に入ります。

```xml
<clickhouse>
    <!- ... -->
    <user_directories>
        <!- 一般的なLDAPサーバー。 -->
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

        <!- 検出されたユーザーDNに依存した役割マッピングを持つ典型的なActive Directory。 -->
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

注意: `user_directories`セクション内の`ldap`セクションで参照されている`my_ldap_server`は、前述のように`config.xml`で設定された事前に定義されたLDAPサーバーでなければなりません（[LDAPサーバーの定義](#ldap-server-definition)を参照）。

**パラメーター**

- `server` — 上記の`ldap_servers`構成セクションで定義されたLDAPサーバー名の1つ。このパラメーターは必須で、空にすることはできません。
- `roles` — LDAPサーバーから取得した各ユーザーに割り当てられる、ローカルに定義されたロールのリストを含むセクション。
    - ここで指定されたロールがない場合、または下記のロールマッピング中に割り当てられたロールがない場合、ユーザーは認証後にいかなる操作も行うことができません。
- `role_mapping` — LDAP検索パラメーターとマッピングルールを持つセクション。
    - ユーザーが認証される際、LDAPにバインドされたまま、`search_filter`とログインしたユーザーの名前を使用してLDAP検索が実行されます。その検索中に見つかった各エントリに対して、指定された属性の値が抽出されます。指定されたプレフィックスを持つ各属性値について、プレフィックスが削除され、結果の値がClickHouseで定義されたローカルロールの名前になります。これは[CREATE ROLE](/docs/ja/sql-reference/statements/create/role.md#create-role-statement)ステートメントで事前に作成されていることが期待されます。
    - 同じ`ldap`セクション内に複数の`role_mapping`セクションを定義することができます。それらはすべて適用されます。
        - `base_dn` — LDAP検索のためのベースDNを構築するためのテンプレート。
            - LDAP検索中に実際のユーザー名、バインドDN、およびユーザーDNでテンプレート中の`{user_name}`、`{bind_dn}`、および`{user_dn}`サブストリングをすべて置き換えることにより構築されます。
        - `scope` — LDAP検索のスコープ。
            - 許容値は: `base`, `one_level`, `children`, `subtree`（デフォルト）です。
        - `search_filter` — LDAP検索のための検索フィルタを構築するためのテンプレート。
            - LDAP検索中に実際のユーザー名、バインドDN、ユーザーDN、およびベースDNでテンプレート中の`{user_name}`、`{bind_dn}`、`{user_dn}`、および`{base_dn}`サブストリングをすべて置き換えることにより構築されます。
            - 特殊文字はXMLで適切にエスケープされる必要があります。
        - `attribute` — LDAP検索によって返される値を持つ属性名。デフォルトでは`cn`です。
        - `prefix` — LDAP検索によって返される元の文字列リストの各文字列の前に存在すると予想されるプレフィックス。プレフィックスは元の文字列から削除され、その結果得られた文字列はローカルロール名として扱われます。デフォルトでは空です。
