---
description: 'ユーザーとロールを設定するための設定項目。'
sidebar_label: 'ユーザー設定'
sidebar_position: 63
slug: /operations/settings/settings-users
title: 'ユーザーとロールの設定'
doc_type: 'reference'
---

`users.xml` 設定ファイルの `users` セクションには、ユーザー設定が含まれます。

:::note
ClickHouse はユーザー管理のための [SQL ベースのワークフロー](/ja/operations/access-rights#access-control-usage) もサポートしています。こちらの使用を推奨します。
:::

`users` セクションの構造:

```xml
<users>
    <!-- If user name was not specified, 'default' user is used. -->
    <user_name>
        <!-- Exactly one authentication method may be specified at the users.user_name level. For example: -->
        <password></password>
        <!-- Or (exclusive) -->
        <password_sha256_hex></password_sha256_hex>
 
        <!-- Or (exclusive) (N.B. multiple SSH keys are allowed for backwards compatibility) -->
        <ssh_keys>
            <ssh_key>
                <type>ssh-ed25519</type>
                <base64_key>AAAAC3NzaC1lZDI1NTE5AAAAIDNf0r6vRl24Ix3tv2IgPmNPO2ATa2krvt80DdcTatLj</base64_key>
            </ssh_key>
            <ssh_key>
                <type>ecdsa-sha2-nistp256</type>
                <base64_key>AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBNxeV2uN5UY6CUbCzTA1rXfYimKQA5ivNIqxdax4bcMXz4D0nSk2l5E1TkR5mG8EBWtmExSPbcEPJ8V7lyWWbA8=</base64_key>
            </ssh_key>
            <ssh_key>
                <type>ssh-rsa</type>
                <base64_key>AAAAB3NzaC1yc2EAAAADAQABAAABgQCpgqL1SHhPVBOTFlOm0pu+cYBbADzC2jL41sPMawYCJHDyHuq7t+htaVVh2fRgpAPmSEnLEC2d4BEIKMtPK3bfR8plJqVXlLt6Q8t4b1oUlnjb3VPA9P6iGcW7CV1FBkZQEVx8ckOfJ3F+kI5VsrRlEDgiecm/C1VPl0/9M2llW/mPUMaD65cM9nlZgM/hUeBrfxOEqM11gDYxEZm1aRSbZoY4dfdm3vzvpSQ6lrCrkjn3X2aSmaCLcOWJhfBWMovNDB8uiPuw54g3ioZ++qEQMlfxVsqXDGYhXCrsArOVuW/5RbReO79BvXqdssiYShfwo+GhQ0+aLWMIW/jgBkkqx/n7uKLzCMX7b2F+aebRYFh+/QXEj7SnihdVfr9ud6NN3MWzZ1ltfIczlEcFLrLJ1Yq57wW6wXtviWh59WvTWFiPejGjeSjjJyqqB49tKdFVFuBnIU5u/bch2DXVgiAEdQwUrIp1ACoYPq22HFFAYUJrL32y7RxX3PGzuAv3LOc=</base64_key>
            </ssh_key>
        </ssh_keys>

        <!-- Or (exclusive) for multiple authentication methods: -->
        <auth_methods>
            <method1>
                <password></password>
            </method1>
            <method2>
                <password_sha256_hex></password_sha256_hex>
            </method2>
            <!-- ... -->
            <methodN>
                <!-- ... -->
            </methodN>
        </auth_methods>

        <access_management>0|1</access_management>

        <networks incl="networks" replace="replace">
        </networks>

        <profile>profile_name</profile>

        <quota>default</quota>
        <default_database>default</default_database>
        <databases>
            <database_name>
                <table_name>
                    <filter>expression</filter>
                </table_name>
            </database_name>
        </databases>

        <grants>
            <query>GRANT SELECT ON system.*</query>
        </grants>
    </user_name>
    <!-- Other users settings -->
</users>
```

<div id="user-namepassword">
  ### user_name/password
</div>

パスワードは、平文または SHA256 (16進形式) で指定できます。

* パスワードを平文で設定する場合 (**非推奨**) は、`password` 要素に指定します。

  たとえば、`<password>qwerty</password>` です。パスワードは空のままにすることもできます。

<a id="password_sha256_hex" />

* SHA256 ハッシュでパスワードを設定する場合は、`password_sha256_hex` 要素に指定します。

  たとえば、`<password_sha256_hex>65e84be33532fb784c48129675f9eff3a682b27168c0ea744b2cf58ee02337c5</password_sha256_hex>` です。

  シェルからパスワードを生成する例:

  ```bash
  PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha256sum | tr -d '-'
  ```

  結果の 1 行目がパスワード、2 行目が対応する SHA256 ハッシュです。

<a id="password_double_sha1_hex" />

* MySQL clients との互換性のため、パスワードはダブル SHA1 ハッシュでも指定できます。`password_double_sha1_hex` 要素に指定します。

  たとえば、`<password_double_sha1_hex>08b4a0f1de6ad37da17359e592c8d74788a83eb0</password_double_sha1_hex>` です。

  シェルからパスワードを生成する例:

  ```bash
  PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha1sum | tr -d '-' | xxd -r -p | sha1sum | tr -d '-'
  ```

  結果の 1 行目がパスワード、2 行目が対応するダブル SHA1 ハッシュです。

<div id="totp-authentication-configuration">
  ### TOTP認証の設定
</div>

Time-Based One-Time Password (TOTP) は、有効時間が限られた一時的なアクセスコードを生成することで、ClickHouseユーザーの認証に使用できます。
このTOTP認証方式は [RFC 6238](https://datatracker.ietf.org/doc/html/rfc6238) に準拠しているため、Google Authenticator や 1Password など、一般的なTOTPアプリや同様のツールと互換性があります。
これは、パスワードベースの認証に加えて、`users.xml` 設定ファイルで設定できます。
なお、SQL-drivenなアクセス制御では、まだサポートされていません。

TOTPを使用して認証するには、ユーザーはプライマリパスワードに加え、TOTPアプリで生成されたワンタイムパスワードを `--one-time-password` コマンドラインオプションで指定するか、`+` 文字でメインパスワードに連結して指定する必要があります。
たとえば、プライマリパスワードが `some_password`、生成されたTOTPコードが `345123` の場合、ClickHouse への接続時に `--password some_password+345123` または `--password some_password --one-time-password 345123` を指定できます。パスワードが指定されていない場合、`clickhouse-client` は対話的に入力を求めます。

ユーザーのTOTP認証を有効にするには、`users.xml` の `time_based_one_time_password` セクションを設定します。このセクションでは、シークレット、有効期間、桁数、ハッシュアルゴリズムなどのTOTP設定を定義します。

**例**

````xml
<clickhouse>
    <!-- ... -->
    <users>
        <my_user>
            <!-- Primary password-based authentication: -->
            <password>some_password</password>
            <password_sha256_hex>1464acd6765f91fccd3f5bf4f14ebb7ca69f53af91b0a5790c2bba9d8819417b</password_sha256_hex>
            <!-- ... or any other supported authentication method ... -->

            <!-- TOTP authentication configuration -->
            <time_based_one_time_password>
                <secret>JBSWY3DPEHPK3PXP</secret>      <!-- Base32-encoded TOTP secret -->
                <period>30</period>                    <!-- Optional: OTP validity period in seconds -->
                <digits>6</digits>                     <!-- Optional: Number of digits in the OTP -->
                <algorithm>SHA1</algorithm>            <!-- Optional: Hash algorithm: SHA1, SHA256, SHA512 -->
            </time_based_one_time_password>
        </my_user>
    </users>
</clickhouse>

Parameters:

- secret - (Required) The base32-encoded secret key used to generate TOTP codes.
- period - Optional. Sets the validity period of each OTP in seconds. Must be a positive number not exceeding 120. Default is 30.
- digits - Optional. Specifies the number of digits in each OTP. Must be between 4 and 10. Default is 6.
- algorithm - Optional. Defines the hash algorithm for generating OTPs. Supported values are SHA1, SHA256, and SHA512. Default is SHA1.

Generating a TOTP Secret

To generate a TOTP-compatible secret for use with ClickHouse, run the following command in the terminal:

```bash
$ base32 -w32 < /dev/urandom | head -1
````

このコマンドは、`users.xml` の secret フィールドに追加できる、Base32 エンコード済みのシークレットを生成します。

特定のユーザーで TOTP を有効にするには、既存のパスワード用フィールド (`password` や `password_sha256_hex` など) に、`time_based_one_time_password` セクションを追加します。

TOTP シークレットの QR コードを生成するには、[qrencode](https://linux.die.net/man/1/qrencode) ツールを使用できます。

```bash
$ qrencode -t ansiutf8 'otpauth://totp/ClickHouse?issuer=ClickHouse&secret=JBSWY3DPEHPK3PXP'
```

ユーザーにTOTPを設定すると、前述のとおり、認証プロセスの一部としてワンタイムパスワードを使用できます。

### username/ssh-key

この設定では、SSH鍵による認証を利用できます。

次のような (`ssh-keygen` で生成した) SSH鍵があるとします

```text
ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIDNf0r6vRl24Ix3tv2IgPmNPO2ATa2krvt80DdcTatLj john@example.com
```

`ssh_key` 要素には次の内容が想定されます

```xml
<ssh_key>
     <type>ssh-ed25519</type>
     <base64_key>AAAAC3NzaC1lZDI1NTE5AAAAIDNf0r6vRl24Ix3tv2IgPmNPO2ATa2krvt80DdcTatLj</base64_key>
 </ssh_key>
```

他のサポート対象アルゴリズムを使用する場合は、`ssh-ed25519` を `ssh-rsa` または `ecdsa-sha2-nistp256` に置き換えてください。

### 複数の認証方式

1 人のユーザーに対して、`<auth_methods>` 要素を使って複数の認証方式を設定できます。これにより、ユーザーは列挙された方式のいずれか 1 つで認証できます。たとえば、ユーザーはパスワードと LDAP 認証情報の両方を持つことができ、そのどちらでログインしても成功します。

`<auth_methods>` の各子要素は、1 つの認証タイプだけを含む任意の名前のラッパー要素です。ラッパー名 (例: `<method1>`、`<primary>`、`<a1>`) は重要ではなく、実際に使用されるのは内部の認証要素だけです。

**例: 複数のパスワード**

```xml
<users>
    <my_user>
        <auth_methods>
            <primary>
                <password>password_one</password>
            </primary>
            <secondary>
                <password_sha256_hex>65e84be33532fb784c48129675f9eff3a682b27168c0ea744b2cf58ee02337c5</password_sha256_hex>
            </secondary>
        </auth_methods>
    </my_user>
</users>
```

**例: 複数の認証方式**

```xml
<users>
    <my_user>
        <auth_methods>
            <a1>
                <password>plaintext_pass</password>
            </a1>
            <a2>
                <password_sha256_hex>e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855</password_sha256_hex>
            </a2>
            <a3>
                <ldap>
                    <server>my_ldap_server</server>
                </ldap>
            </a3>
        </auth_methods>
    </my_user>
</users>
```

`<auth_methods>` 内では、次の認証タイプがサポートされています。

* **`password`** — 平文パスワード
* **`password_sha256_hex`** — SHA256 パスワードハッシュ
* **`password_scram_sha256_hex`** — SCRAM-SHA-256 パスワードハッシュ
* **`password_double_sha1_hex`** — ダブル SHA1 パスワードハッシュ
* **`ldap`** — LDAP サーバー認証
* **`kerberos`** — Kerberos 認証
* **`ssl_certificates`** — SSL 証明書認証
* **`ssh_keys`** — SSH 鍵認証
* **`http_authentication`** — HTTP 認証

**ルールと制限事項:**

* `<auth_methods>` は、ユーザーレベルで指定する認証方式と一緒に使用することは**できません**。どちらか一方のみを使用し、両方を併用しないでください。
* `<auth_methods>` には、少なくとも 1 つの認証方式を含める必要があります。
* `<auth_methods>` 内の各ラッパー要素には、認証タイプを 1 つだけ含める必要があります (例外は `<ssh_keys>` で、後方互換性のため複数含めることができます) 。
* TOTP (`<time_based_one_time_password>`) はユーザーレベル (`<auth_methods>` の外側) で指定し、リスト内のすべてのパスワードベースの方式に適用されます。TOTP を有効にする場合は、少なくとも 1 つのパスワードベースの方式が必要です。

**例: TOTP を使用した `auth_methods`**

```xml
<users>
    <my_user>
        <auth_methods>
            <a1>
                <password>my_password</password>
            </a1>
            <a2>
                <ldap>
                    <server>ldap_server_1</server>
                </ldap>
            </a2>
        </auth_methods>
        <time_based_one_time_password>
            <secret>JBSWY3DPEHPK3PXP</secret>
        </time_based_one_time_password>
    </my_user>
</users>
```

この例では、TOTP 検証はパスワードベースの方式 (`<password>`) に適用され、LDAP 方式はそれとは独立して外部サーバーに対する認証を行います。

### access_management

この設定では、ユーザーに対して SQL による [Access Control and Account Management](/ja/operations/access-rights#access-control-usage) を使用するかどうかを有効または無効にします。

設定可能な値:

* 0 — 無効。
* 1 — 有効。

デフォルト値: 0。

### grants

この設定では、指定したユーザーに任意の権限を付与できます。
リスト内の各要素は、権限の付与先を指定しない `GRANT` クエリである必要があります。

例:

```xml
<user1>
    <grants>
        <query>GRANT SHOW ON *.*</query>
        <query>GRANT CREATE ON *.* WITH GRANT OPTION</query>
        <query>GRANT SELECT ON system.*</query>
    </grants>
</user1>
```

この設定は、
`dictionaries`、`access_management`、`named_collection_control`、`show_named_collections_secrets`
および `allow_databases` の各設定と同時に指定することはできません。

### user_name/networks

ユーザーが ClickHouse サーバーに接続できる接続元ネットワークの一覧です。

リストの各要素には、次のいずれかの形式を指定できます。

* `<ip>` — IP アドレスまたはネットワークマスク。

  例: `213.180.204.3`, `10.0.0.1/8`, `10.0.0.1/255.255.255.0`, `2a02:6b8::3`, `2a02:6b8::3/64`, `2a02:6b8::3/ffff:ffff:ffff:ffff::`.

* `<host>` — ホスト名。

  例: `example01.host.ru`.

  アクセスを確認する際は DNS クエリが実行され、返されたすべての IP アドレスが接続元アドレスと比較されます。

* `<host_regexp>` — ホスト名に対する正規表現。

  例: `^example\d\d-\d\d-\d\.host\.ru$`

  アクセスを確認する際は、まず接続元アドレスに対して [DNS PTR query](https://en.wikipedia.org/wiki/Reverse_DNS_lookup) が実行され、その後、指定した正規表現が適用されます。次に、PTR クエリの結果に対してさらに DNS クエリが実行され、返されたすべてのアドレスが接続元アドレスと比較されます。正規表現は $ で終わるようにすることを強く推奨します。

DNS リクエストの結果はすべて、サーバーが再起動するまでキャッシュされます。

**例**

任意のネットワークからユーザーがアクセスできるようにするには、次を指定します。

```xml
<ip>::/0</ip>
```

:::note
ファイアウォールが適切に設定されているか、サーバーがインターネットに直接接続されていない場合を除き、あらゆるネットワークからアクセスできるようにするのは安全ではありません。
:::

localhost からのみアクセスできるようにするには、次を指定します:

```xml
<ip>::1</ip>
<ip>127.0.0.1</ip>
```

### user_name/profile

ユーザーに設定プロファイルを割り当てることができます。設定プロファイルは、`users.xml` ファイル内の別セクションで設定します。詳しくは、[Settings のプロファイル](../../operations/settings/settings-profiles.md) を参照してください。

### user_name/quota

クォータを使用すると、一定期間におけるリソース使用量を追跡または制限できます。クォータは、`users.xml`設定ファイルの`quotas`
セクションで設定します。

ユーザーにクォータセットを割り当てることができます。クォータの設定の詳細については、[Quotas](/ja/operations/quotas)を参照してください。

### user_name/databases

このセクションでは、現在のユーザーが実行する `SELECT` クエリに対して ClickHouse が返す行を制限することで、基本的な行レベルセキュリティを実装できます。

**例**

次の設定では、ユーザー `user1` が `SELECT` クエリの結果として、`id` フィールドの値が 1000 である `table1` の行だけを参照できるようにします。

```xml
<user1>
    <databases>
        <database_name>
            <table1>
                <filter>id = 1000</filter>
            </table1>
        </database_name>
    </databases>
</user1>
```

`filter` には、[UInt8](../../sql-reference/data-types/int-uint.md) 型の値を返す任意の式を指定できます。通常は、比較や論理演算子を含みます。`database_name.table1` では、`filter` の結果が 0 になる行はこのユーザーには返されません。このフィルタリングは `PREWHERE` 操作と互換性がなく、`WHERE→PREWHERE` 最適化も無効にします。

## ロール

あらかじめ定義された任意のロールは、`user.xml` 設定ファイルの `roles` セクションで作成できます。

`roles` セクションの構成:

```xml
<roles>
    <test_role>
        <grants>
            <query>GRANT SHOW ON *.*</query>
            <query>REVOKE SHOW ON system.*</query>
            <query>GRANT CREATE ON *.* WITH GRANT OPTION</query>
        </grants>
    </test_role>
</roles>
```

これらのロールは、`users` セクションでユーザーに付与することもできます。

```xml
<users>
    <user_name>
        ...
        <grants>
            <query>GRANT test_role</query>
        </grants>
    </user_name>
<users>
```