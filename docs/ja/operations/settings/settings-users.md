---
slug: /ja/operations/settings/settings-users
sidebar_position: 63
sidebar_label: ユーザー設定
---

# ユーザーとロールの設定

`users.xml` の `users` セクションにはユーザー設定が含まれています。

:::note
ClickHouseは、ユーザー管理のための[SQL駆動のワークフロー](../../guides/sre/user-management/index.md#access-control)もサポートしています。利用をお勧めします。
:::

`users` セクションの構造:

``` xml
<users>
    <!-- ユーザー名が指定されていない場合、'default' ユーザーが使用されます。 -->
    <user_name>
        <password></password>
        <!-- または -->
        <password_sha256_hex></password_sha256_hex>

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
    <!-- 他のユーザー設定 -->
</users>
```

### user_name/password {#user-namepassword}

パスワードはプレーンテキストまたはSHA256（16進形式）で指定できます。

- プレーンテキストでパスワードを割り当てるには（**非推奨**）、`password` 要素に置きます。

    例: `<password>qwerty</password>`。パスワードは空白にすることもできます。

<a id="password_sha256_hex"></a>

- SHA256ハッシュを使用してパスワードを割り当てるには、`password_sha256_hex` 要素に置きます。

    例: `<password_sha256_hex>65e84be33532fb784c48129675f9eff3a682b27168c0ea744b2cf58ee02337c5</password_sha256_hex>`。

    シェルからパスワードを生成する例:

          PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha256sum | tr -d '-'

    結果の最初の行がパスワードです。第2行は対応するSHA256ハッシュです。

<a id="password_double_sha1_hex"></a>

- MySQLクライアントとの互換性のために、パスワードはダブルSHA1ハッシュでも指定できます。`password_double_sha1_hex` 要素に置きます。

    例: `<password_double_sha1_hex>08b4a0f1de6ad37da17359e592c8d74788a83eb0</password_double_sha1_hex>`。

    シェルからパスワードを生成する例:

          PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha1sum | tr -d '-' | xxd -r -p | sha1sum | tr -d '-'

    結果の最初の行がパスワードです。第2行は対応するダブルSHA1ハッシュです。

### username/ssh-key {#user-sshkey}

この設定はSSHキーによる認証を許可します。

例えば、`ssh-keygen` によって生成されたSSH鍵が次のような形式の場合
```
ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIDNf0r6vRl24Ix3tv2IgPmNPO2ATa2krvt80DdcTatLj john@example.com
```
`ssh_key` 要素は次のように指定します
```
<ssh_key>
     <type>ssh-ed25519</type>
     <base64_key>AAAAC3NzaC1lZDI1NTE5AAAAIDNf0r6vRl24Ix3tv2IgPmNPO2ATa2krvt80DdcTatLj</base64_key>
 </ssh_key>
```

他のサポートされているアルゴリズムに対しては、`ssh-ed25519` を `ssh-rsa` や `ecdsa-sha2-nistp256` に置き換えてください。

### access_management {#access_management-user-setting}

この設定は、SQL駆動の[アクセス制御とアカウント管理](../../guides/sre/user-management/index.md#access-control)の使用を、ユーザーに対して有効または無効にします。

可能な値:

- 0 — 無効。
- 1 — 有効。

デフォルト値: 0。

### grants {#grants-user-setting}

この設定は、選択したユーザーに任意の権限を付与することを許可します。
リストの各要素は、被授与者を指定しない `GRANT` クエリであるべきです。

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

この設定は、`dictionaries`、`access_management`、`named_collection_control`、`show_named_collections_secrets`
および `allow_databases` 設定と同時に指定することはできません。

### user_name/networks {#user-namenetworks}

ユーザーがClickHouseサーバーに接続できるネットワークのリストです。

リストの各要素は次の形式のいずれかになります:

- `<ip>` — IPアドレスまたはネットワークマスク。

    例: `213.180.204.3`, `10.0.0.1/8`, `10.0.0.1/255.255.255.0`, `2a02:6b8::3`, `2a02:6b8::3/64`, `2a02:6b8::3/ffff:ffff:ffff:ffff::`。

- `<host>` — ホスト名。

    例: `example01.host.ru`。

    アクセスを確認するために、DNSクエリが実行され、帰ってきたすべてのIPアドレスがピアアドレスと比較されます。

- `<host_regexp>` — ホスト名の正規表現。

    例、`^example\d\d-\d\d-\d\.host\.ru$`

    アクセスを確認するために、[DNS PTRクエリ](https://en.wikipedia.org/wiki/Reverse_DNS_lookup)がピアアドレスに対して行われ、指定された正規表現が適用されます。その後、PTRクエリの結果に対してもう一つのDNSクエリが行われ、得られたすべてのアドレスがピアアドレスと比較されます。正規表現が $ で終わることを強くお勧めします。

すべてのDNSリクエストの結果はサーバーが再起動されるまでキャッシュされます。

**例**

任意のネットワークからのユーザーアクセスを開放するには、次のように指定します:

``` xml
<ip>::/0</ip>
```

:::note
適切に構成されたファイアウォールを持っているか、またはサーバーがインターネットに直接接続されていない限り、任意のネットワークからのアクセスを許可するのは安全ではありません。
:::

localhostのみにアクセスを許可するには、次のように指定します:

``` xml
<ip>::1</ip>
<ip>127.0.0.1</ip>
```

### user_name/profile {#user-nameprofile}

ユーザーに設定プロファイルを割り当てることができます。設定プロファイルは `users.xml` ファイルの別のセクションで設定されます。詳しくは [設定のプロファイル](../../operations/settings/settings-profiles.md) を参照してください。

### user_name/quota {#user-namequota}

クォータにより、一定期間内に使用されるリソースの使用を追跡または制限することができます。クォータは `users.xml` の `quotas` セクションで設定されます。

ユーザーにクォータセットを割り当てることができます。クォータ構成の詳細については [クォータ](../../operations/quotas.md#quotas) を参照してください。

### user_name/databases {#user-namedatabases}

このセクションでは、現在のユーザーが `SELECT` クエリを通じてClickHouseから返される行を制限することができます。基本的な行レベルのセキュリティを実装することができます。

**例**

次の設定は、ユーザー `user1` が `table1` の `SELECT` クエリ結果として、`id` フィールドの値が1000である行のみを見ることができることを強制します。

``` xml
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

`filter` は [UInt8](../../sql-reference/data-types/int-uint.md) 型の値を持つ任意の式であることができます。通常、比較および論理演算子を含みます。`database_name.table1` の行で `filter` が0になるものは、このユーザーには返されません。フィルタリングは `PREWHERE` 操作とは互換性がなく、`WHERE→PREWHERE` 最適化を無効にします。

## ロール

`user.xml` の `roles` セクションを使用して、任意の事前定義されたロールを作成することができます。

`roles` セクションの構造:

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

これらのロールは `users` セクションからユーザーに付与することもできます:

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
