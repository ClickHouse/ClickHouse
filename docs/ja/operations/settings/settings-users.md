---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 63
toc_title: "\u30E6\u30FC\u30B6\u30FC\u8A2D\u5B9A"
---

# ユーザー設定 {#user-settings}

その `users` のセクション `user.xml` 設定ファイルにユーザを設定します。

の構造 `users` セクション:

``` xml
<users>
    <!-- If user name was not specified, 'default' user is used. -->
    <user_name>
        <password></password>
        <!-- Or -->
        <password_sha256_hex></password_sha256_hex>

        <networks incl="networks" replace="replace">
        </networks>

        <profile>profile_name</profile>

        <quota>default</quota>

        <databases>
            <database_name>
                <table_name>
                    <filter>expression</filter>
                <table_name>
            </database_name>
        </databases>
    </user_name>
    <!-- Other users settings -->
</users>
```

### user\_name/パスワード {#user-namepassword}

パスワードは、平文またはsha256（hex形式）で指定できます。

-   平文でパスワードを割り当てるには (**推奨しない**）、それを置く `password` 要素。

    例えば, `<password>qwerty</password>`. パスワードは空白のままにできます。

<a id="password_sha256_hex"></a>

-   SHA256ハッシュを使用してパスワードを割り当てるには、 `password_sha256_hex` 要素。

    例えば, `<password_sha256_hex>65e84be33532fb784c48129675f9eff3a682b27168c0ea744b2cf58ee02337c5</password_sha256_hex>`.

    シェルからパスワードを生成する方法の例:

          PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha256sum | tr -d '-'

    結果の最初の行はパスワードです。 第二の行は、対応するsha256ハッシュです。

<a id="password_double_sha1_hex"></a>

-   MySQLクライアントとの互換性のために、passwordはダブルSHA1ハッシュで指定できます。 それを置く `password_double_sha1_hex` 要素。

    例えば, `<password_double_sha1_hex>08b4a0f1de6ad37da17359e592c8d74788a83eb0</password_double_sha1_hex>`.

    シェルからパスワードを生成する方法の例:

          PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha1sum | tr -d '-' | xxd -r -p | sha1sum | tr -d '-'

    結果の最初の行はパスワードです。 第二の行は、対応するダブルsha1ハッシュです。

### user\_name/networks {#user-namenetworks}

ユーザーがclickhouseサーバーに接続できるネットワークのリスト。

リストの各要素には、次のいずれかの形式があります:

-   `<ip>` — IP address or network mask.

    例: `213.180.204.3`, `10.0.0.1/8`, `10.0.0.1/255.255.255.0`, `2a02:6b8::3`, `2a02:6b8::3/64`, `2a02:6b8::3/ffff:ffff:ffff:ffff::`.

-   `<host>` — Hostname.

    例えば: `example01.host.ru`.

    アクセスを確認するには、dnsクエリが実行され、返されたすべてのipアドレスがピアアドレスと比較されます。

-   `<host_regexp>` — Regular expression for hostnames.

    例えば, `^example\d\d-\d\d-\d\.host\.ru$`

    アクセスを確認するには、 [DNS PTRクエリ](https://en.wikipedia.org/wiki/Reverse_DNS_lookup) ピアアドレスに対して実行され、指定された正規表現が適用されます。 次に、PTRクエリの結果に対して別のDNSクエリが実行され、すべての受信アドレスがピアアドレスと比較されます。 Regexpは$で終わることを強くお勧めします。

すべての結果のdnsの要求をキャッシュまでのサーバが再起動してしまいます。

**例**

オープンアクセスのためのユーザーからネットワークのいずれかを指定し:

``` xml
<ip>::/0</ip>
```

!!! warning "警告"
    この不安にオープンアクセスからネットワークを持っていない場合、ファイアウォールを適切に設定されたサーバーに直接接続されます。

オープンアクセスのみからlocalhostを指定し:

``` xml
<ip>::1</ip>
<ip>127.0.0.1</ip>
```

### user\_name/プロファイル {#user-nameprofile}

を割り当てることができる設定プロファイルをユーザーです。 設定プロファイルはの別のセクションで設定されます `users.xml` ファイル。 詳細については、 [設定のプロファイル](settings-profiles.md).

### ユーザー名/クォータ {#user-namequota}

クォータを使用すると、一定期間にわたってリソース使用量を追跡または制限できます。 クォータは、 `quotas`
のセクション `users.xml` 構成ファイル。

ユーザにクォータセットを割り当てることができます。 クォータ設定の詳細については、以下を参照してください [クォータ](../quotas.md#quotas).

### user\_name/データベース {#user-namedatabases}

このセクションでは、clickhouseによって返される行を制限することができます `SELECT` 現在のユーザーが行うクエリは、基本的な行レベルのセキュリティを実装します。

**例えば**

以下の構成力がユーザー `user1` の行だけを見ることができます `table1` の結果として `SELECT` クエリ、ここでの値 `id` フィールドは1000です。

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

その `filter` 結果として得られる任意の式を指定できます [UInt8](../../sql-reference/data-types/int-uint.md)-タイプ値。 通常、比較演算子と論理演算子が含まれます。 からの行 `database_name.table1` このユーザーに対して0のフィルター結果は返されません。 フィルタリングは `PREWHERE` 操作および無効化 `WHERE→PREWHERE` 最適化。

[元の記事](https://clickhouse.tech/docs/en/operations/settings/settings_users/) <!--hide-->
