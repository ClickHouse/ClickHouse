---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 48
toc_title: "\u30A2\u30AF\u30BB\u30B9\u6A29"
---

# アクセス権 {#access-rights}

ユーザーとアクセス権は、ユーザー設定で設定されます。 これは通常 `users.xml`.

ユーザーは `users` セクション。 ここでの断片です `users.xml` ファイル:

``` xml
<!-- Users and ACL. -->
<users>
    <!-- If the user name is not specified, the 'default' user is used. -->
    <default>
        <!-- Password could be specified in plaintext or in SHA256 (in hex format).

             If you want to specify password in plaintext (not recommended), place it in 'password' element.
             Example: <password>qwerty</password>.
             Password could be empty.

             If you want to specify SHA256, place it in 'password_sha256_hex' element.
             Example: <password_sha256_hex>65e84be33532fb784c48129675f9eff3a682b27168c0ea744b2cf58ee02337c5</password_sha256_hex>

             How to generate decent password:
             Execute: PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha256sum | tr -d '-'
             In first line will be password and in second - corresponding SHA256.
        -->
        <password></password>

        <!-- A list of networks that access is allowed from.
            Each list item has one of the following forms:
            <ip> The IP address or subnet mask. For example: 198.51.100.0/24 or 2001:DB8::/32.
            <host> Host name. For example: example01. A DNS query is made for verification, and all addresses obtained are compared with the address of the customer.
            <host_regexp> Regular expression for host names. For example, ^example\d\d-\d\d-\d\.host\.ru$
                To check it, a DNS PTR request is made for the client's address and a regular expression is applied to the result.
                Then another DNS query is made for the result of the PTR query, and all received address are compared to the client address.
                We strongly recommend that the regex ends with \.host\.ru$.

            If you are installing ClickHouse yourself, specify here:
                <networks>
                        <ip>::/0</ip>
                </networks>
        -->
        <networks incl="networks" />

        <!-- Settings profile for the user. -->
        <profile>default</profile>

        <!-- Quota for the user. -->
        <quota>default</quota>
    </default>

    <!-- For requests from the Yandex.Metrica user interface via the API for data on specific counters. -->
    <web>
        <password></password>
        <networks incl="networks" />
        <profile>web</profile>
        <quota>default</quota>
        <allow_databases>
           <database>test</database>
        </allow_databases>
        <allow_dictionaries>
           <dictionary>test</dictionary>
        </allow_dictionaries>
    </web>
</users>
```

する宣言からユーザー: `default`と`web`. 私達は加えました `web` ユーザー別途。

その `default` ユーザは、ユーザ名が渡されない場合に選択されます。 その `default` userは、サーバーまたはクラスターの構成が指定されていない場合は、分散クエリ処理にも使用されます。 `user` と `password` (上のセクションを参照 [分散](../engines/table-engines/special/distributed.md) エンジン）。

The user that is used for exchanging information between servers combined in a cluster must not have substantial restrictions or quotas – otherwise, distributed queries will fail.

パスワードは、クリアテキスト(非推奨)またはsha-256で指定します。 ハッシュは塩漬けじゃない この点に関し、すべきではないと考えるこれらのパスワードとして提供すことに対する潜在的悪意のある攻撃であった。 むしろ、従業員からの保護のために必要です。

アクセスを許可するネットワークのリストが指定されています。 この例では、両方のユーザーのネットワークの一覧が別のファイルから読み込まれます (`/etc/metrika.xml`)を含む `networks` 置換。 ここにそれの断片があります:

``` xml
<yandex>
    ...
    <networks>
        <ip>::/64</ip>
        <ip>203.0.113.0/24</ip>
        <ip>2001:DB8::/32</ip>
        ...
    </networks>
</yandex>
```

このネットワークのリストを直接 `users.xml` またはファイル内の `users.d` ディレクトリ(詳細については、セクションを参照 “[設定ファイル](configuration-files.md#configuration_files)”).

コンフィグを含むコメントする方法を説明するオープンアクセスいたしました。

生産の使用のために、指定して下さいただ `ip` 要素（IPアドレスとそのマスク）、 `host` と `hoost_regexp` が原因別の待ち時間をゼロにすることに

次のユーザー設定プロファイルが指定の項を参照 “[設定プロファイル](settings/settings-profiles.md)”. 既定のプロファイルを指定できます, `default'`. プロファイルの名前は任意です。 異なるユーザーに同じプロファイルを指定できます。 最も重要なことが書ける設定プロフィール `readonly=1` 読み取り専用アクセスを保証します。 次に、使用するクォータを指定します(セクションを参照 “[クォータ](quotas.md#quotas)”). 既定のクォータを指定できます: `default`. It is set in the config by default to only count resource usage, without restricting it. The quota can have any name. You can specify the same quota for different users – in this case, resource usage is calculated for each user individually.

オプションで `<allow_databases>` 部を指定することもできますリストのデータベースのユーザーがアクセスできる デフォルトでは、すべてのデータベースのユーザーです。 を指定することができ `default` データベース この場合、ユーザーはデフォルトでデータベースにアクセスできます。

オプションで `<allow_dictionaries>` また、ユーザーがアクセスできる辞書のリストを指定することもできます。 デフォルトでは、すべての辞書はユーザーが使用できます。

へのアクセス `system` データベースは常に可（このデータベースを使用して処理クエリ).

ユーザーの一覧を取得してデータベースやテーブルを用いてこれら `SHOW` クエリやシステムテーブルの場合でも、アクセス、個人データベースなのです。

データベースアクセスは、 [読み取り専用](settings/permissions-for-queries.md#settings_readonly) 設定。 できな助成金の全アクセスをデータベース `readonly` 別のものへのアクセス。

[元の記事](https://clickhouse.tech/docs/en/operations/access_rights/) <!--hide-->
