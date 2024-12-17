---
sidebar_label: SSLユーザー証明書認証
sidebar_position: 3
slug: /ja/guides/sre/ssl-user-auth
---

# SSLユーザー証明書の認証設定
import SelfManaged from '@site/docs/ja/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

このガイドでは、SSLユーザー証明書を使用して認証を設定するためのシンプルで最小限の設定を提供します。このチュートリアルは、[SSL-TLSの設定ガイド](../configuring-ssl.md)に基づいています。

:::note
SSLユーザー認証は、`https`またはネイティブインターフェースを使用する場合のみサポートされています。現在、gRPCやPostgreSQL/MySQLエミュレーションポートでは使用されていません。

ClickHouseノードは、安全な認証のために`<verificationMode>strict</verificationMode>`を設定する必要があります（`relaxed`はテスト目的で機能します）。
:::

## 1. SSLユーザー証明書を作成

:::note
この例では、自己署名されたCAを使用した自己署名証明書を使用しています。プロダクション環境では、CSRを作成し、PKIチームまたは証明書プロバイダーに提出して適切な証明書を取得してください。
:::

1. 証明書署名要求（CSR）とキーを生成します。基本的な形式は次のとおりです：
    ```bash
    openssl req -newkey rsa:2048 -nodes -subj "/CN=<my_host>:<my_user>"  -keyout <my_cert_name>.key -out <my_cert_name>.csr
    ```
    この例では、サンプル環境で使用されるドメインとユーザーに対してこれを使用します：
    ```bash
    openssl req -newkey rsa:2048 -nodes -subj "/CN=chnode1.marsnet.local:cert_user"  -keyout chnode1_cert_user.key -out chnode1_cert_user.csr
    ```
    :::note
    CNは任意であり、証明書の識別子として任意の文字列を使用できます。次のステップでユーザーを作成する際に使用されます。
    :::

2. 認証に使用する新しいユーザー証明書を生成し、署名します。基本的な形式は次のとおりです：
    ```bash
    openssl x509 -req -in <my_cert_name>.csr -out <my_cert_name>.crt -CA <my_ca_cert>.crt -CAkey <my_ca_cert>.key -days 365
    ```
    この例では、サンプル環境で使用されるドメインとユーザーに対してこれを使用します：
    ```bash
    openssl x509 -req -in chnode1_cert_user.csr -out chnode1_cert_user.crt -CA marsnet_ca.crt -CAkey marsnet_ca.key -days 365
    ```

## 2. SQLユーザーを作成し、権限を付与

:::note
SQLユーザーを有効にしてロールを設定する方法の詳細については、[SQLユーザーとロールの定義](index.md)ユーザーガイドを参照してください。
:::

1. 証明書認証を使用するように定義されたSQLユーザーを作成します：
    ```sql
    CREATE USER cert_user IDENTIFIED WITH ssl_certificate CN 'chnode1.marsnet.local:cert_user';
    ```

2. 新しい証明書ユーザーに権限を付与します：
    ```sql
    GRANT ALL ON *.* TO cert_user WITH GRANT OPTION;
    ```
    :::note
    この演習ではデモンストレーション目的でユーザーにフル管理者権限を付与しています。権限設定についてはClickHouseの[RBACドキュメント](/docs/ja/guides/sre/user-management/index.md)を参照してください。
    :::

    :::note
    ユーザーとロールを定義するためにSQLを使用することをお勧めしますが、現在設定ファイルでユーザーとロールを定義している場合、ユーザーは次のようになります：
    ```xml
    <users>
        <cert_user>
            <ssl_certificates>
                <common_name>chnode1.marsnet.local:cert_user</common_name>
            </ssl_certificates>
            <networks>
                <ip>::/0</ip>
            </networks>
            <profile>default</profile>
            <access_management>1</access_management>
            <!-- other options -->
        </cert_user>
    </users>
    ```
    :::

## 3. テスト

1. ユーザー証明書、ユーザーキー、CA証明書をリモートノードにコピーします。

2. 証明書とパスを指定して、ClickHouseの[クライアント設定](/docs/ja/interfaces/cli.md#configuration_files)でOpenSSLを構成します。

    ```xml
    <openSSL>
        <client>
            <certificateFile>my_cert_name.crt</certificateFile>
            <privateKeyFile>my_cert_name.key</privateKeyFile>
            <caConfig>my_ca_cert.crt</caConfig>
        </client>
    </openSSL>
    ```

3. `clickhouse-client`を実行します。
    ```
    clickhouse-client --user <my_user> --query 'SHOW TABLES'
    ```
    :::note
    設定で証明書が指定されている場合、clickhouse-clientに渡されるパスワードは無視されることに注意してください。
    :::

## 4. HTTPのテスト

1. ユーザー証明書、ユーザーキー、CA証明書をリモートノードにコピーします。

2. `curl`を使用してサンプルSQLコマンドをテストします。基本的な形式は次のとおりです：
    ```bash
    echo 'SHOW TABLES' | curl 'https://<clickhouse_node>:8443' --cert <my_cert_name>.crt --key <my_cert_name>.key --cacert <my_ca_cert>.crt -H "X-ClickHouse-SSL-Certificate-Auth: on" -H "X-ClickHouse-User: <my_user>" --data-binary @-
    ```
    例えば：
    ```bash
    echo 'SHOW TABLES' | curl 'https://chnode1:8443' --cert chnode1_cert_user.crt --key chnode1_cert_user.key --cacert marsnet_ca.crt -H "X-ClickHouse-SSL-Certificate-Auth: on" -H "X-ClickHouse-User: cert_user" --data-binary @-
    ```
    出力は次のようになります：
    ```response
    INFORMATION_SCHEMA
    default
    information_schema
    system
    ```
    :::note
    パスワードが指定されていないことに注意してください。ClickHouseはパスワードの代わりに証明書を使用してユーザーを認証します。
    :::

## まとめ

この記事では、SSL証明書認証のためのユーザーを作成し設定する基本を紹介しました。この方法は、`clickhouse-client`または`https`インターフェースをサポートし、HTTPヘッダーを設定できるクライアントで使用できます。生成された証明書とキーはプライベートに保ち、限られたアクセスのみを許可してください。証明書とキーはパスワードと同様に扱ってください。
