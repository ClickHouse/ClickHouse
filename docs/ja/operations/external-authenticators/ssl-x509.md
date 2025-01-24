---
slug: /ja/operations/external-authenticators/ssl-x509
title: "SSL X.509証明書認証"
---
import SelfManaged from '@site/docs/ja/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

[SSL 'strict' オプション](../server-configuration-parameters/settings.md#openssl)は、受信接続に対する証明書の厳格な検証を有効にします。この場合、信頼された証明書を持つ接続のみが確立され、信頼されていない証明書の接続は拒否されます。したがって、証明書の検証により、受信接続を一意に認証することが可能となります。証明書の`Common Name`あるいは`subjectAltName extension`フィールドを用いて、接続されたユーザーを識別します。`subjectAltName extension`はサーバー設定でワイルドカード '*' の使用をサポートしています。これにより、複数の証明書を同じユーザーに関連付けることが可能です。さらに、証明書の再発行および失効はClickHouseの設定に影響を与えません。

SSL証明書認証を有効にするには、各ClickHouseユーザーに対応した`Common Name`または`Subject Alt Name`のリストを設定ファイル`users.xml`に指定する必要があります。

**例**
```xml
<clickhouse>
    <!- ... -->
    <users>
        <user_name_1>
            <ssl_certificates>
                <common_name>host.domain.com:example_user</common_name>
                <common_name>host.domain.com:example_user_dev</common_name>
                <!-- 他の名前 -->
            </ssl_certificates>
            <!-- その他の設定 -->
        </user_name_1>
        <user_name_2>
            <ssl_certificates>
                <subject_alt_name>DNS:host.domain.com</subject_alt_name>
                <!-- 他の名前 -->
            </ssl_certificates>
            <!-- その他の設定 -->
        </user_name_2>
        <user_name_3>
            <ssl_certificates>
                <!-- ワイルドカードサポート -->
                <subject_alt_name>URI:spiffe://foo.com/*/bar</subject_alt_name>
            </ssl_certificates>
        </user_name_3>
    </users>
</clickhouse>
```

SSLの[`信頼の連鎖`](https://en.wikipedia.org/wiki/Chain_of_trust)を正しく機能させるためには、[`caConfig`](../server-configuration-parameters/settings.md#openssl)パラメータが適切に設定されていることを確認することも重要です。
