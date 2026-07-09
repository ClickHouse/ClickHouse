---
description: 'SSL X.509 証明書認証のドキュメント'
slug: /operations/external-authenticators/ssl-x509
title: 'SSL X.509 証明書認証'
doc_type: 'reference'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

[SSL &#39;strict&#39; オプション](../server-configuration-parameters/settings.md#openssl)を有効にすると、受信接続に対して証明書の検証が必須になります。この場合、信頼された証明書を使用する接続のみ確立できます。信頼されていない証明書を使用する接続は拒否されます。したがって、証明書の検証により、受信接続を一意に認証できます。接続しているユーザーの識別には、証明書の `Common Name` または `subjectAltName extension` フィールドが使用されます。`subjectAltName extension` では、server configuration で 1 つのワイルドカード &#39;*&#39; を使用できます。これにより、複数の証明書を同じユーザーに関連付けることができます。さらに、証明書の再発行や失効は ClickHouse の設定に影響しません。

SSL 証明書認証を有効にするには、各 ClickHouse ユーザーごとに `Common Name` または `Subject Alt Name` の一覧を設定ファイル `users.xml ` に指定する必要があります。

**例**

```xml
<clickhouse>
    <!- ... -->
    <users>
        <user_name_1>
            <ssl_certificates>
                <common_name>host.domain.com:example_user</common_name>
                <common_name>host.domain.com:example_user_dev</common_name>
                <!-- More names -->
            </ssl_certificates>
            <!-- Other settings -->
        </user_name_1>
        <user_name_2>
            <ssl_certificates>
                <subject_alt_name>DNS:host.domain.com</subject_alt_name>
                <!-- More names -->
            </ssl_certificates>
            <!-- Other settings -->
        </user_name_2>
        <user_name_3>
            <ssl_certificates>
                <!-- Wildcard support -->
                <subject_alt_name>URI:spiffe://foo.com/*/bar</subject_alt_name>
            </ssl_certificates>
        </user_name_3>
    </users>
</clickhouse>
```

SSL の [`信頼の連鎖`](https://en.wikipedia.org/wiki/Chain_of_trust) が正しく機能するためには、[`caConfig`](../server-configuration-parameters/settings.md#openssl) パラメーターが適切に設定されていることを確認することも重要です。