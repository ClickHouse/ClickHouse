---
description: 'SSL X.509 证书身份验证文档'
slug: /operations/external-authenticators/ssl-x509
title: 'SSL X.509 证书身份验证'
doc_type: 'reference'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

[SSL &#39;strict&#39; 选项](../server-configuration-parameters/settings.md#openssl)会对传入连接强制执行证书验证。在这种情况下，只有使用受信任证书的连接才能建立。使用不受信任证书的连接将被拒绝。因此，证书验证可用于唯一标识并验证传入连接的身份。证书中的 `Common Name` 或 `subjectAltName extension` 字段用于标识已连接的用户。`subjectAltName extension` 支持在服务器配置中使用一个通配符 &#39;*&#39;。这样可以将多个证书关联到同一用户。此外，重新签发或吊销证书也不会影响 ClickHouse 配置。

要启用 SSL 证书身份验证，必须在设置文件 `users.xml ` 中为每个 ClickHouse 用户指定 `Common Name` 或 `Subject Alt Name` 列表：

**示例**

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

为确保 SSL [`信任链`](https://en.wikipedia.org/wiki/Chain_of_trust) 能正常工作，同样也需要确认 [`caConfig`](../server-configuration-parameters/settings.md#openssl) 参数已正确配置。