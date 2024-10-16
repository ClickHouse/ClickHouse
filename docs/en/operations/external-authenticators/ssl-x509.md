---
slug: /en/operations/external-authenticators/ssl-x509
title: "SSL X.509 certificate authentication"
---
import SelfManaged from '@site/docs/en/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

[SSL 'strict' option](../server-configuration-parameters/settings.md#openssl) enables mandatory certificate validation for the incoming connections. In this case, only connections with trusted certificates can be established. Connections with untrusted certificates will be rejected. Thus, certificate validation allows to uniquely authenticate an incoming connection. `Common Name` or `subjectAltName extension` field of the certificate is used to identify the connected user. `subjectAltName extension` supports the usage of one wildcard '*' in the server configuration. This allows to associate multiple certificates with the same user. Additionally, reissuing and revoking of the certificates does not affect the ClickHouse configuration.

To enable SSL certificate authentication, a list of `Common Name`'s or `Subject Alt Name`'s for each ClickHouse user must be specified in the settings file `users.xml `:

**Example**
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

For the SSL [`chain of trust`](https://en.wikipedia.org/wiki/Chain_of_trust) to work correctly, it is also important to make sure that the [`caConfig`](../server-configuration-parameters/settings.md#openssl) parameter is configured properly.
