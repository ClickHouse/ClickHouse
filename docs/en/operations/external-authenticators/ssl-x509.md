# SSL X.509 certificate authentication {#ssl-external-authentication}

[SSL 'strict' option](../server-configuration-parameters/settings.md#server_configuration_parameters-openssl) enables mandatory certificate validation for the incoming connections. In this case, only connections with trusted certificates can be established. Connections with untrusted certificates will be rejected. Thus, certificate validation allows to uniquely authenticate an incoming connection. `Common Name` field of the certificate is used to identify connected user. This allows to associate multiple certificates with the same user. Additionally, reissuing and revoking of the certificates does not affect the ClickHouse configuration.

To enable SSL certificate authentication, a list of `Common Name`'s for each ClickHouse user must be sspecified in the settings file `config.xml `:

**Example**
```xml
<clickhouse>
    <!- ... -->
    <users>
        <user_name>
            <certificates>
                <common_name>host.domain.com:example_user</common_name>
                <common_name>host.domain.com:example_user_dev</common_name>
                <!-- More names -->
            </certificates>
            <!-- Other settings -->
        </user_name>
    </users>
</clickhouse>
```

For the SSL [`chain of trust`](https://en.wikipedia.org/wiki/Chain_of_trust) to work correctly, it is also important to make sure that the [`caConfig`](../server-configuration-parameters/settings.md#server_configuration_parameters-openssl) parameter is configured properly.