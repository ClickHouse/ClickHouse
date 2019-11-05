# User settings

The `users` section of the `user.xml` configuration file contains user settings.

Structure of the `users` section:

```xml
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

### user_name/password

Password could be specified in plaintext or in SHA256 (hex format).

- To assign a password in plaintext (**not recommended**), place it in a `password` element.

    For example, `<password>qwerty</password>`. The password can be left blank.

- To assign a password using its SHA256 hash, place it in a `password_sha256_hex` element.

    For example, `<password_sha256_hex>65e84be33532fb784c48129675f9eff3a682b27168c0ea744b2cf58ee02337c5</password_sha256_hex>`.

    Example of how to generate a password from shell:

    ```
    PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha256sum | tr -d '-'
    ```

    The first line of the result is the password. The second line is the corresponding SHA256 hash.


### user_name/networks

List of networks from which the user can connect to the ClickHouse server.

Each element of the list can have one of the following forms:

- `<ip>` — IP address or network mask.

    Examples: `213.180.204.3`, `10.0.0.1/8`, `10.0.0.1/255.255.255.0`, `2a02:6b8::3`, `2a02:6b8::3/64`, `2a02:6b8::3/ffff:ffff:ffff:ffff::`.

- `<host>` — Hostname.

    Example: `server01.yandex.ru`.

    To check access, a DNS query is performed, and all returned IP addresses are compared to the peer address.

- `<host_regexp>` — Regular expression for hostnames.

    Example, `^server\d\d-\d\d-\d\.yandex\.ru$`

    To check access, a [DNS PTR query](https://en.wikipedia.org/wiki/Reverse_DNS_lookup) is performed for the peer address and then the specified regexp is applied. Then, another DNS query is performed for the results of the PTR query and all the received addresses are compared to the peer address. We strongly recommend that regexp ends with $.

All results of DNS requests are cached until the server restarts.

**Examples**

To open access for user from any network, specify:

```xml
<ip>::/0</ip>
```

!!! warning "Warning"
    It's insecure to open access from any network unless you have a firewall properly configured or the server is not directly connected to Internet.


To open access only from localhost, specify:

```xml
<ip>::1</ip>
<ip>127.0.0.1</ip>
```

### user_name/profile

You can assign a settings profile for the user. Settings profiles are configured in a separate section of the `users.xml` file. For more information, see [Profiles of Settings](settings_profiles.md).

### user_name/quota

Quotas allow you to track or limit resource usage over a period of time. Quotas are configured in the `quotas`
section of the `users.xml` configuration file.

You can assign a quotas set for the user. For a detailed description of quotas configuration, see [Quotas](../quotas.md#quotas).

### user_name/databases

In this section, you can you can limit rows that are returned by ClickHouse for `SELECT` queries made by the current user, thus implementing basic row-level security.

**Example**

The following configuration forces that user `user1` can only see the rows of `table1` as the result of `SELECT` queries, where the value of the `id` field is 1000.

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

The `filter` can be any expression resulting in a [UInt8](../../data_types/int_uint.md)-type value. It usually contains comparisons and logical operators. Rows from `database_name.table1` where filter results to 0 are not returned for this user. The filtering is incompatible with `PREWHERE` operations and disables `WHERE→PREWHERE` optimization.

[Original article](https://clickhouse.yandex/docs/en/operations/settings/settings_users/) <!--hide-->
