# User settings

The `users` section of the `user.xml` configuration file aggregates settings for users.

Structure of the `users` section:

```
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

- To specify password in plaintext (not recommended), place it in a `password` element.

    For example, `<password>qwerty</password>`. Password can be empty.

- To specify SHA256 hash of a password, place it in a `password_sha256_hex` element.

    For example, `<password_sha256_hex>65e84be33532fb784c48129675f9eff3a682b27168c0ea744b2cf58ee02337c5</password_sha256_hex>`.

    To generate password, in the command prompt perform the command:

    ```
    PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha256sum | tr -d '-'
    ```

    The first line of the result is the password. The second line is the corresponding SHA256 hash.


### user_name/networks

List of networks which the user can connect to ClickHouse server from.

Each element of list has one of the following forms:

- `<ip>` — IP-address or a network mask.

    Examples: `213.180.204.3`, `10.0.0.1/8`, `10.0.0.1/255.255.255.0`, `2a02:6b8::3`, `2a02:6b8::3/64`, `2a02:6b8::3/ffff:ffff:ffff:ffff::`.

- `<host>` — Hostname.

    Example: `server01.yandex.ru`.

    To check access, DNS query is performed, and all received addresses compared to peer address.

- `<host_regexp>` — Regular expression for host names.

    Example, `^server\d\d-\d\d-\d\.yandex\.ru$`

    To check access, DNS PTR query is performed for peer address and then regexp is applied. Then, for result of PTR query, another DNS query is performed and all received addresses compared to peer address. Strongly recommended that regexp is ends with $

All results of DNS requests are cached till server restart.

**Examples**

To open access for user from any network, specify:

```
<ip>::/0</ip>
```

To open access only from localhost, specify:

```
<ip>::1</ip>
<ip>127.0.0.1</ip>
```

### user_name/profile

You can assign a settings profile for the user. Settings profiles are configured in a separate section of the `users.xml` file. For more information see the [Settings profile](settings_profiles.md).

### user_name/quota

Quotas allow you to limit resource usage over a period of time, or track the use of resources. Quotas are configured in the `quotas`
section of the `users.xml` configuration file.

You can assign a quotas set for the user. For the detailed description of quotas configuration see the [Quotas](../quotas.md) topic in the document.

### user_name/databases

In this section you can choose which rows ClickHouse returns by `SELECT` query. This feature implements a kind of Row-Level Security in ClickHouse.

**Example**

The following configuration sets that the user `user1` can see only the rows of `table1` as a result of `SELECT` query where the value of field `id` equals to 1000.

```
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

The `filter` can be any expression resulting with the [UInt8](../../data_types/int_uint.md)-typed value. It usually contains comparisons and logical operators. The filtering is incompatible with `PREWHERE` operations and disables `WHERE→PREWHERE` optimization.
