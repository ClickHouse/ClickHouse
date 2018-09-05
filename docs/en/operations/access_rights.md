# Access rights

Users and access rights are set up in the user config. This is usually `users.xml`.

Users are recorded in the `users` section. Here is a fragment of the `users.xml` file:

```xml
<!-- Users and ACL. -->
<users>
    <!-- If the user name is not specified, the 'default' user is used. -->
    <default>
        <!-- Password could be specified in plaintext or in SHA256 (in hex format).

             If you want to specify the password in plain text (not recommended), place it in the 'password' element.
             Example: <password>qwerty</password>.
             Password can be empty.

             If you want to specify SHA256, place it in the 'password_sha256_hex' element.
                          Example: <password_sha256_hex>65e84be33532fb784c48129675f9eff3a682b27168c0ea744b2cf58ee02337c5</password_sha256_hex>

             How to generate decent password:
             Execute: PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha256sum | tr -d '-'
             In first line will be password and in second - corresponding SHA256.
        -->
        <password></password>
        <!-- A list of networks that access is allowed from.
            Each list item has one of the following forms:
            <ip>IP address or subnet mask. For example: 198.51.100.0/24 or 2001:DB8::/32.
            <host> Host name. For example: example01. A DNS query is made for verification, and all addresses obtained are compared with the address of the customer.
            <host_regexp> Regular expression for host names. For example: ^example\d\d-\d\d-\d\.yandex\.ru$
                For verification, a DNS PTR query is made for the customer's address and a regular expression is applied to the result.
                Then another DNS query is made for the result of the PTR query, and all received address are compared to the client address.
                We strongly recommend that the regex ends with \.yandex\.ru$.

            If you are installing ClickHouse yourself, enter:
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
    </web>
</users>
```

You can see a declaration from two users: `default` and `web`. We added the `web` user separately.

The `default` user is chosen in cases when the username is not passed. The `default` user is also used for distributed query processing, if the configuration of the server or cluster doesn't specify the `user` and `password` (see the section on the [Distributed](../operations/table_engines/distributed.md#table_engines-distributed) engine).

The user that is used for exchanging information between servers combined in a cluster must not have substantial restrictions or quotas – otherwise, distributed queries will fail.

The password is specified in cleartext (not recommended) or in SHA-256. The hash isn't salted. In this regard, you should not consider these passwords as providing security against potential malicious attacks. Rather, they are necessary for protection from employees.

A list of networks is specified that access is allowed from. In this example, the list of networks for both users is loaded from a separate file (/etc/metrika.xml) containing the 'networks' substitution. Here is a fragment of it:

```xml
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

We could have defined this list of networks directly in 'users.xml', or in a file in the 'users.d' directory (for more information, see the section "Configuration files").

The config includes comments explaining how to open access from everywhere.

For use in production, only specify IP elements (IP addresses and their masks), since using 'host' and 'hoost_regexp' might cause extra latency.

Next the user settings profile is specified (see the section "Settings profiles"). You can specify the default profile, `default`. The profile can have any name. You can specify the same profile for different users. The most important thing you can write in the settings profile is 'readonly' set to 1, which provides read-only access.

After this, the quota is defined (see the section "Quotas"). You can specify the default quota, `default`. It is set in the config by default so that it only counts resource usage, but does not restrict it. The quota can have any name. You can specify the same quota for different users – in this case, resource usage is calculated for each user individually.

In the optional `<allow_databases>` section, you can also specify a list of databases that the user can access. By default, all databases are available to the user. You can specify the `default` database. In this case, the user will receive access to the database by default.

Access to the `system` database is always allowed (since this database is used for processing queries).

The user can get a list of all databases and tables in them by using `SHOW` queries or system tables, even if access to individual databases isn't allowed.

Database access is not related to the [readonly](settings/query_complexity.md#query_complexity_readonly) setting. You can't grant full access to one database and `readonly` access to another one.
