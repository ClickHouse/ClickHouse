# 访问权限 {#access-rights}

用户和访问权限在用户配置中设置。 这通常是 `users.xml`.

用户被记录在 `users` 科。 这里是一个片段 `users.xml` 文件:

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
            <host_regexp> Regular expression for host names. For example, ^example\d\d-\d\d-\d\.yandex\.ru$
                To check it, a DNS PTR request is made for the client's address and a regular expression is applied to the result.
                Then another DNS query is made for the result of the PTR query, and all received address are compared to the client address.
                We strongly recommend that the regex ends with \.yandex\.ru$.

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
    </web>
```

您可以看到两个用户的声明: `default`和`web`. 我们添加了 `web` 用户分开。

该 `default` 在用户名未通过的情况下选择用户。 该 `default` 如果服务器或群集的配置没有指定分布式查询处理，则user也用于分布式查询处理 `user` 和 `password` （见上的部分 [分布](../engines/table-engines/special/distributed.md) 发动机）。

The user that is used for exchanging information between servers combined in a cluster must not have substantial restrictions or quotas – otherwise, distributed queries will fail.

密码以明文（不推荐）或SHA-256形式指定。 哈希没有腌制。 在这方面，您不应将这些密码视为提供了针对潜在恶意攻击的安全性。 相反，他们是必要的保护员工。

指定允许访问的网络列表。 在此示例中，将从单独的文件加载两个用户的网络列表 (`/etc/metrika.xml`）包含 `networks` 替代。 这里是它的一个片段:

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

您可以直接在以下内容中定义此网络列表 `users.xml`，或在文件中 `users.d` directory (for more information, see the section «[配置文件](configuration-files.md#configuration_files)»).

该配置包括解释如何从任何地方打开访问的注释。

对于在生产中使用，仅指定 `ip` 元素（IP地址及其掩码），因为使用 `host` 和 `hoost_regexp` 可能会导致额外的延迟。

Next the user settings profile is specified (see the section «[设置配置文件](settings/settings-profiles.md)»). You can specify the default profile, `default'`. 配置文件可以有任何名称。 您可以为不同的用户指定相同的配置文件。 您可以在设置配置文件中编写的最重要的事情是 `readonly=1`，这确保只读访问。
Then specify the quota to be used (see the section «[配额](quotas.md#quotas)»). You can specify the default quota: `default`. It is set in the config by default to only count resource usage, without restricting it. The quota can have any name. You can specify the same quota for different users – in this case, resource usage is calculated for each user individually.

在可选 `<allow_databases>` 您还可以指定用户可以访问的数据库列表。 默认情况下，所有数据库都可供用户使用。 您可以指定 `default` 数据库。 在这种情况下，默认情况下，用户将接收对数据库的访问权限。

访问 `system` 始终允许数据库（因为此数据库用于处理查询）。

用户可以通过以下方式获取其中所有数据库和表的列表 `SHOW` 查询或系统表，即使不允许访问单个数据库。

数据库访问是不相关的 [只读](settings/permissions-for-queries.md#settings_readonly) 设置。 您不能授予对一个数据库的完全访问权限，并 `readonly` 进入另一个。

[原始文章](https://clickhouse.tech/docs/en/operations/access_rights/) <!--hide-->
