---
slug: /zh/operations/settings/settings-users
machine_translated: false
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
sidebar_position: 63
sidebar_label: "\u7528\u6237\u8BBE\u7F6E"
---

# 用户设置 {#user-settings}

`users.xml` 中的 `users` 配置段包含了用户配置

:::note
ClickHouse还支持 [SQL驱动的工作流](/docs/en/operations/access-rights#access-control) 用于管理用户。 我们建议使用它。
:::

`users` 配置段的结构:

``` xml
<users>
    <!-- If user name was not specified, 'default' user is used. -->
    <user_name>
        <password></password>
        <!-- Or -->
        <password_sha256_hex></password_sha256_hex>

        <access_management>0|1</access_management>

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

### user_name/password {#user-namepassword}

密码可以以明文或SHA256（十六进制格式）指定。

-   以明文形式分配密码 (**不推荐**），把它放在一个 `password` 配置段中。

    例如, `<password>qwerty</password>`. 密码可以留空。

<a id="password_sha256_hex"></a>

-   要使用SHA256加密后的密码，请将其放置在 `password_sha256_hex` 配置段。

    例如, `<password_sha256_hex>65e84be33532fb784c48129675f9eff3a682b27168c0ea744b2cf58ee02337c5</password_sha256_hex>`.

    从shell生成加密密码的示例:

          PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha256sum | tr -d '-'

    结果的第一行是密码。 第二行是相应的SHA256哈希。

<a id="password_double_sha1_hex"></a>

-   为了与MySQL客户端兼容，密码可以设置为双SHA1哈希加密, 请将其放置在 `password_double_sha1_hex` 配置段。

    例如, `<password_double_sha1_hex>08b4a0f1de6ad37da17359e592c8d74788a83eb0</password_double_sha1_hex>`.

    从shell生成密码的示例:

          PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha1sum | tr -d '-' | xxd -r -p | sha1sum | tr -d '-'

    结果的第一行是密码。 第二行是相应的双SHA1哈希。

### access_management {#access_management-user-setting}

此设置可为用户启用或禁用 SQL-driven [访问控制和帐户管理](/docs/en/operations/access-rights#access-control) 。

可能的值:

-   0 — Disabled.
-   1 — Enabled.

默认值：0。

### user_name/networks {#user-namenetworks}

用户访问来源列表

列表中的每个元素都可以具有以下形式之一:

-   `<ip>` — IP地址或网络掩码

    例: `213.180.204.3`, `10.0.0.1/8`, `10.0.0.1/255.255.255.0`, `2a02:6b8::3`, `2a02:6b8::3/64`, `2a02:6b8::3/ffff:ffff:ffff:ffff::`.

-   `<host>` — 域名

    示例: `example01.host.ru`.

    为检查访问，将执行DNS查询，并将所有返回的IP地址与对端地址进行比较。

-   `<host_regexp>` — 域名的正则表达式.

    示例, `^example\d\d-\d\d-\d\.host\.ru$`

    为检查访问，[DNS PTR查询](https://en.wikipedia.org/wiki/Reverse_DNS_lookup) 对对端地址执行，然后应用指定的正则表达式。 然后，以PTR查询的结果执行另一个DNS查询，并将所有接收到的地址与对端地址进行比较. 我们强烈建议正则表达式以$结尾.

DNS请求的所有结果都将被缓存，直到服务器重新启动。

**例**

要开启任意来源网络的访问, 请指定:

``` xml
<ip>::/0</ip>
```

!!! warning "警告"
    从任何网络开放访问是不安全的，除非你有一个正确配置的防火墙, 或者服务器没有直接连接到互联网。

若要限定本机访问, 请指定:

``` xml
<ip>::1</ip>
<ip>127.0.0.1</ip>
```

### user_name/profile {#user-nameprofile}

您可以为用户分配设置配置文件。 设置配置文件在`users.xml` 中有单独的配置段. 有关详细信息，请参阅 [设置配置文件](settings-profiles.md).

### user_name/quota {#user-namequota}

配额允许您在一段时间内跟踪或限制资源使用情况。 配额在`users.xml` 中的 `quotas` 配置段下.

您可以为用户分配配额。 有关配额配置的详细说明，请参阅 [配额](../quotas.md#quotas).

### user_name/databases {#user-namedatabases}

在本配置段中，您可以限制ClickHouse中由当前用户进行的 `SELECT` 查询所返回的行，从而实现基本的行级安全性。

**示例**

以下配置使用户 `user1` 通过SELECT查询只能得到table1中id为1000的行

``` xml
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

该 `filter` 可以是[UInt8](../../sql-reference/data-types/int-uint.md)编码的任何表达式。 它通常包含比较和逻辑运算符, 当filter返回0时, database_name.table1 的该行结果将不会返回给用户.过滤不兼容 `PREWHERE` 操作并禁用 `WHERE→PREWHERE` 优化。
