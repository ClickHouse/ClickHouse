---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 63
toc_title: "\u7528\u6237\u8BBE\u7F6E"
---

# 用户设置 {#user-settings}

该 `users` 一节 `user.xml` 配置文件包含用户设置。

!!! note "信息"
    ClickHouse还支持 [SQL驱动的工作流](../access-rights.md#access-control) 用于管理用户。 我们建议使用它。

的结构 `users` 科:

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

### 用户名称/密码 {#user-namepassword}

密码可以以明文或SHA256（十六进制格式）指定。

-   以明文形式分配密码 (**不推荐**），把它放在一个 `password` 元素。

    例如, `<password>qwerty</password>`. 密码可以留空。

<a id="password_sha256_hex"></a>

-   要使用其SHA256散列分配密码，请将其放置在 `password_sha256_hex` 元素。

    例如, `<password_sha256_hex>65e84be33532fb784c48129675f9eff3a682b27168c0ea744b2cf58ee02337c5</password_sha256_hex>`.

    如何从shell生成密码的示例:

          PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha256sum | tr -d '-'

    结果的第一行是密码。 第二行是相应的SHA256哈希。

<a id="password_double_sha1_hex"></a>

-   为了与MySQL客户端兼容，密码可以在双SHA1哈希中指定。 放进去 `password_double_sha1_hex` 元素。

    例如, `<password_double_sha1_hex>08b4a0f1de6ad37da17359e592c8d74788a83eb0</password_double_sha1_hex>`.

    如何从shell生成密码的示例:

          PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha1sum | tr -d '-' | xxd -r -p | sha1sum | tr -d '-'

    结果的第一行是密码。 第二行是相应的双SHA1哈希。

### 访问管理 {#access_management-user-setting}

此设置启用禁用使用SQL驱动 [访问控制和帐户管理](../access-rights.md#access-control) 对于用户。

可能的值:

-   0 — Disabled.
-   1 — Enabled.

默认值：0。

### 用户名称/网络 {#user-namenetworks}

用户可以从中连接到ClickHouse服务器的网络列表。

列表中的每个元素都可以具有以下形式之一:

-   `<ip>` — IP address or network mask.

    例: `213.180.204.3`, `10.0.0.1/8`, `10.0.0.1/255.255.255.0`, `2a02:6b8::3`, `2a02:6b8::3/64`, `2a02:6b8::3/ffff:ffff:ffff:ffff::`.

-   `<host>` — Hostname.

    示例: `example01.host.ru`.

    要检查访问，将执行DNS查询，并将所有返回的IP地址与对等地址进行比较。

-   `<host_regexp>` — Regular expression for hostnames.

    示例, `^example\d\d-\d\d-\d\.host\.ru$`

    要检查访问，a [DNS PTR查询](https://en.wikipedia.org/wiki/Reverse_DNS_lookup) 对对等体地址执行，然后应用指定的正则表达式。 然后，对PTR查询的结果执行另一个DNS查询，并将所有接收到的地址与对等地址进行比较。 我们强烈建议正则表达式以$结尾。

DNS请求的所有结果都将被缓存，直到服务器重新启动。

**例**

要从任何网络打开用户的访问权限，请指定:

``` xml
<ip>::/0</ip>
```

!!! warning "警告"
    从任何网络开放访问是不安全的，除非你有一个防火墙正确配置或服务器没有直接连接到互联网。

若要仅从本地主机打开访问权限，请指定:

``` xml
<ip>::1</ip>
<ip>127.0.0.1</ip>
```

### user\_name/profile {#user-nameprofile}

您可以为用户分配设置配置文件。 设置配置文件在单独的部分配置 `users.xml` 文件 有关详细信息，请参阅 [设置配置文件](settings-profiles.md).

### 用户名称/配额 {#user-namequota}

配额允许您在一段时间内跟踪或限制资源使用情况。 配额在配置 `quotas`
一节 `users.xml` 配置文件。

您可以为用户分配配额。 有关配额配置的详细说明，请参阅 [配额](../quotas.md#quotas).

### 用户名/数据库 {#user-namedatabases}

在本节中，您可以限制ClickHouse返回的行 `SELECT` 由当前用户进行的查询，从而实现基本的行级安全性。

**示例**

以下配置强制该用户 `user1` 只能看到的行 `table1` 作为结果 `SELECT` 查询，其中的值 `id` 场是1000。

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

该 `filter` 可以是导致任何表达式 [UInt8](../../sql-reference/data-types/int-uint.md)-键入值。 它通常包含比较和逻辑运算符。 从行 `database_name.table1` 其中，不会为此用户返回为0的筛选结果。 过滤是不兼容的 `PREWHERE` 操作和禁用 `WHERE→PREWHERE` 优化。

[原始文章](https://clickhouse.tech/docs/en/operations/settings/settings_users/) <!--hide-->
