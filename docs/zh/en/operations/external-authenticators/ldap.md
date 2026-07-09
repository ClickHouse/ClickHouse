---
description: '配置 ClickHouse LDAP 身份验证的指南'
slug: /operations/external-authenticators/ldap
title: 'LDAP'
doc_type: 'reference'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

LDAP 服务器可用于对 ClickHouse 用户进行身份验证。实现这一点有两种不同的方法：

* 将 LDAP 用作现有用户的外部身份验证器；这些用户定义在 `users.xml` 或本地访问控制配置中。
* 将 LDAP 用作外部用户目录，并允许对本地未定义的用户进行身份验证，前提是这些用户存在于 LDAP 服务器中。

对于这两种方法，都必须在 ClickHouse 配置中定义一个内部命名的 LDAP 服务器，以便配置中的其他部分引用它。

<div id="ldap-server-definition">
  ## LDAP 服务器定义
</div>

要定义 LDAP 服务器，必须在 `config.xml` 中添加 `ldap_servers` 部分。

**示例**

```xml
<clickhouse>
    <!- ... -->
    <ldap_servers>
        <!- Typical LDAP server. -->
        <my_ldap_server>
            <host>localhost</host>
            <port>636</port>
            <bind_dn>uid={user_name},ou=users,dc=example,dc=com</bind_dn>
            <verification_cooldown>300</verification_cooldown>
            <follow_referrals>false</follow_referrals>
            <enable_tls>yes</enable_tls>
            <tls_minimum_protocol_version>tls1.2</tls_minimum_protocol_version>
            <tls_require_cert>demand</tls_require_cert>
            <tls_cert_file>/path/to/tls_cert_file</tls_cert_file>
            <tls_key_file>/path/to/tls_key_file</tls_key_file>
            <tls_ca_cert_file>/path/to/tls_ca_cert_file</tls_ca_cert_file>
            <tls_ca_cert_dir>/path/to/tls_ca_cert_dir</tls_ca_cert_dir>
            <tls_cipher_suite>ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:AES256-GCM-SHA384</tls_cipher_suite>
        </my_ldap_server>

        <!- Typical Active Directory with configured user DN detection for further role mapping. -->
        <my_ad_server>
            <host>localhost</host>
            <port>389</port>
            <bind_dn>EXAMPLE\{user_name}</bind_dn>
            <user_dn_detection>
                <base_dn>CN=Users,DC=example,DC=com</base_dn>
                <search_filter>(&amp;(objectClass=user)(sAMAccountName={user_name}))</search_filter>
            </user_dn_detection>
            <enable_tls>no</enable_tls>
        </my_ad_server>
    </ldap_servers>
</clickhouse>
```

请注意，您可以在 `ldap_servers` 部分中使用不同的名称来定义多个 LDAP 服务器。

**参数**

| 参数                             | 默认值           | 描述                                                                                                                                                                                                                                                                   |
| ------------------------------ | ------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `host`                         | —             | LDAP 服务器主机名或 IP。此参数为必填项，不能为空。                                                                                                                                                                                                                                        |
| `port`                         | `636` / `389` | LDAP 服务器端口。如果 `enable_tls` 设置为 `yes`，默认值为 `636`；否则为 `389`。                                                                                                                                                                                                           |
| `bind_dn`                      | —             | 用于构造 bind DN 的模板。在每次身份验证尝试时，模板中所有 `{user_name}` 子字符串都会被替换为实际用户名，从而生成最终的 DN。                                                                                                                                                                                             |
| `auth_dn_prefix`               | —             | **已弃用。** `bind_dn` 的另一种写法。不能与 `bind_dn` 同时使用。指定后，bind DN 将按 `auth_dn_prefix + {user_name} + auth_dn_suffix` 构造。例如，将 `auth_dn_prefix` 设置为 `uid=`，并将 `auth_dn_suffix` 设置为 `,ou=users,dc=example,dc=com`，等同于将 `bind_dn` 设置为 `uid={user_name},ou=users,dc=example,dc=com`。 |
| `auth_dn_suffix`               | —             | **已弃用。** 参见 `auth_dn_prefix`。                                                                                                                                                                                                                                        |
| `verification_cooldown`        | `0`           | 成功绑定后的一段时间 (单位为秒) 。在此期间，后续所有请求都会被视为该用户已成功通过身份验证，而无需联系 LDAP 服务器。指定 `0` 可禁用缓存，并强制每次身份验证请求都联系 LDAP 服务器。                                                                                                                                                                 |
| `follow_referrals`             | `false`       | 一个标志，用于允许 LDAP 客户端库自动跟踪服务器返回的 LDAP 转介。该参数主要适用于 Microsoft Active Directory 环境；在这类环境中，对高层级 base DN (例如 `DC=example,DC=com`) 执行子树搜索时，可能会返回转介/搜索引用 (例如 `DC=DomainDnsZones,...`) 。仅当你明确需要跨分区搜索时，才将其设置为 `true`。                                                            |
| `enable_tls`                   | `yes`         | 一个标志，用于启用与 LDAP 服务器的安全连接。指定 `no` 表示使用明文 `ldap://` 协议 (不推荐) ，指定 `yes` 表示使用基于 SSL/TLS 的 LDAP `ldaps://` 协议 (推荐) ，指定 `starttls` 表示使用传统的 StartTLS 协议 (先使用明文 `ldap://` 协议，再升级为 TLS) 。                                                                                     |
| `tls_minimum_protocol_version` | `tls1.2`      | SSL/TLS 的最低协议版本。可接受的值：`ssl2`、`ssl3`、`tls1.0`、`tls1.1`、`tls1.2`。                                                                                                                                                                                                      |
| `tls_require_cert`             | `demand`      | SSL/TLS 对端证书验证行为。可接受的值：`never`、`allow`、`try`、`demand`。                                                                                                                                                                                                               |
| `tls_cert_file`                | —             | 证书文件路径。                                                                                                                                                                                                                                                              |
| `tls_key_file`                 | —             | 证书密钥文件路径。                                                                                                                                                                                                                                                            |
| `tls_ca_cert_file`             | —             | CA 证书文件路径。                                                                                                                                                                                                                                                           |
| `tls_ca_cert_dir`              | —             | 包含 CA 证书的目录路径。                                                                                                                                                                                                                                                       |
| `tls_cipher_suite`             | —             | 允许的密码套件 (采用 OpenSSL 表示法) 。                                                                                                                                                                                                                                           |
| `search_limit`                 | `256`         | 此服务器定义执行的 LDAP 搜索查询可返回的最大条目数 (用于 user DN 检测和角色映射) 。                                                                                                                                                                                                                  |

**`user_dn_detection` 子参数**

本节包含用于检测已绑定用户实际 user DN 的 LDAP 搜索参数。这主要用于服务器为 Active Directory 时，在后续角色映射中编写搜索过滤器。得到的 user DN 将用于替换所有允许位置中的 `{user_dn}` 子字符串。默认情况下，user DN 等于 bind DN；但一旦执行搜索，它将更新为实际检测到的 user DN 值。

| 参数              | 默认值       | 描述                                                                                                                                               |
| --------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| `base_dn`       | —         | 用于构造 LDAP 搜索 base DN 的模板。在 LDAP 搜索期间，模板中所有 `{user_name}` 和 `{bind_dn}` 子字符串都会被替换为实际用户名和 bind DN，从而生成最终的 DN。                                      |
| `scope`         | `subtree` | LDAP 搜索范围。可接受的值：`base`、`one_level`、`children`、`subtree`。                                                                                         |
| `search_filter` | —         | 用于构造 LDAP 搜索过滤器的模板。在 LDAP 搜索期间，模板中所有 `{user_name}`、`{bind_dn}` 和 `{base_dn}` 子字符串都会被替换为实际用户名、bind DN 和 base DN，从而生成最终的过滤器。请注意，特殊字符必须在 XML 中正确转义。 |

<div id="ldap-external-authenticator">
  ## LDAP 外部身份验证器
</div>

远程 LDAP 服务器可作为验证本地定义用户 (在 `users.xml` 或本地访问控制配置中定义的用户) 密码的一种方式。为此，请在用户定义中指定先前定义的 LDAP 服务器名称，而不要使用 `password` 或类似部分。

每次尝试登录时，ClickHouse 都会使用提供的凭据，尝试绑定到 [LDAP 服务器定义](#ldap-server-definition) 中由 `bind_dn` 参数指定的 DN；如果绑定成功，则该用户会被视为已通过身份验证。这通常称为“简单绑定”方法。

**示例**

```xml
<clickhouse>
    <!- ... -->
    <users>
        <!- ... -->
        <my_user>
            <!- ... -->
            <ldap>
                <server>my_ldap_server</server>
            </ldap>
        </my_user>
    </users>
</clickhouse>
```

请注意，用户 `my_user` 对应的是 `my_ldap_server`。必须按照前文所述，在主 `config.xml` 文件中配置该 LDAP 服务器。

启用 SQL 驱动的 [访问控制与账户管理](/zh/operations/access-rights#access-control-usage) 后，也可以使用 [CREATE USER](/zh/sql-reference/statements/create/user) 语句创建由 LDAP 服务器认证的用户。

```sql title="Query"
CREATE USER my_user IDENTIFIED WITH ldap SERVER 'my_ldap_server';
```

<div id="ldap-external-user-directory">
  ## LDAP 外部用户目录
</div>

除了本地定义的用户外，还可以使用远程 LDAP 服务器作为用户定义来源。为此，请在 `config.xml` 文件中 `users_directories` 下的 `ldap` 部分指定此前定义的 LDAP 服务器名称 (请参见 [LDAP 服务器定义](#ldap-server-definition)) 。

每次尝试登录时，ClickHouse 都会先在本地查找用户定义，并按常规进行身份验证。如果该用户未定义，ClickHouse 会认为该用户定义存在于外部 LDAP 目录中，并尝试使用提供的凭据在 LDAP 服务器上对指定的 DN 执行“绑定”。如果成功，则该用户会被视为存在且已通过身份验证。系统会为该用户分配 `roles` 部分中指定列表里的角色。此外，如果还配置了 `role_mapping` 部分，也可以执行 LDAP“搜索”，并将结果转换为角色名称，再分配给该用户。所有这些都意味着已启用 SQL 驱动的 [访问控制与账户管理](/zh/operations/access-rights#access-control-usage)，并且角色是使用 [CREATE ROLE](/zh/sql-reference/statements/create/role) 语句创建的。

**示例**

添加到 `config.xml` 中。

```xml
<clickhouse>
    <!- ... -->
    <user_directories>
        <!- Typical LDAP server. -->
        <ldap>
            <server>my_ldap_server</server>
            <roles>
                <my_local_role1 />
                <my_local_role2 />
            </roles>
            <role_mapping>
                <base_dn>ou=groups,dc=example,dc=com</base_dn>
                <scope>subtree</scope>
                <search_filter>(&amp;(objectClass=groupOfNames)(member={bind_dn}))</search_filter>
                <attribute>cn</attribute>
                <prefix>clickhouse_</prefix>
            </role_mapping>
        </ldap>

        <!- Typical Active Directory with role mapping that relies on the detected user DN. -->
        <ldap>
            <server>my_ad_server</server>
            <role_mapping>
                <base_dn>CN=Users,DC=example,DC=com</base_dn>
                <attribute>CN</attribute>
                <scope>subtree</scope>
                <search_filter>(&amp;(objectClass=group)(member={user_dn}))</search_filter>
                <prefix>clickhouse_</prefix>
            </role_mapping>
        </ldap>
    </user_directories>
</clickhouse>
```

请注意，`user_directories` 部分中 `ldap` 小节里引用的 `my_ldap_server` 必须是一个先前已定义并在 `config.xml` 中配置好的 LDAP 服务器 (请参见 [LDAP 服务器定义](#ldap-server-definition)) 。

**参数**

| 参数       | 默认值 | 描述                                                                                                 |
| -------- | --- | -------------------------------------------------------------------------------------------------- |
| `server` | —   | 上文 `ldap_servers` config 小节中定义的 LDAP 服务器名称之一。此参数为必填项，且不能为空。                                        |
| `roles`  | —   | 该小节包含一组本地定义的角色，这些角色会分配给从 LDAP 服务器获取的每个用户。如果此处未指定任何角色，且在角色映射期间 (见下文) 也未分配任何角色，则用户在完成身份验证后将无法执行任何操作。 |

**`role_mapping` 子参数**

本小节包含 LDAP 搜索参数和映射规则。用户进行身份验证时，在仍与 LDAP 保持绑定的情况下，会使用 `search_filter` 和登录用户名执行 LDAP 搜索。对于该搜索中找到的每个条目，都会提取指定 attribute 的值。对于每个带有指定前缀的 attribute 值，都会移除该前缀，其余部分将作为 ClickHouse 中本地定义角色的名称；该角色应事先通过 [CREATE ROLE](/zh/sql-reference/statements/create/role) 语句创建。同一个 `ldap` 小节中可以定义多个 `role_mapping` 小节，且都会生效。

| 参数              | 默认值       | 说明                                                                                                                                                                    |
| --------------- | --------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `base_dn`       | —         | 用于构造 LDAP 搜索 base DN 的模板。每次执行 LDAP 搜索时，都会将模板中的所有 `{user_name}`、`{bind_dn}` 和 `{user_dn}` 子串替换为实际的用户名、bind DN 和 user DN，以生成最终的 DN。                                     |
| `scope`         | `subtree` | LDAP 搜索的范围。可接受的值：`base`、`one_level`、`children`、`subtree`。                                                                                                             |
| `search_filter` | —         | 用于构造 LDAP 搜索过滤器的模板。每次执行 LDAP 搜索时，都会将模板中的所有 `{user_name}`、`{bind_dn}`、`{user_dn}` 和 `{base_dn}` 子串替换为实际的用户名、bind DN、user DN 和 base DN，以生成最终的过滤器。请注意，特殊字符必须在 XML 中正确转义。 |
| `attribute`     | `cn`      | LDAP 搜索将返回其值的属性名。                                                                                                                                                     |
| `prefix`        | 空         | LDAP 搜索返回的原始字符串列表中，预期每个字符串前都带有此前缀。该前缀会从原始字符串中移除，得到的字符串将被视为本地角色名。                                                                                                      |