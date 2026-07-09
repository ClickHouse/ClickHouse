---
description: '现有且已正确配置的 ClickHouse 用户可通过 Kerberos 身份验证协议进行身份验证。'
slug: /operations/external-authenticators/kerberos
title: 'Kerberos'
doc_type: 'reference'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<div id="kerberos">
  # Kerberos
</div>

<SelfManaged />

现有且已正确配置的 ClickHouse 用户可通过 Kerberos 身份验证协议进行身份验证。

目前，Kerberos 只能作为现有用户的外部身份验证器，这些用户定义在 `users.xml` 或本地访问控制配置中。此类用户只能使用 HTTP 请求，并且必须能够通过 GSS-SPNEGO 机制完成身份验证。

采用这种方式时，必须在系统中配置 Kerberos，并在 ClickHouse config 中启用。

<div id="enabling-kerberos-in-clickhouse">
  ## 在 ClickHouse 中启用 Kerberos
</div>

要启用 Kerberos，需要在 `config.xml` 中添加 `kerberos` 部分。该部分还可以包含其他参数。

<div id="parameters">
  #### 参数
</div>

* `principal` - 接受安全上下文时将获取并使用的规范服务主体名称。
  * 此参数为可选；如果省略，将使用默认主体。

* `realm` - 用于将身份验证限制为仅允许发起方领域与其匹配的请求的领域。
  * 此参数为可选；如果省略，则不会应用基于领域的额外过滤。

* `keytab` - 服务 keytab 文件的路径。
  * 此参数为可选；如果省略，则必须在 `KRB5_KTNAME` 环境变量中设置服务 keytab 文件路径。

示例 (放入 `config.xml` 中) ：

```xml
<clickhouse>
    <!- ... -->
    <kerberos />
</clickhouse>
```

指定 principal 时：

```xml
<clickhouse>
    <!- ... -->
    <kerberos>
        <principal>HTTP/clickhouse.example.com@EXAMPLE.COM</principal>
    </kerberos>
</clickhouse>
```

按领域筛选：

```xml
<clickhouse>
    <!- ... -->
    <kerberos>
        <realm>EXAMPLE.COM</realm>
    </kerberos>
</clickhouse>
```

:::note
只能定义一个 `kerberos` 部分。如果存在多个 `kerberos` 部分，ClickHouse 将禁用 Kerberos 身份验证。
:::

:::note
不能同时指定 `principal` 和 `realm` 部分。如果同时存在 `principal` 和 `realm` 部分，ClickHouse 将禁用 Kerberos 身份验证。
:::

<div id="kerberos-as-an-external-authenticator-for-existing-users">
  ## Kerberos 作为现有用户的外部身份验证器
</div>

Kerberos 可用作验证本地定义用户身份的方法 (即在 `users.xml` 或本地访问控制配置中定义的用户) 。目前，**只有**通过 HTTP 接口发起的请求才能使用 *Kerberos 身份验证* (通过 GSS-SPNEGO 机制) 。

Kerberos 主体名称格式通常遵循以下模式：

* *primary/instance@REALM*

*/instance* 部分可以出现零次或多次。**身份验证要成功，发起方规范主体名称中的 *primary* 部分应与进行 Kerberos 身份验证的用户名一致**。

<div id="enabling-kerberos-in-users-xml">
  ### 在 `users.xml` 中启用 Kerberos
</div>

要为用户启用 Kerberos 身份验证，请在用户定义中指定 `kerberos` 部分，而不要使用 `password` 或其他类似部分。

参数：

* `realm` - 用于将身份验证限制为仅允许发起方领域与其匹配的请求。
  * 此参数为可选；如果省略，则不会再按领域进行额外过滤。

示例 (放入 `users.xml`) ：

```xml
<clickhouse>
    <!- ... -->
    <users>
        <!- ... -->
        <my_user>
            <!- ... -->
            <kerberos>
                <realm>EXAMPLE.COM</realm>
            </kerberos>
        </my_user>
    </users>
</clickhouse>
```

:::note
请注意，Kerberos 身份验证不能与任何其他身份验证机制同时使用。除了 `kerberos` 之外，如果还配置了 `password` 等其他部分，ClickHouse 将会关闭。
:::

:::info Reminder
请注意，现在只要用户 `my_user` 使用 `kerberos`，就必须按照前文所述在主 `config.xml` 文件中启用 Kerberos。
:::

<div id="enabling-kerberos-using-sql">
  ### 使用 SQL 启用 Kerberos
</div>

当 ClickHouse 启用 [SQL 驱动的访问控制与账户管理](/zh/operations/access-rights#access-control-usage) 时，也可以通过 SQL 语句创建使用 Kerberos 进行身份验证的用户。

```sql
CREATE USER my_user IDENTIFIED WITH kerberos REALM 'EXAMPLE.COM'
```

……或者，不按领域筛选：

```sql
CREATE USER my_user IDENTIFIED WITH kerberos
```