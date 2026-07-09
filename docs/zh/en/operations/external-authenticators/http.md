---
description: 'HTTP 相关文档'
slug: /operations/external-authenticators/http
title: 'HTTP'
doc_type: 'reference'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

HTTP 服务器可用于对 ClickHouse 用户进行身份验证。HTTP 身份验证只能作为现有用户的外部身份验证器，这些用户需在 `users.xml` 或本地访问控制配置中定义。目前支持使用 GET 方法的 [Basic](https://datatracker.ietf.org/doc/html/rfc7617) 身份验证方案。

<div id="http-auth-server-definition">
  ## HTTP 身份验证服务器定义
</div>

要定义 HTTP 身份验证服务器，必须在 `config.xml` 中添加 `http_authentication_servers` 部分。

**示例**

```xml
<clickhouse>
    <!- ... -->
    <http_authentication_servers>
        <basic_auth_server>
          <uri>http://localhost:8000/auth</uri>
          <connection_timeout_ms>1000</connection_timeout_ms>
          <receive_timeout_ms>1000</receive_timeout_ms>
          <send_timeout_ms>1000</send_timeout_ms>
          <max_tries>3</max_tries>
          <retry_initial_backoff_ms>50</retry_initial_backoff_ms>
          <retry_max_backoff_ms>1000</retry_max_backoff_ms>
          <forward_headers>
            <name>Custom-Auth-Header-1</name>
            <name>Custom-Auth-Header-2</name>
          </forward_headers>

        </basic_auth_server>
    </http_authentication_servers>
</clickhouse>

```

请注意，您可以在 `http_authentication_servers` 部分中使用不同名称定义多个 HTTP 服务器。

**参数**

* `uri` - 用于发起身份验证请求的 URI

用于与服务器通信的套接字超时时间 (以毫秒为单位) ：

* `connection_timeout_ms` - 默认值：1000 毫秒。
* `receive_timeout_ms` - 默认值：1000 毫秒。
* `send_timeout_ms` - 默认值：1000 毫秒。

重试参数：

* `max_tries` - 发起身份验证请求的最大尝试次数。默认值：3
* `retry_initial_backoff_ms` - 重试时的初始退避间隔。默认值：50 毫秒
* `retry_max_backoff_ms` - 最大退避间隔。默认值：1000 毫秒

转发请求头：

此部分定义了哪些请求头会从客户端请求头转发到外部 HTTP 身份验证器。请注意，请求头会以不区分大小写的方式与配置中的请求头进行匹配，但在转发时会保持原样，即不作修改。

<div id="enabling-http-auth-in-users-xml">
  ### 在 `users.xml` 中启用 HTTP 身份验证
</div>

要为用户启用 HTTP 身份验证，请在用户定义中指定 `http_authentication` 部分，而不是 `password` 或类似部分。

参数：

* `server` - 如前所述，在主 `config.xml` 文件中配置的 HTTP 身份验证服务器名称。
* `scheme` - HTTP 身份验证方案。目前仅支持 `Basic`。默认值：`Basic`

示例 (添加到 `users.xml` 中) ：

```xml
<clickhouse>
    <!- ... -->
    <my_user>
        <!- ... -->
        <http_authentication>
            <server>basic_server</server>
            <scheme>basic</scheme>
        </http_authentication>
    </test_user_2>
</clickhouse>
```

:::note
请注意，HTTP 身份验证不能与任何其他身份验证机制同时使用。如果在 `http_authentication` 的同时存在其他配置节 (如 `password`) ，ClickHouse 将会关闭。
:::

<div id="enabling-http-auth-using-sql">
  ### 通过 SQL 启用 HTTP 身份验证
</div>

在 ClickHouse 中启用 [SQL 驱动的访问控制与账户管理](/zh/operations/access-rights#access-control-usage) 后，也可以使用 SQL 语句创建通过 HTTP 身份验证识别的用户。

```sql
CREATE USER my_user IDENTIFIED WITH HTTP SERVER 'basic_server' SCHEME 'Basic'
```

...或者，如果未显式定义认证方案，则默认使用 `Basic`

```sql
CREATE USER my_user IDENTIFIED WITH HTTP SERVER 'basic_server'
```

<div id="passing-session-settings">
  ### 传递会话设置
</div>

如果来自 HTTP 身份验证服务器的响应正文为 JSON 格式，且包含 `settings` 子对象，ClickHouse 会尝试将其中的 key: value 对按字符串值解析，并将其设为已通过身份验证用户当前会话的会话设置。如果解析失败，则会忽略该服务器返回的响应正文。