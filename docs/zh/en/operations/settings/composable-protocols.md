---
description: 'Composable protocols 可更灵活地配置对 ClickHouse 服务器 的 TCP 访问。'
sidebar_label: 'Composable protocols'
sidebar_position: 64
slug: /operations/settings/composable-protocols
title: 'Composable protocols'
doc_type: 'reference'
---

<div id="overview">
  ## 概述
</div>

Composable protocols 支持以更灵活的方式配置对
ClickHouse 服务器的 TCP 访问。此配置既可与传统配置并存，也可替代
传统配置。

<div id="composable-protocols-section-is-denoted-as-protocols-in-configuration-xml">
  ## 配置 Composable protocols
</div>

可以在 XML 配置文件中配置 Composable protocols。该部分在 XML 配置文件中由 `protocols` 标签标识：

```xml
<protocols>

</protocols>
```

<div id="basic-modules-define-protocol-layers">
  ### 配置协议 layer
</div>

你可以使用基础模块定义协议 layer。例如，要定义一个
HTTP layer，可以在 `protocols` 部分添加一个新的基础模块：

```xml
<protocols>

  <!-- plain_http module -->
  <plain_http>
    <type>http</type>
  </plain_http>

</protocols>
```

模块可按以下内容进行配置：

* `plain_http` - 可供另一layer引用的名称
* `type` - 表示将实例化用于处理数据的协议处理程序。
  它支持以下一组预定义的协议处理程序：
  * `tcp` - ClickHouse 原生协议处理程序
  * `http` - ClickHouse HTTP 协议处理程序
  * `tls` - TLS 加密layer
  * `proxy1` - PROXYv1 layer
  * `mysql` - MySQL 兼容性协议处理程序
  * `postgres` - PostgreSQL 兼容性协议处理程序
  * `prometheus` - Prometheus 协议处理程序
  * `interserver` - ClickHouse interserver 处理程序

:::note
`gRPC` 协议处理程序尚未在 `Composable protocols` 中实现
:::

<div id="endpoint-ie-listening-port-is-denoted-by-port-and-optional-host-tags">
  ### 配置端点
</div>

端点 (监听端口) 用 `<port>` 和可选的 `<host>` 标签表示。
例如，要在前面添加的 HTTP layer 上配置一个端点，我们
可以按如下方式修改配置：

```xml
<protocols>

  <plain_http>

    <type>http</type>
    <!-- endpoint -->
    <host>127.0.0.1</host>
    <port>8123</port>

  </plain_http>

</protocols>
```

如果省略 `<host>` 标签，则使用根配置中的 `<listen_host>`。

<div id="layers-sequence-is-defined-by-impl-tag-referencing-another-module">
  ### 配置 layer 序列
</div>

layer 序列通过 `<impl>` 标签定义，并引用另一个
模块。例如，要在我们的 plain&#95;http 模块 之上配置一个 TLS layer，
可以按如下方式进一步修改配置：

```xml
<protocols>

  <!-- http module -->
  <plain_http>
    <type>http</type>
  </plain_http>

  <!-- https module configured as a tls layer on top of plain_http module -->
  <https>
    <type>tls</type>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8443</port>
  </https>

</protocols>
```

<div id="endpoint-can-be-attached-to-any-layer">
  ### 将端点关联到layer
</div>

端点可以关联到任何layer。例如，我们可以为
HTTP (端口 8123) 和 HTTPS (端口 8443) 定义端点：

```xml
<protocols>

  <plain_http>
    <type>http</type>
    <host>127.0.0.1</host>
    <port>8123</port>
  </plain_http>

  <https>
    <type>tls</type>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8443</port>
  </https>

</protocols>
```

<div id="additional-endpoints-can-be-defined-by-referencing-any-module-and-omitting-type-tag">
  ### 定义其他端点
</div>

通过引用任意模块并省略
`<type>` 标签，可以定义其他端点。例如，我们可以按如下方式为
`plain_http` 模块定义 `another_http` 端点：

```xml
<protocols>

  <plain_http>
    <type>http</type>
    <host>127.0.0.1</host>
    <port>8123</port>
  </plain_http>

  <https>
    <type>tls</type>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8443</port>
  </https>

  <another_http>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8223</port>
  </another_http>

</protocols>
```

<div id="custom-http-handlers-per-endpoint">
  ### 按端点自定义 HTTP 处理器
</div>

默认情况下，所有 `type=http` 协议条目共用同一个 `<http_handlers>`
配置。你可以添加一个 `<handlers>` 标签并将其指向其他配置节，以覆盖这一默认行为。
这样，每个 HTTP 端口都可以提供一组不同的 HTTP 路由规则。

例如，要在 8124 端口上运行一个使用自身处理器的替代 HTTP API：

```xml
<protocols>

  <plain_http>
    <type>http</type>
    <host>127.0.0.1</host>
    <port>8123</port>
  </plain_http>

  <alt_http>
    <type>http</type>
    <host>127.0.0.1</host>
    <port>8124</port>
    <handlers>http_handlers_alt</handlers>
  </alt_http>

</protocols>

<!-- Default handlers used by plain_http (port 8123) -->
<http_handlers>
    <defaults/>
</http_handlers>

<!-- Alternative handlers used by alt_http (port 8124) -->
<http_handlers_alt>
    <rule>
        <url>/custom</url>
        <handler>
            <type>predefined_query_handler</type>
            <query>SELECT 'custom_endpoint'</query>
        </handler>
    </rule>
    <defaults/>
</http_handlers_alt>
```

在此示例中，发往端口 8123 的请求使用标准的 `<http_handlers>` 规则，
而发往端口 8124 的请求使用 `<http_handlers_alt>` 规则。如果省略 `<handlers>`，
该端点将回退到默认的 `<http_handlers>`。

自定义处理程序部分遵循与
[`<http_handlers>`](/zh/docs/operations/server-configuration-parameters/settings#http_handlers) 相同的格式。
重新加载配置时，系统会检测到对自定义处理程序部分的更改，并自动重启
相应的端点。

<div id="some-modules-can-contain-specific-for-its-layer-parameters">
  ### 指定额外的 layer 参数
</div>

某些模块可能包含额外的 layer 参数。例如，TLS layer
允许按如下方式指定私钥 (`privateKeyFile`) 和证书文件 (`certificateFile`) ：

```xml
<protocols>

  <plain_http>
    <type>http</type>
    <host>127.0.0.1</host>
    <port>8123</port>
  </plain_http>

  <https>
    <type>tls</type>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8443</port>
    <privateKeyFile>another_server.key</privateKeyFile>
    <certificateFile>another_server.crt</certificateFile>
  </https>

</protocols>
```