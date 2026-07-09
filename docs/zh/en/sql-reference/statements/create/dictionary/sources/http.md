---
slug: /sql-reference/statements/create/dictionary/sources/http
title: 'HTTP(S) 字典源'
sidebar_position: 5
sidebar_label: 'HTTP(S)'
description: '将 HTTP 或 HTTPS 端点配置为 ClickHouse 的字典源。'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

使用 HTTP(S) 服务器的方式取决于[字典在内存中的存储布局](../layouts/)。如果字典采用 `cache` 和 `complex_key_cache` 存储，ClickHouse 会通过 `POST` 方法发送请求，以获取所需的键。

设置示例：

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(HTTP(
        url 'http://[::1]/os.tsv'
        format 'TabSeparated'
        credentials(user 'user' password 'password')
        headers(header(name 'API-KEY' value 'key'))
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="配置文件">
    ```xml
    <source>
        <http>
            <url>http://[::1]/os.tsv</url>
            <format>TabSeparated</format>
            <credentials>
                <user>user</user>
                <password>password</password>
            </credentials>
            <headers>
                <header>
                    <name>API-KEY</name>
                    <value>key</value>
                </header>
            </headers>
        </http>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

要让 ClickHouse 能访问 HTTPS 资源，必须在服务器配置中[配置 openSSL](/zh/operations/server-configuration-parameters/settings#openssl)。

设置字段：

| Setting       | Description                                         |
| ------------- | --------------------------------------------------- |
| `url`         | 源 URL。                                              |
| `format`      | 文件格式。支持 [Formats](/zh/sql-reference/formats) 中描述的所有格式。 |
| `credentials` | HTTP Basic 身份验证。可选。                                 |
| `user`        | 身份验证所需的用户名。                                         |
| `password`    | 身份验证所需的密码。                                          |
| `headers`     | HTTP 请求中使用的所有自定义 HTTP 请求头条目。可选。                     |
| `header`      | 单个 HTTP 请求头条目。                                      |
| `name`        | 请求中发送的请求头名称。                                        |
| `value`       | 为特定请求头名称设置的值。                                       |

使用 DDL 命令 (`CREATE DICTIONARY ...`) 创建字典时，会根据配置中 `remote_url_allow_hosts` 部分的内容检查 HTTP 字典的远程主机，以防止数据库用户访问任意 HTTP 服务器。