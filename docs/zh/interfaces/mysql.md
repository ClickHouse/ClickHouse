---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 20
toc_title: "MySQL\u63A5\u53E3"
---

# MySQL接口 {#mysql-interface}

ClickHouse支持MySQL线协议。 它可以通过启用 [mysql\_port](../operations/server-configuration-parameters/settings.md#server_configuration_parameters-mysql_port) 在配置文件中设置:

``` xml
<mysql_port>9004</mysql_port>
```

使用命令行工具连接的示例 `mysql`:

``` bash
$ mysql --protocol tcp -u default -P 9004
```

如果连接成功，则输出:

``` text
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 4
Server version: 20.2.1.1-ClickHouse

Copyright (c) 2000, 2019, Oracle and/or its affiliates. All rights reserved.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql>
```

为了与所有MySQL客户端兼容，建议使用以下命令指定用户密码 [双SHA1](../operations/settings/settings-users.md#password_double_sha1_hex) 在配置文件中。
如果使用用户密码指定 [SHA256](../operations/settings/settings-users.md#password_sha256_hex)，一些客户端将无法进行身份验证（mysqljs和旧版本的命令行工具mysql）。

限制:

-   不支持准备好的查询

-   某些数据类型以字符串形式发送

[原始文章](https://clickhouse.tech/docs/en/interfaces/mysql/) <!--hide-->
