---
sidebar_position: 20
sidebar_label: MySQL接口
---

# MySQL接口 {#mysql-interface}

ClickHouse支持MySQL wire通讯协议。可以通过在配置文件中设置 [mysql_port](../operations/server-configuration-parameters/settings.md#server_configuration_parameters-mysql_port) 来启用它:

``` xml
<mysql_port>9004</mysql_port>
```

使用命令行工具 `mysql` 进行连接的示例:

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

为了与所有MySQL客户端兼容，建议在配置文件中使用 [double SHA1](../operations/settings/settings-users.md#password_double_sha1_hex) 来指定用户密码。
如果使用 [SHA256](../operations/settings/settings-users.md#password_sha256_hex) 指定用户密码，一些客户端将无法进行身份验证（比如mysqljs和旧版本的命令行工具mysql）。

限制:

-   不支持prepared queries

-   某些数据类型以字符串形式发送

[原始文章](https://clickhouse.com/docs/en/interfaces/mysql/) <!--hide-->
