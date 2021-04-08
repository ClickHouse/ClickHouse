---
toc_priority: 20
toc_title: MySQL Interface
---

# MySQL Interface {#mysql-interface}

ClickHouse supports MySQL wire protocol. It can be enabled by [mysql\_port](../operations/server-configuration-parameters/settings.md#server_configuration_parameters-mysql_port) setting in configuration file:

``` xml
<mysql_port>9004</mysql_port>
```

Example of connecting using command-line tool `mysql`:

``` bash
$ mysql --protocol tcp -u default -P 9004
```

Output if a connection succeeded:

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

For compatibility with all MySQL clients, it is recommended to specify user password with [double SHA1](../operations/settings/settings-users.md#password_double_sha1_hex) in configuration file.
If user password is specified using [SHA256](../operations/settings/settings-users.md#password_sha256_hex), some clients won’t be able to authenticate (mysqljs and old versions of command-line tool mysql).

Restrictions:

-   prepared queries are not supported

-   some data types are sent as strings

[Original article](https://clickhouse.tech/docs/en/interfaces/mysql/) <!--hide-->
