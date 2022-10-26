---
sidebar_position: 20
sidebar_label: MySQL Interface
---

# MySQL Interface

ClickHouse supports MySQL wire protocol. It can be enabled by [mysql_port](../operations/server-configuration-parameters/settings.md#server_configuration_parameters-mysql_port) setting in configuration file:

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
If user password is specified using [SHA256](../operations/settings/settings-users.md#password_sha256_hex), some clients won’t be able to authenticate (mysqljs and old versions of command-line tool MySQL and MariaDB).

Restrictions:

-   prepared queries are not supported

-   some data types are sent as strings

To cancel a long query use `KILL QUERY connection_id` statement (it is replaced with `KILL QUERY WHERE query_id = connection_id` while proceeding). For example:

``` bash
$ mysql --protocol tcp -h mysql_server -P 9004 default -u default --password=123 -e "KILL QUERY 123456;"
```

[Original article](https://clickhouse.com/docs/en/interfaces/mysql/) <!--hide-->
