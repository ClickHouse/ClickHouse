# Interfaz de MySQL {#mysql-interface}

ClickHouse soporta el protocolo de cable MySQL. Puede ser habilitado por [mysql\_port](../operations/server_settings/settings.md#server_settings-mysql_port) configuración en el archivo de configuración:

``` xml
<mysql_port>9004</mysql_port>
```

Ejemplo de conexión mediante la herramienta de línea de comandos `mysql`:

``` bash
$ mysql --protocol tcp -u default -P 9004
```

Salida si una conexión se realizó correctamente:

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

Para la compatibilidad con todos los clientes MySQL, se recomienda especificar la contraseña de usuario con [Doble SHA1](../operations/settings/settings_users.md#password_double_sha1_hex) en el archivo de configuración.
Si la contraseña de usuario se especifica usando [SHA256](../operations/settings/settings_users.md#password_sha256_hex), algunos clientes no podrán autenticarse (mysqljs y versiones antiguas de la herramienta de línea de comandos mysql).

Restricción:

-   las consultas preparadas no son compatibles

-   algunos tipos de datos se envían como cadenas

[Artículo Original](https://clickhouse.tech/docs/es/interfaces/mysql/) <!--hide-->
