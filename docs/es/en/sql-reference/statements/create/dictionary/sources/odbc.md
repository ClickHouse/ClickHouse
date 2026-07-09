---
slug: /sql-reference/statements/create/dictionary/sources/odbc
title: 'Fuente de diccionario ODBC'
sidebar_position: 6
sidebar_label: 'ODBC'
description: 'Configure una conexión ODBC como fuente de diccionario en ClickHouse.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Puede utilizar este método para conectarse a cualquier base de datos que tenga un controlador ODBC.

Ejemplo de ajustes:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(ODBC(
        db 'DatabaseName'
        table 'SchemaName.TableName'
        connection_string 'DSN=some_parameters'
        invalidate_query 'SQL_QUERY'
        query 'SELECT id, value_1, value_2 FROM db_name.table_name'
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="Archivo de configuración">
    ```xml
    <source>
        <odbc>
            <db>DatabaseName</db>
            <table>ShemaName.TableName</table>
            <connection_string>DSN=some_parameters</connection_string>
            <invalidate_query>SQL_QUERY</invalidate_query>
            <query>SELECT id, value_1, value_2 FROM ShemaName.TableName</query>
        </odbc>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

Campos de configuración:

| Setting                | Description                                                                                                                                                   |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `db`                   | Nombre de la base de datos. Omítalo si el nombre de la base de datos está establecido en los parámetros de `<connection_string>`.                             |
| `table`                | Nombre de la tabla y del esquema, si existe.                                                                                                                  |
| `connection_string`    | Cadena de conexión.                                                                                                                                           |
| `invalidate_query`     | Consulta para comprobar el estado del diccionario. Opcional. Lea más en la sección [Actualización de datos de diccionario mediante LIFETIME](../lifetime.md). |
| `background_reconnect` | Volver a conectarse a la réplica en segundo plano si falla la conexión. Opcional.                                                                             |
| `query`                | Consulta personalizada. Opcional.                                                                                                                             |

:::note
Los campos `table` y `query` no pueden usarse juntos. Debe declararse uno de los dos campos: `table` o `query`.
:::

ClickHouse recibe los caracteres de comillas del controlador ODBC y entrecomilla todos los ajustes en las consultas enviadas al controlador, por lo que es necesario especificar el nombre de la tabla respetando el uso de mayúsculas y minúsculas en la base de datos.

Si tiene problemas de codificación al usar Oracle, consulte la entrada correspondiente de [FAQ](/es/knowledgebase/oracle-odbc).

<div id="known-vulnerability-of-the-odbc-dictionary-functionality">
  ### Vulnerabilidad conocida de la funcionalidad de Diccionario ODBC
</div>

:::note
Al conectarse a la base de datos mediante ODBC, el parámetro de conexión del controlador `Servername` puede sustituirse. En ese caso, los valores de `USERNAME` y `PASSWORD` de `odbc.ini` se envían al servidor remoto y podrían verse comprometidos.
:::

**Ejemplo de uso inseguro**

Configuremos unixODBC para PostgreSQL. Contenido de `/etc/odbc.ini`:

```text
[gregtest]
Driver = /usr/lib/psqlodbca.so
Servername = localhost
PORT = 5432
DATABASE = test_db
#OPTION = 3
USERNAME = test
PASSWORD = test
```

Si a continuación realiza una consulta como

```sql
SELECT * FROM odbc('DSN=gregtest;Servername=some-server.com', 'test_db');
```

El controlador ODBC enviará los valores de `USERNAME` y `PASSWORD` de `odbc.ini` a `some-server.com`.

<div id="example-of-connecting-postgresql">
  ### Ejemplo de conexión a PostgreSQL
</div>

Sistema operativo Ubuntu.

Instalación de unixODBC y del ODBC controlador para PostgreSQL:

```bash
$ sudo apt-get install -y unixodbc odbcinst odbc-postgresql
```

Configuración de `/etc/odbc.ini` (o `~/.odbc.ini` si ha iniciado sesión como el usuario que ejecuta ClickHouse):

```text
    [DEFAULT]
    Driver = myconnection

    [myconnection]
    Description         = PostgreSQL connection to my_db
    Driver              = PostgreSQL Unicode
    Database            = my_db
    Servername          = 127.0.0.1
    UserName            = username
    Password            = password
    Port                = 5432
    Protocol            = 9.3
    ReadOnly            = No
    RowVersioning       = No
    ShowSystemTables    = No
    ConnSettings        =
```

La configuración del diccionario en ClickHouse:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY table_name (
        id UInt64,
        some_column UInt64 DEFAULT 0
    )
    PRIMARY KEY id
    SOURCE(ODBC(connection_string 'DSN=myconnection' table 'postgresql_table'))
    LAYOUT(HASHED())
    LIFETIME(MIN 300 MAX 360)
    ```
  </TabItem>

  <TabItem value="xml" label="Archivo de configuración">
    ```xml
    <clickhouse>
        <dictionary>
            <name>table_name</name>
            <source>
                <odbc>
                    <!-- Puede especificar los siguientes parámetros en connection_string: -->
                    <!-- DSN=myconnection;UID=username;PWD=password;HOST=127.0.0.1;PORT=5432;DATABASE=my_db -->
                    <connection_string>DSN=myconnection</connection_string>
                    <table>postgresql_table</table>
                </odbc>
            </source>
            <lifetime>
                <min>300</min>
                <max>360</max>
            </lifetime>
            <layout>
                <hashed/>
            </layout>
            <structure>
                <id>
                    <name>id</name>
                </id>
                <attribute>
                    <name>some_column</name>
                    <type>UInt64</type>
                    <null_value>0</null_value>
                </attribute>
            </structure>
        </dictionary>
    </clickhouse>
    ```
  </TabItem>
</Tabs>

<br />

Puede que necesite editar `odbc.ini` para especificar la ruta completa a la biblioteca del controlador `DRIVER=/usr/local/lib/psqlodbcw.so`.

<div id="example-of-connecting-ms-sql-server">
  ### Ejemplo de conexión a MS SQL Server
</div>

Sistema operativo Ubuntu.

Instalación del controlador ODBC para conectarse a MS SQL:

```bash
$ sudo apt-get install tdsodbc freetds-bin sqsh
```

Configuración del controlador:

```bash
    $ cat /etc/freetds/freetds.conf
    ...

    [MSSQL]
    host = 192.168.56.101
    port = 1433
    tds version = 7.0
    client charset = UTF-8

    # test TDS connection
    $ sqsh -S MSSQL -D database -U user -P password


    $ cat /etc/odbcinst.ini

    [FreeTDS]
    Description     = FreeTDS
    Driver          = /usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so
    Setup           = /usr/lib/x86_64-linux-gnu/odbc/libtdsS.so
    FileUsage       = 1
    UsageCount      = 5

    $ cat /etc/odbc.ini
    # $ cat ~/.odbc.ini # if you signed in under a user that runs ClickHouse

    [MSSQL]
    Description     = FreeTDS
    Driver          = FreeTDS
    Servername      = MSSQL
    Database        = test
    UID             = test
    PWD             = test
    Port            = 1433


    # (optional) test ODBC connection (to use isql-tool install the [unixodbc](https://packages.debian.org/sid/unixodbc)-package)
    $ isql -v MSSQL "user" "password"
```

Observaciones:

* para determinar la versión más antigua de TDS compatible con una versión específica de SQL Server, consulte la documentación del producto o consulte [MS-TDS Product Behavior](https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/135d0ebe-5c4c-4a94-99bf-1811eccb9f4a)

Configuración del diccionario en ClickHouse:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY test (
        k UInt64,
        s String DEFAULT ''
    )
    PRIMARY KEY k
    SOURCE(ODBC(table 'dict' connection_string 'DSN=MSSQL;UID=test;PWD=test'))
    LAYOUT(FLAT())
    LIFETIME(MIN 300 MAX 360)
    ```
  </TabItem>

  <TabItem value="xml" label="Archivo de configuración">
    ```xml
    <clickhouse>
        <dictionary>
            <name>test</name>
            <source>
                <odbc>
                    <table>dict</table>
                    <connection_string>DSN=MSSQL;UID=test;PWD=test</connection_string>
                </odbc>
            </source>

            <lifetime>
                <min>300</min>
                <max>360</max>
            </lifetime>

            <layout>
                <flat />
            </layout>

            <structure>
                <id>
                    <name>k</name>
                </id>
                <attribute>
                    <name>s</name>
                    <type>String</type>
                    <null_value></null_value>
                </attribute>
            </structure>
        </dictionary>
    </clickhouse>
    ```
  </TabItem>
</Tabs>