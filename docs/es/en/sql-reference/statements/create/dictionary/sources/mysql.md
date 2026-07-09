---
slug: /sql-reference/statements/create/dictionary/sources/mysql
title: 'Origen del diccionario MySQL'
sidebar_position: 7
sidebar_label: 'MySQL'
description: 'Configura MySQL como origen de diccionario en ClickHouse.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Ejemplo de configuración:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(MYSQL(
        port 3306
        user 'clickhouse'
        password 'qwerty'
        replica(host 'example01-1' priority 1)
        replica(host 'example01-2' priority 1)
        db 'db_name'
        table 'table_name'
        where 'id=10'
        invalidate_query 'SQL_QUERY'
        fail_on_connection_loss 'true'
        query 'SELECT id, value_1, value_2 FROM db_name.table_name'
        enable_compression 1
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="Archivo de configuración">
    ```xml
    <source>
      <mysql>
          <port>3306</port>
          <user>clickhouse</user>
          <password>qwerty</password>
          <replica>
              <host>example01-1</host>
              <priority>1</priority>
          </replica>
          <replica>
              <host>example01-2</host>
              <priority>1</priority>
          </replica>
          <db>db_name</db>
          <table>table_name</table>
          <where>id=10</where>
          <invalidate_query>SQL_QUERY</invalidate_query>
          <fail_on_connection_loss>true</fail_on_connection_loss>
          <query>SELECT id, value_1, value_2 FROM db_name.table_name</query>
          <enable_compression>1</enable_compression>
      </mysql>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

Campos de configuración:

| Configuración             | Descripción                                                                                                                                                                                                                                                                                                                                                                                 |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `port`                    | El puerto del servidor MySQL. Puede especificarlo para todas las réplicas o para cada una individualmente (dentro de `<replica>`).                                                                                                                                                                                                                                                          |
| `user`                    | Nombre del usuario MySQL. Puede especificarlo para todas las réplicas o para cada una individualmente (dentro de `<replica>`).                                                                                                                                                                                                                                                              |
| `password`                | Contraseña del usuario MySQL. Puede especificarlo para todas las réplicas o para cada una individualmente (dentro de `<replica>`).                                                                                                                                                                                                                                                          |
| `replica`                 | Sección de configuraciones de réplica. Puede haber varias secciones.                                                                                                                                                                                                                                                                                                                        |
| `replica/host`            | El host de MySQL.                                                                                                                                                                                                                                                                                                                                                                           |
| `replica/priority`        | La prioridad de la réplica. Al intentar conectarse, ClickHouse recorre las réplicas en orden de prioridad. Cuanto menor sea el número, mayor será la prioridad.                                                                                                                                                                                                                             |
| `db`                      | Nombre de la base de datos.                                                                                                                                                                                                                                                                                                                                                                 |
| `table`                   | Nombre de la tabla.                                                                                                                                                                                                                                                                                                                                                                         |
| `where`                   | Los criterios de selección. La sintaxis de las condiciones es la misma que la de la cláusula `WHERE` en MySQL; por ejemplo, `id > 10 AND id < 20`. Opcional.                                                                                                                                                                                                                                |
| `invalidate_query`        | Consulta para comprobar el estado del Diccionario. Opcional. Lea más en la sección [Refreshing dictionary data using LIFETIME](../lifetime.md).                                                                                                                                                                                                                                             |
| `fail_on_connection_loss` | Controla el comportamiento del servidor cuando se pierde la conexión. Si es `true`, se lanza inmediatamente una excepción si se pierde la conexión entre el client y el servidor. Si es `false`, el servidor reintenta obtener los datos al menos tres veces antes de informar de un error. Tenga en cuenta que reintentar aumenta los tiempos de respuesta. Valor predeterminado: `false`. |
| `query`                   | La consulta personalizada. Opcional.                                                                                                                                                                                                                                                                                                                                                        |
| `enable_compression`      | Habilita la compresión zlib para la conexión del protocolo MySQL. Cuando se establece en `1`, ClickHouse solicita compresión a nivel de protocolo al servidor MySQL. También puede establecerse por réplica dentro de `<replica>`. Valor predeterminado: `0`.                                                                                                                               |

:::note
Los campos `table` o `where` no pueden usarse junto con el campo `query`. Además, debe declararse uno de los campos `table` o `query`.
:::

:::note
No existe un parámetro explícito `secure`. Al establecer una conexión SSL, la seguridad es obligatoria.
:::

Puede conectarse a MySQL en un host local mediante sockets. Para ello, configure `host` y `socket`.

Ejemplo de configuración:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(MYSQL(
        host 'localhost'
        socket '/path/to/socket/file.sock'
        user 'clickhouse'
        password 'qwerty'
        db 'db_name'
        table 'table_name'
        where 'id=10'
        invalidate_query 'SQL_QUERY'
        fail_on_connection_loss 'true'
        query 'SELECT id, value_1, value_2 FROM db_name.table_name'
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="Archivo de configuración">
    ```xml
    <source>
      <mysql>
          <host>localhost</host>
          <socket>/path/to/socket/file.sock</socket>
          <user>clickhouse</user>
          <password>qwerty</password>
          <db>db_name</db>
          <table>table_name</table>
          <where>id=10</where>
          <invalidate_query>SQL_QUERY</invalidate_query>
          <fail_on_connection_loss>true</fail_on_connection_loss>
          <query>SELECT id, value_1, value_2 FROM db_name.table_name</query>
      </mysql>
    </source>
    ```
  </TabItem>
</Tabs>