---
slug: /sql-reference/statements/create/dictionary/sources/clickhouse
title: 'Fuente de diccionario de ClickHouse'
sidebar_position: 8
sidebar_label: 'ClickHouse'
description: 'Configurar una tabla de ClickHouse como fuente de diccionario.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Ejemplo de configuración:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(CLICKHOUSE(
        host 'example01-01-1'
        port 9000
        user 'default'
        password ''
        db 'default'
        table 'ids'
        where 'id=10'
        secure 1
        query 'SELECT id, value_1, value_2 FROM default.ids'
    ));
    ```
  </TabItem>

  <TabItem value="xml" label="Archivo de configuración">
    ```xml
    <source>
        <clickhouse>
            <host>example01-01-1</host>
            <port>9000</port>
            <user>default</user>
            <password></password>
            <db>default</db>
            <table>ids</table>
            <where>id=10</where>
            <secure>1</secure>
            <query>SELECT id, value_1, value_2 FROM default.ids</query>
        </clickhouse>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

Campos de configuración:

| Configuración      | Descripción                                                                                                                                                                                                                                                             |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `host`             | El host de ClickHouse. Si se trata de un host local, la consulta se procesa sin ninguna actividad de red. Para mejorar la tolerancia a fallos, puede crear una tabla [Distributed](/es/engines/table-engines/special/distributed) y usarla en configuraciones posteriores. |
| `port`             | El puerto del servidor de ClickHouse.                                                                                                                                                                                                                                   |
| `user`             | Nombre del usuario de ClickHouse.                                                                                                                                                                                                                                       |
| `password`         | Contraseña del usuario de ClickHouse.                                                                                                                                                                                                                                   |
| `db`               | Nombre de la base de datos.                                                                                                                                                                                                                                             |
| `table`            | Nombre de la tabla.                                                                                                                                                                                                                                                     |
| `where`            | Criterio de selección. Opcional.                                                                                                                                                                                                                                        |
| `invalidate_query` | Consulta para comprobar el estado del diccionario. Opcional. Consulte más información en la sección [Refreshing dictionary data using LIFETIME](../lifetime.md).                                                                                                        |
| `secure`           | Utilice SSL para la conexión.                                                                                                                                                                                                                                           |
| `query`            | Consulta personalizada. Opcional.                                                                                                                                                                                                                                       |

:::note
Los campos `table` o `where` no pueden usarse junto con el campo `query`. Además, debe declararse uno de los campos `table` o `query`.
:::