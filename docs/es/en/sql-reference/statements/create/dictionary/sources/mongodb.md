---
slug: /sql-reference/statements/create/dictionary/sources/mongodb
title: 'Fuente de diccionario de MongoDB'
sidebar_position: 9
sidebar_label: 'MongoDB'
description: 'Configure MongoDB como fuente de diccionario en ClickHouse.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Ejemplo de configuración:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(MONGODB(
        host 'localhost'
        port 27017
        user ''
        password ''
        db 'test'
        collection 'dictionary_source'
        options 'ssl=true'
    ))
    ```

    O bien, usando una URI:

    ```sql
    SOURCE(MONGODB(
        uri 'mongodb://localhost:27017/clickhouse'
        collection 'dictionary_source'
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="Archivo de configuración">
    ```xml
    <source>
        <mongodb>
            <host>localhost</host>
            <port>27017</port>
            <user></user>
            <password></password>
            <db>test</db>
            <collection>dictionary_source</collection>
            <options>ssl=true</options>
        </mongodb>
    </source>
    ```

    O bien, usando una URI:

    ```xml
    <source>
        <mongodb>
            <uri>mongodb://localhost:27017/test?ssl=true</uri>
            <collection>dictionary_source</collection>
        </mongodb>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

Campos de configuración:

| Ajuste       | Descripción                                                                           |
| ------------ | ------------------------------------------------------------------------------------- |
| `host`       | El host de MongoDB.                                                                   |
| `port`       | El puerto del servidor de MongoDB.                                                    |
| `user`       | Nombre del usuario de MongoDB.                                                        |
| `password`   | Contraseña del usuario de MongoDB.                                                    |
| `db`         | Nombre de la base de datos.                                                           |
| `collection` | Nombre de la colección.                                                               |
| `options`    | Opciones de la cadena de conexión de MongoDB. Opcional.                               |
| `uri`        | URI para establecer la conexión (alternativa a los campos individuales host/port/db). |

[Más información sobre el motor](/es/engines/table-engines/integrations/mongodb)