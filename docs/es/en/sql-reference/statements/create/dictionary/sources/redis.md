---
slug: /sql-reference/statements/create/dictionary/sources/redis
title: 'Origen del diccionario Redis'
sidebar_position: 10
sidebar_label: 'Redis'
description: 'Configure Redis como origen de un diccionario en ClickHouse.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Ejemplo de configuración:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(REDIS(
        host 'localhost'
        port 6379
        storage_type 'simple'
        db_index 0
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="Archivo de configuración">
    ```xml
    <source>
        <redis>
            <host>localhost</host>
            <port>6379</port>
            <storage_type>simple</storage_type>
            <db_index>0</db_index>
        </redis>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

Campos de configuración:

| Configuración  | Descripción                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| -------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `host`         | El host de Redis.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `port`         | El puerto del servidor Redis.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `storage_type` | La estructura del almacenamiento interno de Redis que se utiliza para trabajar con claves. `simple` usa un mapa plano de claves y valores y admite disposiciones de clave simple, así como disposiciones de clave compleja de una sola columna (como `complex_key_cache` y `complex_key_direct`). `hash_map` usa un hash de Redis y es necesario para claves complejas compuestas; espera exactamente dos columnas de clave. Las columnas de clave deben ser de tipo entero o de cadena. Las disposiciones por rango no son compatibles. El valor predeterminado es `simple`. Opcional. |
| `db_index`     | El índice numérico específico de la base de datos lógica de Redis. El valor predeterminado es `0`. Opcional.                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |