---
slug: /sql-reference/statements/create/dictionary/layouts/ssd-cache
title: 'tipos de diseño del diccionario ssd_cache'
sidebar_label: 'ssd_cache'
sidebar_position: 8
description: 'Almacena los datos del diccionario en SSD con un índice en memoria: tipos ssd_cache o complex_key_ssd_cache'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="ssd_cache">
  ## ssd_cache
</div>

Es similar a `cache`, pero almacena los datos en SSD y el índice en RAM. Todos los ajustes de los diccionarios `cache` relacionados con la cola de actualización también pueden aplicarse a los diccionarios `ssd_cache`.

La clave del diccionario es de tipo [UInt64](/es/sql-reference/data-types/int-uint.md).

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 16777216 READ_BUFFER_SIZE 1048576
        PATH '/var/lib/clickhouse/user_files/test_dict'))
    ```
  </TabItem>

  <TabItem value="xml" label="Archivo de configuración">
    ```xml
    <layout>
        <ssd_cache>
            <!-- Tamaño del bloque de lectura elemental en bytes. Se recomienda que sea igual al tamaño de página del SSD. -->
            <block_size>4096</block_size>
            <!-- Tamaño máximo del archivo de caché en bytes. -->
            <file_size>16777216</file_size>
            <!-- Tamaño del búfer en RAM, en bytes, para leer elementos desde SSD. -->
            <read_buffer_size>131072</read_buffer_size>
            <!-- Tamaño del búfer en RAM, en bytes, para acumular elementos antes de volcarlos al SSD. -->
            <write_buffer_size>1048576</write_buffer_size>
            <!-- Ruta donde se almacenará el archivo de caché. -->
            <path>/var/lib/clickhouse/user_files/test_dict</path>
        </ssd_cache>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

<div id="complex_key_ssd_cache">
  ## complex_key_ssd_cache
</div>

Este tipo de almacenamiento está pensado para usarse con [claves compuestas](../attributes.md#composite-key). Es similar a `ssd_cache`.