---
slug: /sql-reference/statements/create/dictionary/layouts/hashed
title: 'tipos de disposición de diccionario hashed'
sidebar_label: 'hashed'
sidebar_position: 3
description: 'Almacena un diccionario en memoria mediante tablas hash: hashed, sparse_hashed, complex_key_hashed, complex_key_sparse_hashed'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="hashed">
  ## hashed
</div>

El diccionario se almacena por completo en memoria en forma de tabla hash. Puede contener cualquier cantidad de elementos con cualquier identificador. En la práctica, el número de claves puede llegar a decenas de millones de elementos.

La clave del diccionario es de tipo [UInt64](/es/sql-reference/data-types/int-uint.md).

Se admiten todo tipo de fuentes. Al actualizar, los datos (desde un archivo o desde una tabla) se leen por completo.

Ejemplo de configuración:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(HASHED())
    ```
  </TabItem>

  <TabItem value="xml" label="Archivo de configuración">
    ```xml
    <layout>
      <hashed />
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

Ejemplo de configuración con ajustes:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
    ```
  </TabItem>

  <TabItem value="xml" label="Archivo de configuración">
    ```xml
    <layout>
      <hashed>
        <!-- Si shards es mayor que 1 (el valor predeterminado es `1`), el diccionario cargará
             los datos en paralelo, lo que resulta útil si tienes una gran cantidad de elementos en un
             mismo diccionario. -->
        <shards>10</shards>

        <!-- Tamaño del backlog de la cola para bloques en paralelo.

             Dado que el cuello de botella en la carga en paralelo es el rehash, y para evitar
             que el proceso se detenga porque un hilo esté realizando el rehash, es necesario
             disponer de cierto backlog.

             10000 ofrece un buen equilibrio entre memoria y velocidad.
             Incluso con 10e10 elementos, puede manejar toda la carga sin inanición. -->
        <shard_load_queue_backlog>10000</shard_load_queue_backlog>

        <!-- Factor de carga máximo de la tabla hash; con valores más altos, la memoria
             se aprovecha mejor (se desperdicia menos), pero el rendimiento de lectura
             puede deteriorarse.

             Valores válidos: [0.5, 0.99]
             Predeterminado: 0.5 -->
        <max_load_factor>0.5</max_load_factor>
      </hashed>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

<div id="sparse_hashed">
  ## sparse_hashed
</div>

Similar a `hashed`, pero usa menos memoria a costa de un mayor uso de CPU.

La clave del diccionario es de tipo [UInt64](/es/sql-reference/data-types/int-uint.md).

Ejemplo de configuración:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(SPARSE_HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
    ```
  </TabItem>

  <TabItem value="xml" label="Archivo de configuración">
    ```xml
    <layout>
      <sparse_hashed>
        <!-- <shards>1</shards> -->
        <!-- <shard_load_queue_backlog>10000</shard_load_queue_backlog> -->
        <!-- <max_load_factor>0.5</max_load_factor> -->
      </sparse_hashed>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

También es posible usar `shards` con este tipo de diccionario y, de nuevo, es más importante para `sparse_hashed` que para `hashed`, ya que `sparse_hashed` es más lento.

<div id="complex_key_hashed">
  ## complex_key_hashed
</div>

Este tipo de almacenamiento se utiliza con [claves](../attributes.md#composite-key) compuestas. Similar a `hashed`.

Ejemplo de configuración:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(COMPLEX_KEY_HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
    ```
  </TabItem>

  <TabItem value="xml" label="Archivo de configuración">
    ```xml
    <layout>
      <complex_key_hashed>
        <!-- <shards>1</shards> -->
        <!-- <shard_load_queue_backlog>10000</shard_load_queue_backlog> -->
        <!-- <max_load_factor>0.5</max_load_factor> -->
      </complex_key_hashed>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

<div id="complex_key_sparse_hashed">
  ## complex_key_sparse_hashed
</div>

Este tipo de almacenamiento se utiliza con [claves](../attributes.md#composite-key) compuestas. Es similar a [sparse&#95;hashed](#sparse_hashed).

Ejemplo de configuración:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(COMPLEX_KEY_SPARSE_HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
    ```
  </TabItem>

  <TabItem value="xml" label="Archivo de configuración">
    ```xml
    <layout>
      <complex_key_sparse_hashed>
        <!-- <shards>1</shards> -->
        <!-- <shard_load_queue_backlog>10000</shard_load_queue_backlog> -->
        <!-- <max_load_factor>0.5</max_load_factor> -->
      </complex_key_sparse_hashed>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />