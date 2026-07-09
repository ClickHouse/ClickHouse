---
slug: /sql-reference/statements/create/dictionary/layouts/flat
title: 'layout flat de diccionario'
sidebar_label: 'flat'
sidebar_position: 2
description: 'Almacena un diccionario en memoria como arrays planos.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Con la disposición `flat`, el diccionario se almacena completamente en memoria en forma de arrays planos.
La cantidad de memoria utilizada es proporcional al tamaño de la clave más grande (en términos del espacio ocupado).

:::tip
Este tipo de disposición ofrece el mejor rendimiento entre todos los métodos disponibles para almacenar un diccionario.
:::

La clave del diccionario tiene el tipo [UInt64](/es/sql-reference/data-types/int-uint.md) y su valor está limitado por `max_array_size` (de forma predeterminada: 500,000).
Si al crear el diccionario se encuentra una clave mayor, ClickHouse lanza una excepción y no crea el diccionario.
El tamaño inicial de los arrays planos del diccionario se controla mediante la configuración `initial_array_size` (de forma predeterminada: 1024).

Se admiten todos los tipos de orígenes.
Al actualizar el diccionario, los datos (de un archivo o de una tabla) se leen en su totalidad.

Ejemplo de configuración:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(FLAT(INITIAL_ARRAY_SIZE 50000 MAX_ARRAY_SIZE 5000000))
    ```
  </TabItem>

  <TabItem value="xml" label="Archivo de configuración">
    ```xml
    <layout>
      <flat>
        <initial_array_size>50000</initial_array_size>
        <max_array_size>5000000</max_array_size>
      </flat>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />