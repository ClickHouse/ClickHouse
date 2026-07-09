---
slug: /sql-reference/statements/create/dictionary/layouts/hashed-array
title: 'tipos de diseño de diccionario hashed_array'
sidebar_label: 'hashed_array'
sidebar_position: 4
description: 'Almacena un diccionario en memoria mediante una tabla hash con arrays de atributos.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="hashed_array">
  ## hashed_array
</div>

El diccionario se almacena completamente en memoria. Cada atributo se almacena en un array. El atributo clave se almacena en forma de una tabla hash, donde el valor es un índice en el array de atributos. El diccionario puede contener cualquier número de elementos con todo tipo de identificadores. En la práctica, el número de claves puede alcanzar decenas de millones de elementos.

La clave del diccionario tiene el tipo [UInt64](/es/sql-reference/data-types/int-uint.md).

Se admiten todo tipo de fuentes. Al actualizar, los datos (desde un archivo o desde una tabla) se leen en su totalidad.

Ejemplo de configuración:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(HASHED_ARRAY([SHARDS 1]))
    ```
  </TabItem>

  <TabItem value="xml" label="Archivo de configuración">
    ```xml
    <layout>
      <hashed_array>
      </hashed_array>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

<div id="complex_key_hashed_array">
  ## complex_key_hashed_array
</div>

Este tipo de almacenamiento se utiliza con [claves](../attributes.md#composite-key) compuestas. Es similar a [hashed&#95;array](#hashed_array).

Ejemplo de configuración:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(COMPLEX_KEY_HASHED_ARRAY([SHARDS 1]))
    ```
  </TabItem>

  <TabItem value="xml" label="Archivo de configuración">
    ```xml
    <layout>
      <complex_key_hashed_array />
    </layout>
    ```
  </TabItem>
</Tabs>

<br />