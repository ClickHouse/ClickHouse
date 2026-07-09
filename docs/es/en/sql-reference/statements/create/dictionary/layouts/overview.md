---
description: 'Tipos de layouts de diccionario para almacenar diccionarios en memoria'
sidebar_label: 'Descripción general'
sidebar_position: 1
slug: /sql-reference/statements/create/dictionary/layouts
title: 'Layouts de diccionario'
doc_type: 'referencia'
---

import CloudDetails from '@site/docs/sql-reference/statements/create/dictionary/_snippet_dictionary_in_cloud.md';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="storing-dictionaries-in-memory">
  ## Tipos de layout de diccionarios
</div>

Hay varias formas de almacenar diccionarios en memoria, cada una con sus propias compensaciones entre uso de CPU y RAM.

| Layout                                                                                                     | Descripción                                                                                                                                                  |
| ---------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| [flat](./flat.md)                                                                                          | Almacena los datos en arrays planos indexados por clave. Es el layout más rápido, pero las claves deben ser `UInt64` y estar limitadas por `max_array_size`. |
| [hashed](./hashed.md)                                                                                      | Almacena los datos en una tabla hash. No tiene límite de tamaño de clave y admite cualquier cantidad de elementos.                                           |
| [sparse&#95;hashed](./hashed.md#sparse_hashed)                                                             | Como `hashed`, pero intercambia CPU por un menor uso de memoria.                                                                                             |
| [complex&#95;key&#95;hashed](./hashed.md#complex_key_hashed)                                               | Como `hashed`, para claves compuestas.                                                                                                                       |
| [complex&#95;key&#95;sparse&#95;hashed](./hashed.md#complex_key_sparse_hashed)                             | Como `sparse_hashed`, para claves compuestas.                                                                                                                |
| [hashed&#95;array](./hashed-array.md)                                                                      | Atributos almacenados en arrays con una tabla hash que asigna claves a índices de array. Eficiente en memoria cuando hay muchos atributos.                   |
| [complex&#95;key&#95;hashed&#95;array](./hashed-array.md#complex_key_hashed_array)                         | Como `hashed_array`, para claves compuestas.                                                                                                                 |
| [range&#95;hashed](./range-hashed.md)                                                                      | Tabla hash con rangos ordenados. Admite búsquedas por clave + rango de fecha/hora.                                                                           |
| [complex&#95;key&#95;range&#95;hashed](./range-hashed.md#complex_key_range_hashed)                         | Como `range_hashed`, para claves compuestas.                                                                                                                 |
| [cache](./cache.md)                                                                                        | Caché en memoria de tamaño fijo. Solo se almacenan las claves a las que se accede con frecuencia.                                                            |
| [complex&#95;key&#95;cache](/es/sql-reference/statements/create/dictionary/layouts/hashed#complex_key_hashed) | Como `cache`, para claves compuestas.                                                                                                                        |
| [ssd&#95;cache](./ssd-cache.md)                                                                            | Como `cache`, pero almacena los datos en SSD con un índice en memoria.                                                                                       |
| [complex&#95;key&#95;ssd&#95;cache](./ssd-cache.md#complex_key_ssd_cache)                                  | Como `ssd_cache`, para claves compuestas.                                                                                                                    |
| [direct](./direct.md)                                                                                      | Sin almacenamiento en memoria: consulta el origen directamente en cada solicitud.                                                                            |
| [complex&#95;key&#95;direct](./direct.md#complex_key_direct)                                               | Como `direct`, para claves compuestas.                                                                                                                       |
| [ip&#95;trie](./ip-trie.md)                                                                                | Estructura trie para búsquedas rápidas de prefijos IP (basadas en CIDR).                                                                                     |

:::tip Layouts recomendados
[flat](./flat.md), [hashed](./hashed.md) y [complex&#95;key&#95;hashed](./hashed.md#complex_key_hashed) ofrecen el mejor rendimiento de las consultas.
Los layouts con caché no se recomiendan debido a su rendimiento potencialmente deficiente y a la dificultad de ajustar sus parámetros; consulta [cache](./cache.md) para obtener más información.
:::

<div id="specify-dictionary-layout">
  ## Especificar el layout del diccionario
</div>

<CloudDetails />

Puede configurar el layout de un diccionario con la cláusula `LAYOUT` (para DDL) o con la opción `layout` en las definiciones del archivo de configuración.

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY (...)
    ...
    LAYOUT(LAYOUT_TYPE(param value)) -- opciones de layout
    ...
    ```
  </TabItem>

  <TabItem value="xml" label="Archivo de configuración">
    ```xml
    <clickhouse>
        <dictionary>
            ...
            <layout>
                <layout_type>
                    <!-- opciones de layout -->
                </layout_type>
            </layout>
            ...
        </dictionary>
    </clickhouse>
    ```
  </TabItem>
</Tabs>

<br />

Consulte también [CREATE DICTIONARY](../overview.md) para ver la sintaxis DDL completa.

Los diccionarios cuyo layout no contiene la palabra `complex-key*` tienen una clave de tipo [UInt64](/es/sql-reference/data-types/int-uint.md); los diccionarios `complex-key*` tienen una clave compuesta (compleja, con tipos arbitrarios).

**Ejemplo de clave numérica** (la columna key&#95;column es de tipo [UInt64](/es/sql-reference/data-types/int-uint.md)):

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY dict_name (
        key_column UInt64,
        ...
    )
    PRIMARY KEY key_column
    ```
  </TabItem>

  <TabItem value="xml" label="Archivo de configuración">
    ```xml
    <structure>
        <id>
            <name>key_column</name>
        </id>
        ...
    </structure>
    ```
  </TabItem>
</Tabs>

<br />

**Ejemplo de clave compuesta** (la clave tiene un elemento de tipo [String](/es/sql-reference/data-types/string.md)):

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY dict_name (
        country_code String,
        ...
    )
    PRIMARY KEY country_code
    ```
  </TabItem>

  <TabItem value="xml" label="Archivo de configuración">
    ```xml
    <structure>
        <key>
            <attribute>
                <name>country_code</name>
                <type>String</type>
            </attribute>
        </key>
        ...
    </structure>
    ```
  </TabItem>
</Tabs>

<div id="improve-performance">
  ## Mejora el rendimiento de los diccionarios
</div>

Hay varias formas de mejorar el rendimiento de los diccionarios:

* Llame a la función que trabaja con el diccionario después de `GROUP BY`.
* Marque como inyectivos los atributos que se van a extraer.
  Un atributo se considera inyectivo si a claves distintas les corresponden valores de atributo distintos.
  Por lo tanto, cuando `GROUP BY` usa una función que obtiene un valor de atributo a partir de la clave, esta función se extrae automáticamente de `GROUP BY`.

ClickHouse genera una excepción cuando se producen errores con los diccionarios.
Algunos ejemplos de errores son:

* No se pudo cargar el diccionario al que se intenta acceder.
* Error al consultar un diccionario `cached`.

Puede ver la lista de diccionarios y sus estados en la tabla [system.dictionaries](/es/operations/system-tables/dictionaries.md).