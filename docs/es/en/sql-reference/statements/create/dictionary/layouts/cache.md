---
slug: /sql-reference/statements/create/dictionary/layouts/cache
title: 'layout de diccionario cache'
sidebar_label: 'cache'
sidebar_position: 6
description: 'Almacena un diccionario en una caché en memoria de tamaño fijo.'
doc_type: 'referencia'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

El tipo de layout de diccionario `cached` almacena el diccionario en una caché con un número fijo de celdas.
Estas celdas contienen elementos de uso frecuente.

La clave del diccionario es de tipo [UInt64](/es/sql-reference/data-types/int-uint.md).

Al buscar en un diccionario, primero se consulta la caché. Para cada bloque de datos, todas las claves que no se encuentran en la caché o que están desactualizadas se solicitan al origen mediante `SELECT attrs... FROM db.table WHERE id IN (k1, k2, ...)`. Después, los datos recibidos se escriben en la caché.

Si no se encuentran claves en el diccionario, se crea una tarea de actualización de la caché y se añade a la cola de actualización. Las propiedades de la cola de actualización se pueden controlar con los settings `max_update_queue_size`, `update_queue_push_timeout_milliseconds`, `query_wait_timeout_milliseconds`, `max_threads_for_updates`.

En los diccionarios de caché, se puede establecer el [tiempo de vida](../lifetime.md) de los datos almacenados en la caché. Si ha transcurrido más tiempo que `lifetime` desde que se cargaron los datos en una celda, el valor de la celda deja de usarse y la clave pasa a estar expirada. La clave se volverá a solicitar la próxima vez que sea necesaria. Este comportamiento se puede configurar con el setting `allow_read_expired_keys`.

Esta es la menos eficaz de todas las formas de almacenar diccionarios. La velocidad de la caché depende en gran medida de una configuración correcta y del escenario de uso. Un diccionario de tipo caché solo funciona bien cuando la tasa de aciertos es lo bastante alta (se recomienda un 99% o más). Puede ver la tasa media de aciertos en la tabla [system.dictionaries](/es/operations/system-tables/dictionaries.md).

Si el setting `allow_read_expired_keys` se establece en 1 (su valor predeterminado es 0), el diccionario puede admitir actualizaciones asíncronas. Si un client solicita claves y todas están en la caché, pero algunas han expirado, el diccionario devolverá al client las claves expiradas y las solicitará de forma asíncrona al origen.

Para mejorar el rendimiento de la caché, use una subconsulta con `LIMIT` y llame a la función con el diccionario de forma externa.

Se admiten todos los tipos de orígenes.

Ejemplo de settings:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(CACHE(SIZE_IN_CELLS 1000000000))
    ```
  </TabItem>

  <TabItem value="xml" label="Archivo de configuración">
    ```xml
    <layout>
        <cache>
            <!-- El tamaño de la caché, en número de celdas. Se redondea hacia arriba a una potencia de dos. -->
            <size_in_cells>1000000000</size_in_cells>
            <!-- Permite leer claves expiradas. -->
            <allow_read_expired_keys>0</allow_read_expired_keys>
            <!-- Tamaño máximo de la cola de actualización. -->
            <max_update_queue_size>100000</max_update_queue_size>
            <!-- Tiempo de espera máximo en milisegundos para insertar la tarea de actualización en la cola. -->
            <update_queue_push_timeout_milliseconds>10</update_queue_push_timeout_milliseconds>
            <!-- Tiempo de espera máximo en milisegundos para que se complete la tarea de actualización. -->
            <query_wait_timeout_milliseconds>60000</query_wait_timeout_milliseconds>
            <!-- Número máximo de hilos para actualizar el diccionario de caché. -->
            <max_threads_for_updates>4</max_threads_for_updates>
        </cache>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

Configure un tamaño de caché lo bastante grande. Deberá hacer pruebas para seleccionar el número de celdas:

1. Establezca un valor.
2. Ejecute consultas hasta que la caché esté completamente llena.
3. Evalúe el consumo de memoria con la tabla `system.dictionaries`.
4. Aumente o reduzca el número de celdas hasta alcanzar el consumo de memoria requerido.

:::note
No se recomienda usar ClickHouse como origen para este layout. Las búsquedas en diccionarios requieren lecturas puntuales aleatorias, que no coinciden con el patrón de acceso para el que ClickHouse está optimizado.
:::