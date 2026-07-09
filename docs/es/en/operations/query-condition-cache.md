---
description: 'Guía para usar y configurar la función de caché de condiciones de consulta en ClickHouse'
sidebar_label: 'Caché de condiciones de consulta'
sidebar_position: 64
slug: /operations/query-condition-cache
title: 'Caché de condiciones de consulta'
doc_type: 'guide'
---

:::note
La caché de condiciones de consulta solo funciona cuando [enable&#95;analyzer](https://clickhouse.com/docs/operations/settings/settings#enable_analyzer) está configurado como true, que es el valor predeterminado.
:::

Muchas cargas de trabajo reales implican consultas repetidas sobre los mismos datos o sobre datos casi idénticos (por ejemplo, datos ya existentes más datos nuevos).
ClickHouse ofrece diversas técnicas para optimizar este tipo de patrones de consulta.
Una posibilidad es ajustar la estructura física de los datos mediante estructuras de índice (p. ej., índices de clave primaria, índices de omisión, proyecciones) o mediante precálculo (vistas materializadas).
Otra posibilidad es usar la [query cache](query-cache.md) de ClickHouse para evitar la evaluación repetida de consultas.
La desventaja del primer enfoque es que requiere intervención manual y supervisión por parte de un administrador de bases de datos.
El segundo enfoque puede devolver resultados obsoletos (ya que la query cache no es coherente a nivel transaccional), algo que puede ser aceptable o no según el caso de uso.

La caché de condiciones de consulta ofrece una solución elegante para ambos problemas.
Se basa en la idea de que evaluar una condición de filtro (p. ej., `WHERE col = 'xyz'`) sobre los mismos datos siempre devolverá los mismos resultados.
Más concretamente, la caché de condiciones de consulta recuerda, para cada filtro evaluado y cada gránulo (= un bloque de 8192 filas de forma predeterminada), si ninguna fila del gránulo cumple la condición de filtro.
La información se registra como un único bit: un bit con valor 0 representa que ninguna fila coincide con el filtro, mientras que un bit con valor 1 significa que existe al menos una fila coincidente.
En el primer caso, ClickHouse puede omitir el gránulo correspondiente durante la evaluación del filtro; en el segundo, el gránulo debe cargarse y evaluarse.

La caché de condiciones de consulta es eficaz si se cumplen tres requisitos previos:

* Primero, la carga de trabajo debe evaluar repetidamente las mismas condiciones de filtro. Esto ocurre de forma natural si una consulta se repite varias veces, pero también puede suceder si dos consultas comparten los mismos filtros, p. ej. `SELECT product FROM products WHERE quality > 3` y `SELECT vendor, count() FROM products WHERE quality > 3`.
* Segundo, la mayor parte de los datos debe ser inmutable, es decir, no cambiar entre consultas. Este suele ser el caso en ClickHouse, ya que las partes son inmutables y solo se crean mediante INSERTs.
* Tercero, los filtros deben ser selectivos, es decir, solo un número relativamente pequeño de filas debe satisfacer la condición de filtro. Cuantas menos filas coincidan con la condición de filtro, más gránulos se registrarán con el bit 0 (sin filas coincidentes) y más datos podrán descartarse en evaluaciones de filtro posteriores.

<div id="memory-consumption">
  ## Consumo de memoria
</div>

Dado que la caché de condiciones de consulta almacena solo un único bit por condición de filtro y gránulo, apenas consume memoria.
El tamaño máximo de la caché de condiciones de consulta se puede configurar mediante la opción de configuración del servidor [`query_condition_cache_size`](server-configuration-parameters/settings.md#query_condition_cache_size) (valor predeterminado: 100 MB).
Un tamaño de caché de 100 MB corresponde a 100 * 1024 * 1024 * 8 = 838,860,800 entradas.
Como cada entrada representa una marca (8192 filas de forma predeterminada), la caché puede abarcar hasta 6,871,947,673,600 (6,8 billones) filas de una sola columna.
En la práctica, los filtros se evalúan sobre más de una columna, por lo que ese número debe dividirse entre la cantidad de columnas filtradas.

<div id="configuration-settings-and-usage">
  ## Configuración y uso
</div>

El ajuste [use&#95;query&#95;condition&#95;cache](settings/settings#use_query_condition_cache) controla si una consulta específica o todas las consultas de la sesión actual deben usar la caché de condiciones de consulta.

Por ejemplo, la primera ejecución de la consulta

```sql
SELECT col1, col2
FROM table
WHERE col1 = 'x'
SETTINGS use_query_condition_cache = true;
```

almacenará los rangos de la tabla que no satisfacen el predicado.
Las ejecuciones posteriores de la misma consulta, también con el parámetro `use_query_condition_cache = true`, utilizarán la caché de condiciones de consulta para escanear menos datos.

<div id="administration">
  ## Administración
</div>

La caché de condiciones de consulta no se conserva entre reinicios de ClickHouse.

Para borrar la caché de condiciones de consulta, ejecute [`SYSTEM CLEAR QUERY CONDITION CACHE`](../sql-reference/statements/system.md#drop-query-condition-cache).

El contenido de la caché se muestra en la tabla del sistema [system.query&#95;condition&#95;cache](system-tables/query_condition_cache.md).
Para calcular el tamaño actual de la caché de condiciones de consulta en MB, ejecute `SELECT formatReadableSize(sum(entry_size)) FROM system.query_condition_cache`.
Si desea investigar condiciones de filtro concretas, puede consultar el campo `condition` en `system.query_condition_cache`. Tenga en cuenta que este campo solo está disponible en compilaciones de depuración.

El número de aciertos y fallos de la caché de condiciones de consulta desde el arranque de la base de datos se muestra como los eventos &quot;QueryConditionCacheHits&quot; y &quot;QueryConditionCacheMisses&quot; en la tabla del sistema [system.events](system-tables/events.md).
Ambos contadores solo se actualizan para consultas `SELECT` que se ejecutan con la configuración `use_query_condition_cache = true`; las demás consultas no afectan a &quot;QueryCacheMisses&quot;.

<div id="related-content">
  ## Contenido relacionado
</div>

* Blog: [Introducing the Query Condition Cache](https://clickhouse.com/blog/introducing-the-clickhouse-query-condition-cache)
* [Predicate Caching: Query-Driven Secondary Indexing for Cloud Data Warehouses (Schmidt et. al., 2024)](https://doi.org/10.1145/3626246.3653395)