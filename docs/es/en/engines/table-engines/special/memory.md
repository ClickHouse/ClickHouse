---
description: 'El motor Memory almacena datos en RAM, sin comprimir. Los datos se
  almacenan exactamente en la misma forma en que se reciben. En otras palabras, leer
  de esta tabla es completamente gratuito.'
sidebar_label: 'Memory'
sidebar_position: 110
slug: /engines/table-engines/special/memory
title: 'Motor de tabla Memory'
doc_type: 'reference'
---

:::note
Al usar el motor de tabla Memory en ClickHouse Cloud, los datos no se replican en todos los nodos (por diseño). Para garantizar que todas las consultas se enruten al mismo nodo y que el motor de tabla Memory funcione como se espera, puede hacer una de las siguientes cosas:

* Ejecutar todas las operaciones en la misma sesión
* Usar un client que utilice TCP o la interfaz nativa (lo que habilita la compatibilidad con conexiones persistentes), como [clickhouse-client](/es/interfaces/client)
  :::

El motor Memory almacena datos en RAM, sin comprimir. Los datos se almacenan exactamente en la misma forma en que se reciben. En otras palabras, leer de esta tabla es completamente gratuito.
El acceso concurrente a los datos está sincronizado. Los bloqueos son breves: las operaciones de lectura y escritura no se bloquean entre sí.
No admite índices. La lectura se paraleliza.

La productividad máxima (más de 10 GB/s) se alcanza en consultas simples, ya que no hay lectura desde disco, descompresión ni deserialización de datos. (Cabe señalar que, en muchos casos, la productividad del motor MergeTree es casi igual de alta).
Al reiniciar un server, los datos desaparecen de la tabla y esta queda vacía.
Normalmente, el uso de este motor de tabla no está justificado. Sin embargo, puede usarse para pruebas y para tareas en las que se requiere la máxima velocidad con un número relativamente pequeño de filas (hasta aproximadamente 100.000.000).

El sistema usa el motor Memory para las tablas temporales con datos externos de consultas (consulta la sección &quot;Datos externos para procesar una consulta&quot;) y para implementar `GLOBAL IN` (consulta la sección &quot;Operadores IN&quot;).

Se pueden especificar límites superior e inferior para limitar el tamaño de una tabla del motor Memory, lo que en la práctica permite que actúe como un búfer circular (consulta [Parámetros del motor](#engine-parameters)).

<div id="engine-parameters">
  ## Parámetros del motor
</div>

* `min_bytes_to_keep` — Cantidad mínima de bytes que se deben conservar cuando la tabla Memory tiene un límite de tamaño.
  * Valor predeterminado: `0`
  * Requiere `max_bytes_to_keep`
* `max_bytes_to_keep` — Cantidad máxima de bytes que se deben conservar en la tabla Memory; en cada inserción se eliminan las filas más antiguas (es decir, actúa como un búfer circular). Este máximo puede superar el límite indicado si, al añadir un bloque grande, el lote más antiguo de filas que habría que eliminar queda por debajo del límite de `min_bytes_to_keep`.
  * Valor predeterminado: `0`
* `min_rows_to_keep` — Cantidad mínima de filas que se deben conservar cuando la tabla Memory tiene un límite de tamaño.
  * Valor predeterminado: `0`
  * Requiere `max_rows_to_keep`
* `max_rows_to_keep` — Cantidad máxima de filas que se deben conservar en la tabla Memory; en cada inserción se eliminan las filas más antiguas (es decir, actúa como un búfer circular). Este máximo puede superar el límite indicado si, al añadir un bloque grande, el lote más antiguo de filas que habría que eliminar queda por debajo del límite de `min_rows_to_keep`.
  * Valor predeterminado: `0`
* `compress` - Si los datos en memoria deben comprimirse.
  * Valor predeterminado: `false`

<div id="usage">
  ## Uso
</div>

**Inicializar la configuración**

```sql
CREATE TABLE memory (i UInt32) ENGINE = Memory SETTINGS min_rows_to_keep = 100, max_rows_to_keep = 1000;
```

**Modificar la configuración**

```sql
ALTER TABLE memory MODIFY SETTING min_rows_to_keep = 100, max_rows_to_keep = 1000;
```

**Nota:** Los parámetros de límite `bytes` y `rows` pueden establecerse al mismo tiempo; sin embargo, se aplicarán los límites inferiores de `max` y `min`.

<div id="examples">
  ## Ejemplos
</div>

```sql
CREATE TABLE memory (i UInt32) ENGINE = Memory SETTINGS min_bytes_to_keep = 4096, max_bytes_to_keep = 16384;

/* 1. testing oldest block doesn't get deleted due to min-threshold - 3000 rows */
INSERT INTO memory SELECT * FROM numbers(0, 1600); -- 8'192 bytes

/* 2. adding block that doesn't get deleted */
INSERT INTO memory SELECT * FROM numbers(1000, 100); -- 1'024 bytes

/* 3. testing oldest block gets deleted - 9216 bytes - 1100 */
INSERT INTO memory SELECT * FROM numbers(9000, 1000); -- 8'192 bytes

/* 4. checking a very large block overrides all */
INSERT INTO memory SELECT * FROM numbers(9000, 10000); -- 65'536 bytes

SELECT total_bytes, total_rows FROM system.tables WHERE name = 'memory' AND database = currentDatabase();
```

```text
┌─total_bytes─┬─total_rows─┐
│       65536 │      10000 │
└─────────────┴────────────┘
```

además, en el caso de las filas:

```sql
CREATE TABLE memory (i UInt32) ENGINE = Memory SETTINGS min_rows_to_keep = 4000, max_rows_to_keep = 10000;

/* 1. testing oldest block doesn't get deleted due to min-threshold - 3000 rows */
INSERT INTO memory SELECT * FROM numbers(0, 1600); -- 1'600 rows

/* 2. adding block that doesn't get deleted */
INSERT INTO memory SELECT * FROM numbers(1000, 100); -- 100 rows

/* 3. testing oldest block gets deleted - 9216 bytes - 1100 */
INSERT INTO memory SELECT * FROM numbers(9000, 1000); -- 1'000 rows

/* 4. checking a very large block overrides all */
INSERT INTO memory SELECT * FROM numbers(9000, 10000); -- 10'000 rows

SELECT total_bytes, total_rows FROM system.tables WHERE name = 'memory' AND database = currentDatabase();
```

```text
┌─total_bytes─┬─total_rows─┐
│       65536 │      10000 │
└─────────────┴────────────┘
```