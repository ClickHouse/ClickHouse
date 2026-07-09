---
description: 'Tabla del sistema que muestra los índices hipotéticos (what-if) definidos en la sesión actual'
keywords: ['tabla del sistema', 'hypothetical_indexes', 'what-if']
sidebar_label: 'hypothetical_indexes'
sidebar_position: 81
slug: /operations/system-tables/hypothetical_indexes
title: 'system.hypothetical_indexes'
doc_type: 'referencia'
---

<div id="system-hypothetical-indexes">
  # system.hypothetical_indexes
</div>

Enumera cada índice de salto hipotético (what-if) definido en la sesión actual. Consulte [`CREATE HYPOTHETICAL INDEX`](/es/sql-reference/statements/hypothetical-index#create-hypothetical-index) y [`EXPLAIN WHATIF`](/es/sql-reference/statements/explain#explain-whatif).

El contenido se limita al ámbito de la sesión: cada conexión ve solo sus propios índices hipotéticos, y la tabla está vacía cuando no se ha creado ningún índice en la sesión actual.

Los valores actuales de `(database, table)` se resuelven mediante UUID en el momento de la consulta, por lo que reflejan `RENAME TABLE` y las entradas correspondientes a tablas eliminadas se ocultan automáticamente.

<div id="columns">
  ## Columnas
</div>

| Columna       | Tipo     | Descripción                                                                          |
| ------------- | -------- | ------------------------------------------------------------------------------------ |
| `database`    | `String` | Base de datos de destino.                                                            |
| `table`       | `String` | Tabla de destino.                                                                    |
| `name`        | `String` | Nombre del índice.                                                                   |
| `type`        | `String` | Tipo de índice (`minmax`, `set`, `bloom_filter`, etc.).                              |
| `type_full`   | `String` | Expresión del tipo de índice con argumentos incluidos, p. ej., `bloom_filter(0.01)`. |
| `expression`  | `String` | Expresión del índice tal como se escribe en `CREATE HYPOTHETICAL INDEX`.             |
| `granularity` | `UInt64` | Número de gránulos de datos por gránulo de índice.                                   |

<div id="example">
  ## Ejemplo
</div>

```sql
CREATE HYPOTHETICAL INDEX i1 ON t (b) TYPE bloom_filter(0.01)  GRANULARITY 1;
CREATE HYPOTHETICAL INDEX i2 ON t (b) TYPE bloom_filter(0.001) GRANULARITY 1;

SELECT database, table, name, type, type_full, expression, granularity
FROM system.hypothetical_indexes;
```

```text
┌─database─┬─table─┬─name─┬─type─────────┬─type_full───────────┬─expression─┬─granularity─┐
│ default  │ t     │ i1   │ bloom_filter │ bloom_filter(0.01)  │ b          │           1 │
│ default  │ t     │ i2   │ bloom_filter │ bloom_filter(0.001) │ b          │           1 │
└──────────┴───────┴──────┴──────────────┴─────────────────────┴────────────┴─────────────┘
```

`type` es el nombre del tipo base y `type_full` incluye los parámetros, por lo que los usuarios pueden distinguir entre variantes parametrizadas como `bloom_filter(0.01)` y `bloom_filter(0.001)`.

<div id="see-also">
  ## Ver también
</div>

* [`CREATE HYPOTHETICAL INDEX`](/es/sql-reference/statements/hypothetical-index#create-hypothetical-index)
* [`EXPLAIN WHATIF`](/es/sql-reference/statements/explain#explain-whatif)