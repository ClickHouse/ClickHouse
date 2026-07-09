---
description: 'El motor de tabla GenerateRandom produce datos aleatorios para el
  esquema de tabla especificado.'
sidebar_label: 'GenerateRandom'
sidebar_position: 140
slug: /engines/table-engines/special/generate
title: 'Motor de tabla GenerateRandom'
doc_type: 'referencia'
---

El motor de tabla GenerateRandom produce datos aleatorios para el esquema de tabla especificado.

Ejemplos de uso:

* Úselo en pruebas para poblar de forma reproducible una tabla grande.
* Genere datos de entrada aleatorios para pruebas de fuzzing.

<div id="usage-in-clickhouse-server">
  ## Uso en ClickHouse Server
</div>

```sql
ENGINE = GenerateRandom([random_seed [,max_string_length [,max_array_length]]])
```

Los parámetros `max_array_length` y `max_string_length` especifican la longitud máxima, respectivamente, de todas las
columnas de tipo array o map y de las cadenas en los datos generados.

El motor de tabla Generate solo admite consultas `SELECT`.

Admite todos los [tipos de datos](../../../sql-reference/data-types/index.md) que pueden almacenarse en una tabla, excepto `AggregateFunction`.

<div id="example">
  ## Ejemplo
</div>

**1.** Configure la tabla `generate_engine_table`:

```sql
CREATE TABLE generate_engine_table (name String, value UInt32) ENGINE = GenerateRandom(1, 5, 3)
```

**2.** Consulta los datos:

```sql
SELECT * FROM generate_engine_table LIMIT 3
```

```text
┌─name─┬──────value─┐
│ c4xJ │ 1412771199 │
│ r    │ 1791099446 │
│ 7#$  │  124312908 │
└──────┴────────────┘
```

<div id="details-of-implementation">
  ## Detalles de la implementación
</div>

* No compatible con:
  * `ALTER`
  * `SELECT ... SAMPLE`
  * `INSERT`
  * Índices
  * Replicación