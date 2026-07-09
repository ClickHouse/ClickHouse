---
description: 'Documentación sobre índices hipotéticos'
sidebar_label: 'ÍNDICE HIPOTÉTICO'
sidebar_position: 47
slug: /sql-reference/statements/hypothetical-index
title: 'Índices hipotéticos'
doc_type: 'reference'
---

<div id="hypothetical-indexes">
  # Índices hipotéticos
</div>

Los índices hipotéticos son índices de omisión virtuales, con alcance de sesión, que se pueden adjuntar a una tabla de la familia `MergeTree` sin necesidad de construirlos ni almacenarlos realmente. Solo existen dentro de la sesión actual y [`EXPLAIN WHATIF`](/es/sql-reference/statements/explain#explain-whatif) los utiliza para estimar cómo un índice de omisión real afectaría a una consulta — normalmente, la proporción de omisión (la fracción de marcas que podrían omitirse) y un coste aproximado en marcas y bytes.

Use índices hipotéticos para evaluar posibles índices antes de asumir el coste de materializarlos en disco.

<div id="create-hypothetical-index">
  ## CREATE HYPOTHETICAL INDEX
</div>

```sql
CREATE HYPOTHETICAL INDEX [IF NOT EXISTS] name
    ON [db.]table_name (expression) TYPE type[(args)] [GRANULARITY value]
```

La sintaxis reproduce `ALTER TABLE ... ADD INDEX`, pero no se crea ni se escribe ningún índice; solo se almacena la descripción del índice en la sesión actual.

* `name` — nombre del índice; debe ser único dentro de `(database, table)` para esta sesión.
* `expression` — la columna o expresión que se va a indexar.
* `TYPE type` — `minmax`, `set(N)`, `bloom_filter(p)`, `ngrambf_v1(...)`, `tokenbf_v1(...)`. `text` y `vector_similarity` no se admiten y se rechazan en el momento de `CREATE`, porque la validación real de `ALTER TABLE ... ADD INDEX` depende de configuraciones a nivel de tabla que el almacén limitado a la sesión no puede replicar.
* `GRANULARITY value` — número de gránulos de datos por gránulo de índice. El valor predeterminado es 1.

La tabla de destino debe ser una tabla de la familia `MergeTree` en una base de datos `Atomic` (debe tener un UUID). Las tablas sin UUID —por ejemplo, en una base de datos `Ordinary` heredada o con la sintaxis antigua de `MergeTree`— se rechazan, porque el almacén de sesión identifica los índices hipotéticos por el UUID de la tabla.

**Ejemplo**

```sql
CREATE HYPOTHETICAL INDEX idx_b ON t (b) TYPE minmax GRANULARITY 1;
```

<div id="evaluating-a-hypothetical-index-with-explain-whatif">
  ## Evaluación de un índice hipotético con EXPLAIN WHATIF
</div>

Definir un índice hipotético por sí solo no tiene ningún efecto; para ver cómo afectaría a una consulta, ejecute [`EXPLAIN WHATIF`](/es/sql-reference/statements/explain#explain-whatif) en un `SELECT` representativo. El estimador informa sobre la aplicabilidad de cada índice candidato, las marcas que leería, la proporción de omisión resultante y cómo se generó la estimación (`empirical`, `statistical` o `applicability_only`).

```sql
CREATE TABLE t (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 100;

INSERT INTO t SELECT number, number FROM numbers(10000);

CREATE HYPOTHETICAL INDEX idx_b ON t (b) TYPE minmax GRANULARITY 1;

EXPLAIN WHATIF SELECT * FROM t WHERE b = 42;
```

Resultado:

```text
Baseline (after PK + partition + existing indexes):
  table:       default.t
  parts:       1
  marks:       100
  est_bytes:   85.52 KiB

With idx_b (minmax, hypothetical):
  status:       applicable
  marks:        1
  est_bytes:    875.00 B
  skip_ratio:   99.0%

Estimation:
  source:           empirical
  empirical_status: ok
  sampled_parts:    1 / 1
  sampled_marks:    100 / 100
  elapsed_us:       631
```

`est_bytes` es una estimación basada en el tamaño promedio de las filas de la tabla, por lo que la cifra exacta varía según el almacenamiento y la compresión.

Para omitir el escaneo empírico en memoria y estimar en su lugar a partir de las [estadísticas de columna](/es/engines/table-engines/mergetree-family/mergetree#column-statistics), primero defínalas en las columnas pertinentes (están desactivadas de forma predeterminada), espere a que termine la mutación de materialización y, a continuación, desactive la ruta empírica:

```sql
ALTER TABLE t ADD STATISTICS b TYPE TDigest;
ALTER TABLE t MATERIALIZE STATISTICS b SETTINGS mutations_sync = 1;

EXPLAIN WHATIF empirical = 0 SELECT * FROM t WHERE b < 10;
```

```text
With idx_b (minmax, hypothetical):
  status:       applicable
  marks:        1
  est_bytes:    1.66 KiB
  skip_ratio:   99.9%

Estimation:
  source:           statistical
  empirical_status: disabled
```

Consulte la referencia de [`EXPLAIN WHATIF`](/es/sql-reference/statements/explain#explain-whatif) para obtener el esquema de salida completo y los ajustes.

<div id="drop-hypothetical-index">
  ## DROP HYPOTHETICAL INDEX
</div>

```sql
DROP HYPOTHETICAL INDEX [IF EXISTS] name ON [db.]table_name
```

Elimina un índice hipotético de la sesión actual.

<div id="drop-all-hypothetical-indexes">
  ## DROP ALL HYPOTHETICAL INDEXES
</div>

```sql
DROP ALL HYPOTHETICAL INDEXES
```

Elimina todos los índices hipotéticos definidos en la sesión actual, sin importar la tabla.

<div id="scope-and-lifetime">
  ## Ámbito y duración
</div>

* Los índices hipotéticos existen solo en la **sesión actual**: no son visibles para otras sesiones y se descartan cuando la sesión termina.
* Definir o eliminar uno no crea ningún índice ni afecta nunca a las consultas habituales sobre la tabla. Sin embargo, `EXPLAIN WHATIF` empírico sí lee datos de la tabla para construir el índice candidato en memoria, y esa exploración se contabiliza en los límites de lectura y las cuotas de la sesión.
* Consulta los índices hipotéticos de la sesión actual mediante [`system.hypothetical_indexes`](/es/operations/system-tables/hypothetical_indexes).

<div id="limitations">
  ## Limitaciones
</div>

Los candidatos `text` y `vector_similarity` se rechazan en el momento de `CREATE HYPOTHETICAL INDEX`, porque su validación real depende de configuraciones a nivel de tabla que el almacén limitado a la sesión no puede replicar.

`EXPLAIN WHATIF` informa `status: not_applicable` para consultas con `FINAL` (la poda de índices de omisión interactúa con `PrimaryKeyExpand`) y devuelve `NOT_IMPLEMENTED` cuando la consulta se sirve desde una proyección (un índice de la tabla padre no se materializa en las partes de la proyección).

El `skip_ratio` empírico es una **cota superior**: cuenta cada gránulo restante de forma independiente y no modela la coalescencia de huecos de seek (`merge_tree_min_rows_for_seek` / `merge_tree_min_bytes_for_seek`), ni la combinación de un candidato con un índice de omisión existente bajo un predicado disyuntivo (`OR`). Por lo tanto, un índice materializado real puede leer algo más, o podar en casos en los que la estimación no lo hace.

<div id="required-privileges">
  ## Privilegios requeridos
</div>

`CREATE HYPOTHETICAL INDEX` requiere `SELECT` en las columnas a las que hace referencia la expresión del índice; `SELECT` a nivel de columna (por ejemplo, `GRANT SELECT(b)`) es suficiente, porque `EXPLAIN WHATIF` empírico lee esas columnas.

`DROP HYPOTHETICAL INDEX` y `DROP ALL HYPOTHETICAL INDEXES` no requieren ningún privilegio adicional; solo eliminan entradas del almacenamiento local de la sesión.

<div id="see-also">
  ## Véase también
</div>

* [`EXPLAIN WHATIF`](/es/sql-reference/statements/explain#explain-whatif)
* [`system.hypothetical_indexes`](/es/operations/system-tables/hypothetical_indexes)
* [índices de omisión de datos](/es/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping-indexes)