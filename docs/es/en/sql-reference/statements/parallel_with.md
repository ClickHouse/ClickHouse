---
description: 'Documentación de la cláusula PARALLEL WITH'
sidebar_label: 'PARALLEL WITH'
sidebar_position: 53
slug: /sql-reference/statements/parallel_with
title: 'Cláusula PARALLEL WITH'
doc_type: 'reference'
---

Permite ejecutar varias sentencias en paralelo.

<div id="syntax">
  ## Sintaxis
</div>

```sql
statement1 PARALLEL WITH statement2 [PARALLEL WITH statement3 ...]
```

Ejecuta las sentencias `statement1`, `statement2`, `statement3`, ... en paralelo. La salida de esas sentencias se descarta.

Ejecutar sentencias en paralelo puede ser más rápido que ejecutar una secuencia de las mismas sentencias en muchos casos. Por ejemplo, `statement1 PARALLEL WITH statement2 PARALLEL WITH statement3` probablemente sea más rápido que `statement1; statement2; statement3`.

<div id="examples">
  ## Ejemplos
</div>

Crea dos tablas en paralelo:

```sql
CREATE TABLE table1(x Int32) ENGINE = MergeTree ORDER BY tuple()
PARALLEL WITH
CREATE TABLE table2(y String) ENGINE = MergeTree ORDER BY tuple();
```

Elimina dos tablas en paralelo:

```sql
DROP TABLE table1
PARALLEL WITH
DROP TABLE table2;
```

<div id="settings">
  ## Configuración
</div>

El ajuste [max&#95;threads](../../operations/settings/settings.md#max_threads) controla cuántos hilos se crean.

<div id="comparison-with-union">
  ## Comparación con UNION
</div>

La cláusula `PARALLEL WITH` es algo similar a [UNION](select/union.md), que también ejecuta sus operandos en paralelo. Sin embargo, hay algunas diferencias:

* `PARALLEL WITH` no devuelve ningún resultado de la ejecución de sus operandos; si se produce alguna, solo puede volver a lanzar una excepción;
* `PARALLEL WITH` no requiere que sus operandos tengan el mismo conjunto de columnas de resultado;
* `PARALLEL WITH` puede ejecutar cualquier sentencia (no solo `SELECT`).