---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

# Cláusula FROM {#select-from}

El `FROM` cláusula especifica la fuente de la que se leen los datos:

-   [Tabla](../../../engines/table-engines/index.md)
-   [Subconsultas](index.md) {## TODO: mejor enlace ##}
-   [Función de la tabla](../../table-functions/index.md#table-functions)

[JOIN](join.md) y [ARRAY JOIN](array-join.md) también pueden utilizarse para ampliar la funcionalidad del `FROM` clausula.

La subconsulta es otra `SELECT` consulta que se puede especificar entre paréntesis dentro `FROM` clausula.

`FROM` cláusula puede contener múltiples fuentes de datos, separadas por comas, que es equivalente a realizar [CROSS JOIN](join.md) en ellos.

## Modificador FINAL {#select-from-final}

Cuando `FINAL` se especifica, ClickHouse fusiona completamente los datos antes de devolver el resultado y, por lo tanto, realiza todas las transformaciones de datos que ocurren durante las fusiones para el motor de tabla dado.

Es aplicable cuando se seleccionan datos de tablas que utilizan [Método de codificación de datos:](../../../engines/table-engines/mergetree-family/mergetree.md)- familia del motor (excepto `GraphiteMergeTree`). También soportado para:

-   [Replicado](../../../engines/table-engines/mergetree-family/replication.md) versiones de `MergeTree` motor.
-   [Vista](../../../engines/table-engines/special/view.md), [Búfer](../../../engines/table-engines/special/buffer.md), [Distribuido](../../../engines/table-engines/special/distributed.md), y [Método de codificación de datos:](../../../engines/table-engines/special/materializedview.md) motores que funcionan sobre otros motores, siempre que se hayan creado sobre `MergeTree`-mesas de motor.

### Inconveniente {#drawbacks}

Consultas que usan `FINAL` se ejecutan no tan rápido como consultas similares que no lo hacen, porque:

-   La consulta se ejecuta en un solo subproceso y los datos se combinan durante la ejecución de la consulta.
-   Consultas con `FINAL` leer columnas de clave primaria además de las columnas especificadas en la consulta.

**En la mayoría de los casos, evite usar `FINAL`.** El enfoque común es utilizar diferentes consultas que asumen los procesos en segundo plano de la `MergeTree` el motor aún no ha sucedido y tratar con él mediante la aplicación de agregación (por ejemplo, para descartar duplicados). {## TODO: ejemplos ##}

## Detalles de implementación {#implementation-details}

Si el `FROM` se omite la cláusula, los datos se leerán desde el `system.one` tabla.
El `system.one` table contiene exactamente una fila (esta tabla cumple el mismo propósito que la tabla DUAL que se encuentra en otros DBMS).

Para ejecutar una consulta, todas las columnas enumeradas en la consulta se extraen de la tabla adecuada. Las columnas no necesarias para la consulta externa se eliminan de las subconsultas.
Si una consulta no muestra ninguna columnas (por ejemplo, `SELECT count() FROM t`), alguna columna se extrae de la tabla de todos modos (se prefiere la más pequeña), para calcular el número de filas.
