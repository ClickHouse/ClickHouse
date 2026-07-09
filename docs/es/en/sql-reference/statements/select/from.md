---
description: 'Documentación de la cláusula FROM'
sidebar_label: 'FROM'
slug: /sql-reference/statements/select/from
title: 'Cláusula FROM'
doc_type: 'reference'
---

La cláusula `FROM` especifica la fuente desde la que se leen los datos:

* [Tabla](../../../engines/table-engines/index.md)
* [Subconsulta](../../../sql-reference/statements/select/index.md)
* [Función de tabla](/es/sql-reference/table-functions)

Las cláusulas [JOIN](../../../sql-reference/statements/select/join.md) y [ARRAY JOIN](../../../sql-reference/statements/select/array-join.md) también pueden usarse para ampliar la funcionalidad de la cláusula `FROM`.

Una subconsulta es otra consulta `SELECT` que puede especificarse entre paréntesis dentro de la cláusula `FROM`.

Una cláusula `VALUES` del estándar SQL también puede usarse como expresión de tabla:

```sql
SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, val);
```

Consulte la [función de tabla Values](/es/sql-reference/table-functions/values#sql-standard-values-clause) para obtener más información.

`FROM` puede contener varias fuentes de datos, separadas por comas, lo que equivale a realizar un [CROSS JOIN](../../../sql-reference/statements/select/join.md) entre ellas.

`FROM` puede aparecer opcionalmente antes de una cláusula `SELECT`. Esta es una extensión específica de ClickHouse del SQL estándar que facilita la lectura de las sentencias `SELECT`. Ejemplo:

```sql
FROM table
SELECT *
```

<div id="final-modifier">
  ## Modificador FINAL
</div>

Cuando se especifica `FINAL`, ClickHouse fusiona completamente los datos antes de devolver el resultado. Esto también realiza todas las transformaciones de datos que se producen durante las fusiones para el motor de tabla dado.

Se aplica al seleccionar datos de tablas que usan los siguientes motores de tabla:

* `ReplacingMergeTree`
* `SummingMergeTree`
* `AggregatingMergeTree`
* `CollapsingMergeTree`
* `VersionedCollapsingMergeTree`

Las consultas `SELECT` con `FINAL` se ejecutan en paralelo. La configuración [max&#95;final&#95;threads](/es/operations/settings/settings#max_final_threads) limita el número de hilos utilizados.

<div id="drawbacks">
  ### Desventajas
</div>

Las consultas que usan `FINAL` se ejecutan ligeramente más despacio que consultas similares que no lo usan porque:

* Los datos se fusionan durante la ejecución de la consulta.
* Las consultas con `FINAL` pueden leer las columnas de la clave primaria además de las columnas especificadas en la consulta.

`FINAL` requiere recursos adicionales de procesamiento y memoria porque el procesamiento que normalmente tendría lugar durante la fusión debe realizarse en memoria en el momento de la consulta. Sin embargo, a veces es necesario usar `FINAL` para obtener resultados precisos (ya que es posible que los datos aún no se hayan fusionado por completo). Su costo es menor que ejecutar `OPTIMIZE` para forzar una fusión.

Como alternativa a usar `FINAL`, a veces es posible utilizar consultas distintas que asumen que los procesos en segundo plano del motor de tabla `MergeTree` aún no se han producido y resolverlo aplicando una agregación (por ejemplo, para descartar duplicados). Si necesita usar `FINAL` en sus consultas para obtener los resultados requeridos, puede hacerlo sin problema, pero tenga en cuenta el procesamiento adicional que requiere.

`FINAL` puede aplicarse automáticamente mediante la configuración [FINAL](../../../operations/settings/settings.md#final) a todas las tablas de una consulta usando una sesión o un perfil de usuario.

<div id="example-usage">
  ### Ejemplo de uso
</div>

Uso de la palabra clave `FINAL`

```sql
SELECT x, y FROM mytable FINAL WHERE x > 1;
```

Uso de `FINAL` como configuración de consulta

```sql
SELECT x, y FROM mytable WHERE x > 1 SETTINGS final = 1;
```

Uso de `FINAL` como configuración de sesión

```sql
SET final = 1;
SELECT x, y FROM mytable WHERE x > 1;
```

<div id="aliases-and-final">
  ### Alias y FINAL
</div>

Cuando una tabla tiene un alias, `FINAL` va después del alias. Esto se ve con mayor claridad en las consultas de [`JOIN`](/es/sql-reference/statements/select/join), donde las tablas suelen tener alias:

```sql
SELECT t1.id, t2.name
FROM table1 AS t1 FINAL
INNER JOIN table2 AS t2 FINAL ON t1.id = t2.id;
```

`FINAL` es un modificador de la referencia a la tabla, por lo que debe ir después de la expresión completa `table [AS alias]`. Colocarlo antes del alias (`FROM table1 FINAL AS t1`) es un error de sintaxis.

<div id="implementation-details">
  ## Detalles de implementación
</div>

Si se omite la cláusula `FROM`, los datos se leerán de la tabla `system.one`.
La tabla `system.one` contiene exactamente una fila (esta tabla cumple la misma función que la tabla DUAL presente en otros DBMS).

Para ejecutar una consulta, todas las columnas enumeradas en la consulta se extraen de la tabla correspondiente. Las columnas que no son necesarias para la consulta externa se descartan de las subconsultas.
Si una consulta no enumera ninguna columna (por ejemplo, `SELECT count() FROM t`), igualmente se extrae alguna columna de la tabla (preferiblemente la más pequeña) para calcular el número de filas.