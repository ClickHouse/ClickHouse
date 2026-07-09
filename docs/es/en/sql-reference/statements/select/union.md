---
description: 'Documentación de la cláusula UNION'
sidebar_label: 'UNION'
slug: /sql-reference/statements/select/union
title: 'Cláusula UNION'
doc_type: 'reference'
---

Puede usar `UNION` especificando explícitamente `UNION ALL` o `UNION DISTINCT`.

Si no especifica `ALL` o `DISTINCT`, dependerá de la configuración de `union_default_mode`. La diferencia entre `UNION ALL` y `UNION DISTINCT` es que `UNION DISTINCT` elimina los duplicados del resultado de la unión; equivale a aplicar `SELECT DISTINCT` a una subconsulta que contiene `UNION ALL`.

Puede usar `UNION` para combinar cualquier cantidad de consultas `SELECT` concatenando sus resultados. Ejemplo:

```sql title="Query"
SELECT CounterID, 1 AS table, toInt64(count()) AS c
    FROM test.hits
    GROUP BY CounterID

UNION ALL

SELECT CounterID, 2 AS table, sum(Sign) AS c
    FROM test.visits
    GROUP BY CounterID
    HAVING c > 0
```

Las columnas del resultado se emparejan por su índice (orden dentro de `SELECT`). Si los nombres de las columnas no coinciden, los nombres del resultado final se toman de la primera consulta.

La conversión de tipos se realiza para las uniones. Por ejemplo, si dos consultas que se combinan tienen el mismo campo con tipos `Nullable` y no `Nullable` de un tipo compatible, el `UNION` resultante tiene un campo de tipo `Nullable`.

Las consultas que forman parte de `UNION` pueden encerrarse entre `()`. [ORDER BY](../../../sql-reference/statements/select/order-by.md) y [LIMIT](../../../sql-reference/statements/select/limit.md) se aplican a consultas individuales, no al resultado final. Si necesita aplicar una conversión al resultado final, puede poner todas las consultas con `UNION` en una subconsulta en la cláusula [FROM](../../../sql-reference/statements/select/from.md).

Si usa `UNION` sin especificar explícitamente `UNION ALL` o `UNION DISTINCT`, puede indicar el modo de unión mediante la configuración [union&#95;default&#95;mode](/es/operations/settings/settings#union_default_mode). Los valores de esta configuración pueden ser `ALL`, `DISTINCT` o una cadena vacía. Sin embargo, si usa `UNION` con la configuración `union_default_mode` establecida en una cadena vacía, se producirá una excepción. Los siguientes ejemplos muestran los resultados de las consultas con distintos valores de esta configuración.

```sql title="Query"
SET union_default_mode = 'DISTINCT';
SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 2;
```

```text title="Response"
┌─1─┐
│ 1 │
└───┘
┌─1─┐
│ 2 │
└───┘
┌─1─┐
│ 3 │
└───┘
```

```sql title="Query"
SET union_default_mode = 'ALL';
SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 2;
```

```text title="Response"
┌─1─┐
│ 1 │
└───┘
┌─1─┐
│ 2 │
└───┘
┌─1─┐
│ 2 │
└───┘
┌─1─┐
│ 3 │
└───┘
```

Las consultas que forman parte de `UNION/UNION ALL/UNION DISTINCT` pueden ejecutarse simultáneamente y sus resultados pueden mezclarse.

**Véase también**

* La configuración [insert&#95;null&#95;as&#95;default](../../../operations/settings/settings.md#insert_null_as_default).
* La configuración [union&#95;default&#95;mode](/es/operations/settings/settings#union_default_mode).