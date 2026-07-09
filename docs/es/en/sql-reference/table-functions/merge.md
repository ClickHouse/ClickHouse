---
description: 'Crea una tabla Merge temporal. El esquema de la tabla se deriva de las tablas subyacentes mediante la unión de sus columnas y la deducción de tipos comunes.'
sidebar_label: 'merge'
sidebar_position: 130
slug: /sql-reference/table-functions/merge
title: 'merge'
doc_type: 'reference'
---

Crea una tabla temporal [Merge](../../engines/table-engines/special/merge.md).
El esquema de la tabla se deriva de las tablas subyacentes mediante la unión de sus columnas y la deducción de tipos comunes.
Están disponibles las mismas columnas virtuales que para el motor de tabla [Merge](../../engines/table-engines/special/merge.md).

<div id="syntax">
  ## Sintaxis
</div>

```sql
merge(['db_name',] 'tables_regexp')
```

<div id="arguments">
  ## Argumentos
</div>

| Argumento       | Descripción                                                                                                                                                                                                                                                                                                                                                       |
| --------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `db_name`       | Posibles valores (opcional; el valor predeterminado es `currentDatabase()`):<br />    - nombre de la base de datos,<br />    - expresión constante que devuelve una cadena con un nombre de base de datos, por ejemplo, `currentDatabase()`,<br />    - `REGEXP(expression)`, donde `expression` es una expresión regular que coincide con los nombres de las BD. |
| `tables_regexp` | Una expresión regular que coincide con los nombres de las tablas en la BD o las BD especificadas.                                                                                                                                                                                                                                                                 |

<div id="related">
  ## Relacionado
</div>

* motor de tabla [Merge](../../engines/table-engines/special/merge.md)