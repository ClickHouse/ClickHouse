---
alias: []
description: 'Documentación sobre el formato Values'
input_format: true
keywords: ['Values']
output_format: true
slug: /interfaces/formats/Values
title: 'Values'
doc_type: 'guide'
---

| Entrada | Salida | Alias |
| ------- | ------ | ----- |
| ✔       | ✔      |       |

<div id="description">
  ## Descripción
</div>

El formato `Values` imprime cada fila entre corchetes.

* Las filas se separan por comas, sin coma después de la última fila.
* Los valores dentro de los corchetes también se separan por comas.
* Los números se muestran en formato decimal, sin comillas.
* Los arrays se muestran en `[]`.
* Las cadenas, las fechas y los valores de fecha y hora se muestran entre comillas.
* Las reglas de escape y el análisis son similares a las del formato [TabSeparated](TabSeparated/TabSeparated.md).

Durante el formateo no se insertan espacios adicionales, pero durante el análisis se permiten y se omiten (excepto los espacios dentro de los valores de array, que no están permitidos).
[`NULL`](/es/sql-reference/syntax.md) se representa como `NULL`.

El conjunto mínimo de caracteres que debes escapar al pasar datos en el formato `Values` es:

* comillas simples
* barras invertidas

Este es el formato que se usa en `INSERT INTO t VALUES ...`, pero también puedes usarlo para dar formato a los resultados de la consulta.

<div id="example-usage">
  ## Ejemplo de uso
</div>

<div id="inserting-data">
  ### Inserción de datos
</div>

El formato `Values` es el que utiliza `INSERT`, por lo que cualquier sentencia `INSERT ... VALUES`
ya lo utiliza. La cláusula `FORMAT Values` se puede indicar explícitamente, y las
filas se pueden suministrar desde un flujo o un archivo. Cada fila es una
tupla entre paréntesis con elementos separados por comas, y las propias tuplas están separadas por comas:

```sql title="Query"
CREATE TABLE t (id UInt32, name String, values Array(UInt32)) ENGINE = Memory;

INSERT INTO t FORMAT Values (1, 'a', [10, 20]), (2, 'b', [30]);

SELECT * FROM t ORDER BY id;
```

```response title="Response"
┌─id─┬─name─┬─values──┐
│  1 │ a    │ [10,20] │
│  2 │ b    │ [30]    │
└────┴──────┴─────────┘
```

<div id="using-expressions">
  ### Uso de expresiones en la entrada
</div>

A diferencia de la mayoría de los formatos de entrada, `Values` puede evaluar expresiones SQL en cada campo
en lugar de aceptar solo literales. Esto se controla mediante
[`input_format_values_interpret_expressions`](#format-settings) (habilitado de
forma predeterminada): cuando un campo no puede ser leído por el analizador de streaming rápido, ClickHouse
recurre al analizador de SQL e interpreta el campo como una expresión.

```sql title="Query"
CREATE TABLE prices (item String, total UInt32) ENGINE = Memory;

INSERT INTO prices FORMAT Values ('apple', 3 * 4), ('pear', length('hello') + 10);

SELECT * FROM prices ORDER BY total;
```

```response title="Response"
┌─item──┬─total─┐
│ apple │    12 │
│ pear  │    15 │
└───────┴───────┘
```

<div id="selecting-data">
  ### Selección de datos
</div>

El formato `Values` también puede usarse para dar formato a los resultados de la consulta. Los números se
escriben sin comillas, los arrays entre `[]`, y las cadenas y fechas entre comillas simples;
las comillas simples y las barras invertidas dentro de las cadenas se escapan con una barra invertida, y
[`NULL`](/es/sql-reference/syntax.md) se escribe como `NULL`:

```sql title="Query"
SELECT 1 AS a, 'O''Reilly' AS b, NULL::Nullable(String) AS c FORMAT Values;
```

```response title="Response"
(1,'O\'Reilly',NULL)
```

<div id="format-settings">
  ## Configuración del formato
</div>

| Ajuste                                                                                                                                                      | Descripción                                                                                                                                                                                                                                             | Predeterminado |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------- |
| [`input_format_values_interpret_expressions`](../../operations/settings/settings-formats.md/#input_format_values_interpret_expressions)                     | Si el campo no puede ser analizado por el analizador de streaming, ejecute el analizador de SQL e intente interpretarlo como una expresión de SQL.                                                                                                     | `true`         |
| [`input_format_values_deduce_templates_of_expressions`](../../operations/settings/settings-formats.md/#input_format_values_deduce_templates_of_expressions) | Si el campo no puede ser analizado por el analizador de streaming, ejecute el analizador de SQL, deduzca la plantilla de la expresión de SQL, intente analizar todas las filas con la plantilla y luego interprete la expresión para todas las filas. | `true`         |
| [`input_format_values_accurate_types_of_literals`](../../operations/settings/settings-formats.md/#input_format_values_accurate_types_of_literals)           | Al analizar e interpretar expressions con la plantilla, compruebe el tipo real del literal para evitar posibles problemas de overflow y precisión.                                                                                                      | `true`         |