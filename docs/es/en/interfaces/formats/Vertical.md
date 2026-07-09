---
alias: []
description: 'Documentación sobre el formato Vertical'
input_format: false
keywords: ['Vertical']
output_format: true
slug: /interfaces/formats/Vertical
title: 'Vertical'
doc_type: 'reference'
---

| Entrada | Salida | Alias |
| ------- | ------ | ----- |
| ✗       | ✔      |       |

<div id="description">
  ## Descripción
</div>

Imprime cada valor en una línea independiente con el nombre de la columna especificado. Este formato resulta práctico para imprimir solo una o unas pocas filas si cada fila consta de un gran número de columnas.

Tenga en cuenta que [`NULL`](/es/sql-reference/syntax.md) se muestra como `ᴺᵁᴸᴸ` para que sea más fácil distinguir entre el valor de cadena `NULL` y la ausencia de valor. Las columnas JSON se mostrarán en un formato legible, y `NULL` se muestra como `null`, porque es un valor JSON válido y se distingue fácilmente de `"null"`.

<div id="example-usage">
  ## Ejemplo de uso
</div>

Ejemplo:

```sql
SELECT * FROM t_null FORMAT Vertical
```

```response
Row 1:
──────
x: 1
y: ᴺᵁᴸᴸ
```

En el formato Vertical, las filas no se escapan:

```sql
SELECT 'string with \'quotes\' and \t with some special \n characters' AS test FORMAT Vertical
```

```response
Row 1:
──────
test: string with 'quotes' and      with some special
 characters
```

Este formato solo es adecuado para la salida del resultado de una consulta, pero no para el análisis de datos (recuperar datos para insertarlos en una tabla).

<div id="format-settings">
  ## Configuración del formato
</div>
