---
alias: []
description: 'Documentación sobre el formato LineAsString'
input_format: true
keywords: ['LineAsString']
output_format: true
slug: /interfaces/formats/LineAsString
title: 'LineAsString'
doc_type: 'reference'
---

| Entrada | Salida | Alias |
| ------- | ------ | ----- |
| ✔       | ✔      |       |

<div id="description">
  ## Descripción
</div>

El formato `LineAsString` interpreta cada línea de los datos de entrada como un único valor de texto.
Este formato solo puede analizarse en una tabla con un único campo de tipo [String](/es/sql-reference/data-types/string.md).
Las columnas restantes deben definirse como [`DEFAULT`](/es/sql-reference/statements/create/table.md/#default), [`MATERIALIZED`](/es/sql-reference/statements/create/view#materialized-view) o bien omitirse.

<div id="example-usage">
  ## Ejemplo de uso
</div>

```sql title="Query"
DROP TABLE IF EXISTS line_as_string;
CREATE TABLE line_as_string (field String) ENGINE = Memory;
INSERT INTO line_as_string FORMAT LineAsString "I love apple", "I love banana", "I love orange";
SELECT * FROM line_as_string;
```

```text title="Response"
┌─field─────────────────────────────────────────────┐
│ "I love apple", "I love banana", "I love orange"; │
└───────────────────────────────────────────────────┘
```

<div id="format-settings">
  ## Configuración del formato
</div>
