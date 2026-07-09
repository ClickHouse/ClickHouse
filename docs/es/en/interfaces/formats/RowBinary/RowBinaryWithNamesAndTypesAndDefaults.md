---
alias: []
description: 'Documentación sobre el formato RowBinaryWithNamesAndTypesAndDefaults'
input_format: true
keywords: ['RowBinaryWithNamesAndTypesAndDefaults']
output_format: false
slug: /interfaces/formats/RowBinaryWithNamesAndTypesAndDefaults
title: 'RowBinaryWithNamesAndTypesAndDefaults'
doc_type: 'reference'
---

import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| Entrada | Salida | Alias |
| ------- | ------ | ----- |
| ✔       | ✗      |       |

<div id="description">
  ## Descripción
</div>

Similar al formato [`RowBinaryWithNamesAndTypes`](./RowBinaryWithNamesAndTypes.md), pero con un byte adicional antes de cada celda que indica si debe usarse el valor `DEFAULT` de la columna, exactamente igual que en el formato [`RowBinaryWithDefaults`](./RowBinaryWithDefaults.md). Esta combinación admite `INSERT`s con evolución del esquema: quien escribe puede omitir columnas del encabezado (reciben el valor `DEFAULT` de la columna de destino) y, para cualquier columna que sí envíe, puede marcar celdas individuales como &quot;usar el valor `DEFAULT` de la columna&quot; sin confundirlo con `NULL`.

Este formato es solo de entrada.

<div id="wire-format">
  ## Wire format
</div>

El encabezado es idéntico al de [`RowBinaryWithNamesAndTypes`](./RowBinaryWithNamesAndTypes.md):

1. Un `VarUInt` con el número de columnas `N`.
2. `N` `String` con prefijo de longitud que contienen los nombres de las columnas.
3. `N` tipos de columna: nombres textuales o codificación binaria compacta, controlados por la configuración `output_format_binary_encode_types_in_binary_format` / `input_format_binary_decode_types_in_binary_format`.

Después del encabezado, cada fila consta de `N` celdas. Para cada celda:

* Un único byte marcador `UInt8`.
  * `0x01` — usa la expresión `DEFAULT` de la columna de destino. No le siguen bytes de valor.
  * `0x00` — a continuación viene un valor, serializado mediante el serializador `RowBinary` del tipo de columna. Para `Nullable(T)`, los bytes del valor empiezan con el byte nulo de `Nullable` (`0` para no nulo, `1` para `NULL`), seguido del valor interno si no es `NULL`.

<div id="defaults-vs-null">
  ## Valores por defecto vs NULL
</div>

El marcador de valor por defecto por celda y el byte nulo incorporado de `Nullable` son independientes. Una columna `Nullable(UInt32) DEFAULT 42` puede enviarse de tres maneras distintas por fila:

| Bytes     | Significado                                                   |
| --------- | ------------------------------------------------------------- |
| `01`      | Usar `DEFAULT 42`.                                            |
| `00 01`   | Ruta de valor y, después, `NULL` mediante el tipo `Nullable`. |
| `00 00 …` | Ruta de valor y, después, un valor interno no NULL.           |

<div id="schema-evolution">
  ## Evolución del esquema
</div>

| Caso                                                                   | Comportamiento                                                                                                                                                                                     |
| ---------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Columna ausente por completo del encabezado del archivo                | Se rellena en el destino mediante `insertDefaultsForNotSeenColumns`; condicionado por `defaults_for_omitted_fields`.                                                                               |
| Columna presente en el encabezado, marcador de celda `0x01`            | `insertDefault` por fila.                                                                                                                                                                          |
| Columna presente en el encabezado, marcador de celda `0x00`            | El valor se procesa normalmente.                                                                                                                                                                   |
| Columna adicional en el encabezado, no presente en la tabla de destino | Se descarta silenciosamente cuando `input_format_skip_unknown_fields = 1` (primero se consume el marcador; si es `0x01`, no hay nada más; si es `0x00`, el valor tipado se procesa y se descarta). |

<div id="example-usage">
  ## Ejemplo de uso
</div>

```sql title="Query"
SELECT * FROM format(
    'RowBinaryWithNamesAndTypesAndDefaults',
    'x Nullable(UInt32) DEFAULT 42',
    unhex('01' || '0178' || '10' || hex('Nullable(UInt32)') || '01')
);
```

```response title="Response"
┌──x─┐
│ 42 │
└────┘
```

* El encabezado contiene una columna llamada `x` de tipo `Nullable(UInt32)`.
* La única celda utiliza el marcador `0x01`, que significa &quot;usar `DEFAULT 42`&quot;.

<div id="format-settings">
  ## Configuración de formato
</div>

<RowBinaryFormatSettings />