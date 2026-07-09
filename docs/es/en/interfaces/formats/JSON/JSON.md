---
alias: []
description: 'Documentación sobre el formato JSON'
input_format: true
keywords: ['JSON']
output_format: true
slug: /interfaces/formats/JSON
title: 'JSON'
doc_type: 'reference'
---

| Entrada | Salida | Alias |
| ------- | ------ | ----- |
| ✔       | ✔      |       |

<div id="description">
  ## Descripción
</div>

El formato `JSON` lee y genera datos en formato JSON.

El formato `JSON` devuelve lo siguiente:

| Parámetro                    | Descripción                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| ---------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `meta`                       | Nombres y tipos de las columnas.                                                                                                                                                                                                                                                                                                                                                                                                             |
| `data`                       | Tablas de datos                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `rows`                       | El número total de filas de salida.                                                                                                                                                                                                                                                                                                                                                                                                          |
| `rows_before_limit_at_least` | La estimación mínima del número de filas que habría habido sin LIMIT. Solo se muestra si la consulta contiene LIMIT. Esta estimación se calcula a partir de los bloques de datos procesados en el query pipeline antes de la transformación de límite, pero después podrían ser descartados por esa transformación. Si los bloques ni siquiera llegaron a la transformación de límite en el query pipeline, no se incluyen en la estimación. |
| `statistics`                 | Estadísticas como `elapsed`, `rows_read`, `bytes_read`.                                                                                                                                                                                                                                                                                                                                                                                      |
| `totals`                     | Valores totales (al usar WITH TOTALS).                                                                                                                                                                                                                                                                                                                                                                                                       |
| `extremes`                   | Valores extremos (cuando `extremes` está establecido en 1).                                                                                                                                                                                                                                                                                                                                                                                  |

El formato `JSON` es compatible con JavaScript. Para garantizarlo, algunos caracteres se escapan adicionalmente:

* la barra `/` se escapa como `\/`
* los saltos de línea alternativos `U+2028` y `U+2029`, que provocan errores en algunos navegadores, se escapan como `\uXXXX`.
* Los caracteres de control ASCII se escapan: retroceso, salto de página, salto de línea, retorno de carro y tabulación horizontal se reemplazan por `\b`, `\f`, `\n`, `\r`, `\t`, así como los bytes restantes en el rango 00-1F mediante secuencias `\uXXXX`.
* Las secuencias UTF-8 no válidas se sustituyen por el carácter de reemplazo � para que el texto de salida consista en secuencias UTF-8 válidas.

Para mantener la compatibilidad con JavaScript, los enteros Int64 y UInt64 se encierran entre comillas dobles de forma predeterminada.
Para quitar las comillas, puede establecer el parámetro de configuración [`output_format_json_quote_64bit_integers`](/es/operations/settings/settings-formats.md/#output_format_json_quote_64bit_integers) en `0`.

ClickHouse admite [NULL](/es/sql-reference/syntax.md), que se muestra como `null` en la salida JSON. Para habilitar los valores `+nan`, `-nan`, `+inf`, `-inf` en la salida, establezca [output&#95;format&#95;json&#95;quote&#95;denormals](/es/operations/settings/settings-formats.md/#output_format_json_quote_denormals) en `1`.

<div id="example-usage">
  ## Ejemplo de uso
</div>

Ejemplo:

```sql
SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase WITH TOTALS ORDER BY c DESC LIMIT 5 FORMAT JSON
```

```json
{
        "meta":
        [
                {
                        "name": "num",
                        "type": "Int32"
                },
                {
                        "name": "str",
                        "type": "String"
                },
                {
                        "name": "arr",
                        "type": "Array(UInt8)"
                }
        ],

        "data":
        [
                {
                        "num": 42,
                        "str": "hello",
                        "arr": [0,1]
                },
                {
                        "num": 43,
                        "str": "hello",
                        "arr": [0,1,2]
                },
                {
                        "num": 44,
                        "str": "hello",
                        "arr": [0,1,2,3]
                }
        ],

        "rows": 3,

        "rows_before_limit_at_least": 3,

        "statistics":
        {
                "elapsed": 0.001137687,
                "rows_read": 3,
                "bytes_read": 24
        }
}
```

<div id="format-settings">
  ## Configuración de formato
</div>

Para el formato de entrada JSON, si la configuración [`input_format_json_validate_types_from_metadata`](/es/operations/settings/settings-formats.md/#input_format_json_validate_types_from_metadata) está establecida en `1`,
los tipos de los metadatos de los datos de entrada se compararán con los tipos de las columnas correspondientes de la tabla.

<div id="see-also">
  ## Véase también
</div>

* formato [JSONEachRow](/es/interfaces/formats/JSONEachRow)
* ajuste [output&#95;format&#95;json&#95;array&#95;of&#95;rows](/es/operations/settings/settings-formats.md/#output_format_json_array_of_rows)