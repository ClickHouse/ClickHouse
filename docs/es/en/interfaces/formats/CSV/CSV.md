---
alias: []
description: 'Documentación sobre el formato CSV'
input_format: true
keywords: ['CSV']
output_format: true
slug: /interfaces/formats/CSV
title: 'CSV'
doc_type: 'referencia'
---

<div id="description">
  ## Descripción
</div>

Formato Comma Separated Values ([RFC](https://tools.ietf.org/html/rfc4180)).
Al aplicar el formato, las filas se encierran entre comillas dobles. Una comilla doble dentro de una cadena se muestra como dos comillas dobles seguidas.
No hay otras reglas de escape de caracteres.

* La fecha y la fecha-hora se encierran entre comillas dobles.
* Los números se muestran sin comillas.
* Los valores se separan mediante un carácter delimitador, que de forma predeterminada es `,`. El carácter delimitador se define en la configuración [format&#95;csv&#95;delimiter](/es/operations/settings/settings-formats.md/#format_csv_delimiter).
* Las filas se separan usando el salto de línea de Unix (LF).
* Los Arrays se serializan en CSV de la siguiente manera:
  * primero, el array se serializa como una cadena, igual que en el formato TabSeparated
  * La cadena resultante se muestra en CSV entre comillas dobles.
* Los Tuples en formato CSV se serializan como columnas independientes (es decir, se pierde su anidamiento en la tupla).

```bash
$ clickhouse-client --format_csv_delimiter="|" --query="INSERT INTO test.csv FORMAT CSV" < data.csv
```

:::note
De forma predeterminada, el delimitador es `,`
Consulte la configuración [format&#95;csv&#95;delimiter](/es/operations/settings/settings-formats.md/#format_csv_delimiter) para obtener más información.
:::

Durante el análisis, todos los valores pueden interpretarse con comillas o sin ellas. Se admiten tanto comillas dobles como comillas simples.

Las filas también pueden disponerse sin comillas. En este caso, se interpretan hasta el carácter delimitador o el salto de línea (CR o LF).
Sin embargo, incumpliendo el RFC, al analizar filas sin comillas, se ignoran los espacios y tabulaciones iniciales y finales.
El salto de línea admite los siguientes tipos: Unix (LF), Windows (CR LF) y Mac OS Classic (CR LF).

`NULL` se formatea según la configuración [format&#95;csv&#95;null&#95;representation](/es/operations/settings/settings-formats.md/#format_csv_null_representation) (el valor predeterminado es `\N`).

En los datos de entrada, los valores `ENUM` pueden representarse como nombres o como identificadores.
Primero, intentamos hacer coincidir el valor de entrada con el nombre de `ENUM`.
Si no se encuentra coincidencia y el valor de entrada es un número, intentamos hacer coincidir ese número con el identificador de `ENUM`.
Si los datos de entrada contienen solo identificadores de `ENUM`, se recomienda habilitar la configuración [input&#95;format&#95;csv&#95;enum&#95;as&#95;number](/es/operations/settings/settings-formats.md/#input_format_csv_enum_as_number) para optimizar el análisis de `ENUM`.

<div id="example-usage">
  ## Ejemplo de uso
</div>

<div id="format-settings">
  ## Configuración del formato
</div>

| Configuración                                                                                                                                                                            | Descripción                                                                                                                                    | Predeterminado | Notas                                                                                                                                                                                                                     |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- | -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [format&#95;csv&#95;delimiter](/es/operations/settings/settings-formats.md/#format_csv_delimiter)                                                                                           | el carácter que se considerará delimitador en los datos CSV.                                                                                   | `,`            |                                                                                                                                                                                                                           |
| [format&#95;csv&#95;allow&#95;single&#95;quotes](/es/operations/settings/settings-formats.md/#format_csv_allow_single_quotes)                                                               | permite cadenas entre comillas simples.                                                                                                        | `true`         |                                                                                                                                                                                                                           |
| [format&#95;csv&#95;allow&#95;double&#95;quotes](/es/operations/settings/settings-formats.md/#format_csv_allow_double_quotes)                                                               | permite cadenas entre comillas dobles.                                                                                                         | `true`         |                                                                                                                                                                                                                           |
| [format&#95;csv&#95;null&#95;representation](/es/operations/settings/settings-formats.md/#format_tsv_null_representation)                                                                   | representación personalizada de NULL en formato CSV.                                                                                           | `\N`           |                                                                                                                                                                                                                           |
| [input&#95;format&#95;csv&#95;empty&#95;as&#95;default](/es/operations/settings/settings-formats.md/#input_format_csv_empty_as_default)                                                     | trata los campos vacíos en la entrada CSV como valores predeterminados.                                                                        | `true`         | Para expresiones predeterminadas complejas, también se debe habilitar [input&#95;format&#95;defaults&#95;for&#95;omitted&#95;fields](/es/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields). |
| [input&#95;format&#95;csv&#95;enum&#95;as&#95;number](/es/operations/settings/settings-formats.md/#input_format_csv_enum_as_number)                                                         | trata los valores enum insertados en formatos CSV como índices de enum.                                                                        | `false`        |                                                                                                                                                                                                                           |
| [input&#95;format&#95;csv&#95;use&#95;best&#95;effort&#95;in&#95;schema&#95;inference](/es/operations/settings/settings-formats.md/#input_format_csv_use_best_effort_in_schema_inference)   | usa algunos ajustes y heurísticas para inferir el esquema en formato CSV. Si se desactiva, todos los campos se inferirán como cadenas.         | `true`         |                                                                                                                                                                                                                           |
| [input&#95;format&#95;csv&#95;arrays&#95;as&#95;nested&#95;csv](/es/operations/settings/settings-formats.md/#input_format_csv_arrays_as_nested_csv)                                         | al leer Array desde CSV, espera que sus elementos se hayan serializado como CSV anidado y luego se hayan colocado en una cadena.               | `false`        |                                                                                                                                                                                                                           |
| [output&#95;format&#95;csv&#95;crlf&#95;end&#95;of&#95;line](/es/operations/settings/settings-formats.md/#output_format_csv_crlf_end_of_line)                                               | si se establece en true, el final de línea en el formato de salida CSV será `\r\n` en lugar de `\n`.                                           | `false`        |                                                                                                                                                                                                                           |
| [input&#95;format&#95;csv&#95;skip&#95;first&#95;lines](/es/operations/settings/settings-formats.md/#input_format_csv_skip_first_lines)                                                     | omite el número especificado de líneas al principio de los datos.                                                                              | `0`            |                                                                                                                                                                                                                           |
| [input&#95;format&#95;csv&#95;detect&#95;header](/es/operations/settings/settings-formats.md/#input_format_csv_detect_header)                                                               | detecta automáticamente el encabezado con nombres y types en formato CSV.                                                                      | `true`         |                                                                                                                                                                                                                           |
| [input&#95;format&#95;csv&#95;skip&#95;trailing&#95;empty&#95;lines](/es/operations/settings/settings-formats.md/#input_format_csv_skip_trailing_empty_lines)                               | omite las líneas vacías finales al final de los datos.                                                                                         | `false`        |                                                                                                                                                                                                                           |
| [input&#95;format&#95;csv&#95;trim&#95;whitespaces](/es/operations/settings/settings-formats.md/#input_format_csv_trim_whitespaces)                                                         | elimina espacios y tabulaciones en cadenas CSV sin comillas.                                                                                   | `true`         |                                                                                                                                                                                                                           |
| [input&#95;format&#95;csv&#95;allow&#95;whitespace&#95;or&#95;tab&#95;as&#95;delimiter](/es/operations/settings/settings-formats.md/#input_format_csv_allow_whitespace_or_tab_as_delimiter) | permite usar espacios en blanco o tabulaciones como delimitador de campos en cadenas CSV.                                                      | `false`        |                                                                                                                                                                                                                           |
| [input&#95;format&#95;csv&#95;allow&#95;variable&#95;number&#95;of&#95;columns](/es/operations/settings/settings-formats.md/#input_format_csv_allow_variable_number_of_columns)             | permite un número variable de columnas en formato CSV, ignora las columnas adicionales y usa valores predeterminados en las columnas ausentes. | `false`        |                                                                                                                                                                                                                           |
| [input&#95;format&#95;csv&#95;use&#95;default&#95;on&#95;bad&#95;values](/es/operations/settings/settings-formats.md/#input_format_csv_use_default_on_bad_values)                           | permite asignar el valor predeterminado a la columna cuando falle la deserialización de un campo CSV por un valor no válido.                   | `false`        |                                                                                                                                                                                                                           |
| [input&#95;format&#95;csv&#95;try&#95;infer&#95;numbers&#95;from&#95;strings](/es/operations/settings/settings-formats.md/#input_format_csv_try_infer_numbers_from_strings)                 | intenta inferir números a partir de campos de cadena durante la inferencia del esquema.                                                           | `false`        |                                                                                                                                                                                                                           |