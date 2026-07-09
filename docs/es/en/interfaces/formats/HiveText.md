---
alias: []
description: 'Documentación del formato HiveText'
input_format: true
keywords: ['HiveText']
output_format: false
slug: /interfaces/formats/HiveText
title: 'HiveText'
doc_type: 'reference'
---

| Entrada | Salida | Alias |
| ------- | ------ | ----- |
| ✔       | ✗      |       |

<div id="description">
  ## Descripción
</div>

`HiveText` lee el formato de serialización de texto que utilizan las tablas de
[Apache Hive](https://hive.apache.org/) (el formato generado por `LazySimpleSerDe` de Hive). Es un
formato de texto delimitado, similar a [`CSV`](/es/interfaces/formats/CSV), en el que los campos están
separados por el delimitador predeterminado de Hive `\x01` (Ctrl-A). El delimitador de campos se puede
configurar mediante [`input_format_hive_text_fields_delimiter`](#format-settings).

`HiveText` es un formato solo de entrada. Los datos no tienen fila de encabezado: los valores se
asignan por posición a las columnas de la tabla de destino, por lo que los nombres y tipos de las
columnas se toman de la tabla (o de una estructura proporcionada explícitamente) en lugar de
inferirse a partir de los datos. Durante la lectura, ClickHouse interpreta fechas y horas en modo
best-effort (consulte [`date_time_input_format`](/es/operations/settings/formats#date_time_input_format)),
completa los campos finales omitidos con los valores predeterminados de las columnas y omite los
campos que no reconoce.

Dentro de un campo, los valores se analizan con las mismas reglas de escape que `CSV`, en lugar de
usar los delimitadores anidados de Hive. En particular, una columna de tipo
[`Array`](/es/sql-reference/data-types/array) se lee a partir de la representación entre corchetes
(por ejemplo, `"['a','b','c']"`), no de valores separados por el delimitador de colección de
Hive `\x02`.

:::note La configuración de delimitadores anidados no tiene efecto
Las opciones [`input_format_hive_text_collection_items_delimiter`](#format-settings) y
[`input_format_hive_text_map_keys_delimiter`](#format-settings) se aceptan por compatibilidad, pero
actualmente no se usan durante el análisis.
:::

De forma predeterminada, se permite que las filas tengan un número variable de campos (consulte
[`input_format_hive_text_allow_variable_number_of_columns`](#format-settings)): en las filas con menos
campos que la tabla, las columnas faltantes se completan con valores predeterminados; en las filas
con campos finales adicionales, los campos extra se omiten.

<div id="example-usage">
  ## Ejemplo de uso
</div>

Los ejemplos siguientes sustituyen el delimitador de campos predeterminado por una coma (`,`) mediante
[`input_format_hive_text_fields_delimiter`](#format-settings), para que los archivos de entrada sean fáciles de leer.

<div id="reading-data">
  ### Leer un archivo HiveText
</div>

Dado un archivo `hive_data.txt` con campos separados por comas:

```text title="hive_data.txt"
1,3
3,5,9
```

Creamos una tabla que define los nombres y los tipos de las columnas, e insertamos el archivo
en ella con `FORMAT HiveText`:

```sql title="Query"
CREATE TABLE test_tbl (a UInt16, b UInt32, c UInt32) ENGINE = MergeTree ORDER BY a;

INSERT INTO test_tbl FROM INFILE 'hive_data.txt'
SETTINGS input_format_hive_text_fields_delimiter = ','
FORMAT HiveText;

SELECT * FROM test_tbl;
```

```response title="Response"
┌─a─┬─b─┬─c─┐
│ 1 │ 3 │ 0 │
│ 3 │ 5 │ 9 │
└───┴───┴───┘
```

Tenga en cuenta que la primera fila, `1,3`, tiene solo dos campos, por lo que la columna `c` que falta
se rellena con su valor predeterminado `0`.

<div id="variable-number-of-columns">
  ### Número variable de columnas
</div>

Con el valor predeterminado `input_format_hive_text_allow_variable_number_of_columns = 1`,
las filas que tienen más campos que la tabla simplemente omiten
los campos adicionales del final:

```text title="hive_extras.txt"
1,2,3,4,5
6,7,8
```

```sql title="Query"
CREATE TABLE test_extras (a UInt16, b UInt32, c UInt32) ENGINE = MergeTree ORDER BY a;

INSERT INTO test_extras FROM INFILE 'hive_extras.txt'
SETTINGS input_format_hive_text_fields_delimiter = ','
FORMAT HiveText;

SELECT * FROM test_extras ORDER BY a;
```

```response title="Response"
┌─a─┬─b─┬─c─┐
│ 1 │ 2 │ 3 │
│ 6 │ 7 │ 8 │
└───┴───┴───┘
```

Configurar `input_format_hive_text_allow_variable_number_of_columns = 0` en su lugar
impone un número estricto de campos, y una fila con menos campos que la tabla
provoca una excepción de análisis.

<div id="format-settings">
  ## Configuración del formato
</div>

| Setting                                                   | Description                                                                                                                                                               | Default |
| --------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| `input_format_hive_text_fields_delimiter`                 | Delimitador entre campos en Hive Text File                                                                                                                                | `\x01`  |
| `input_format_hive_text_collection_items_delimiter`       | Delimitador entre elementos de la colección (Array o Map) en Hive Text File. Se acepta, pero actualmente no se usa durante el análisis.                                   | `\x02`  |
| `input_format_hive_text_map_keys_delimiter`               | Delimitador entre una pareja de clave/valor de Map en Hive Text File. Se acepta, pero actualmente no se usa durante el análisis.                                          | `\x03`  |
| `input_format_hive_text_allow_variable_number_of_columns` | Ignora las columnas adicionales en la entrada de Hive Text (si el archivo tiene más columnas de las esperadas) y trata los campos que faltan como valores predeterminados | `1`     |