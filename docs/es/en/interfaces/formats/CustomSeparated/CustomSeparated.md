---
alias: []
description: 'Documentación sobre el formato CustomSeparated'
input_format: true
keywords: ['CustomSeparated']
output_format: true
slug: /interfaces/formats/CustomSeparated
title: 'CustomSeparated'
doc_type: 'reference'
---

| Entrada | Salida | Alias |
| ------- | ------ | ----- |
| ✔       | ✔      |       |

<div id="description">
  ## Descripción
</div>

Al igual que [Template](../Template/Template.md), imprime o lee todos los nombres y tipos de las columnas, y usa la regla de escape de la configuración [format&#95;custom&#95;escaping&#95;rule](../../../operations/settings/settings-formats.md/#format_custom_escaping_rule), así como los delimitadores de las siguientes configuraciones:

* [format&#95;custom&#95;field&#95;delimiter](/es/operations/settings/settings-formats.md/#format_custom_field_delimiter)
* [format&#95;custom&#95;row&#95;before&#95;delimiter](/es/operations/settings/settings-formats.md/#format_custom_row_before_delimiter)
* [format&#95;custom&#95;row&#95;after&#95;delimiter](/es/operations/settings/settings-formats.md/#format_custom_row_after_delimiter)
* [format&#95;custom&#95;row&#95;between&#95;delimiter](/es/operations/settings/settings-formats.md/#format_custom_row_between_delimiter)
* [format&#95;custom&#95;result&#95;before&#95;delimiter](/es/operations/settings/settings-formats.md/#format_custom_result_before_delimiter)
* [format&#95;custom&#95;result&#95;after&#95;delimiter](/es/operations/settings/settings-formats.md/#format_custom_result_after_delimiter)

:::note
No usa las configuraciones de reglas de escape ni los delimitadores de las cadenas de formato.
:::

También existe el formato [`CustomSeparatedIgnoreSpaces`](../CustomSeparated/CustomSeparatedIgnoreSpaces.md), similar a [TemplateIgnoreSpaces](../Template//TemplateIgnoreSpaces.md).

<div id="example-usage">
  ## Ejemplo de uso
</div>

<div id="inserting-data">
  ### Inserción de datos
</div>

Usando el siguiente archivo txt, llamado `football.txt`:

```text
row('2022-04-30';2021;'Sutton United';'Bradford City';1;4),row('2022-04-30';2021;'Swindon Town';'Barrow';2;1),row('2022-04-30';2021;'Tranmere Rovers';'Oldham Athletic';2;0),row('2022-05-02';2021;'Salford City';'Mansfield Town';2;2),row('2022-05-02';2021;'Port Vale';'Newport County';1;2),row('2022-05-07';2021;'Barrow';'Northampton Town';1;3),row('2022-05-07';2021;'Bradford City';'Carlisle United';2;0),row('2022-05-07';2021;'Bristol Rovers';'Scunthorpe United';7;0),row('2022-05-07';2021;'Exeter City';'Port Vale';0;1),row('2022-05-07';2021;'Harrogate Town A.F.C.';'Sutton United';0;2),row('2022-05-07';2021;'Hartlepool United';'Colchester United';0;2),row('2022-05-07';2021;'Leyton Orient';'Tranmere Rovers';0;1),row('2022-05-07';2021;'Mansfield Town';'Forest Green Rovers';2;2),row('2022-05-07';2021;'Newport County';'Rochdale';0;2),row('2022-05-07';2021;'Oldham Athletic';'Crawley Town';3;3),row('2022-05-07';2021;'Stevenage Borough';'Salford City';4;2),row('2022-05-07';2021;'Walsall';'Swindon Town';0;3)
```

Configure los ajustes del delimitador personalizado:

```sql
SET format_custom_row_before_delimiter = 'row(';
SET format_custom_row_after_delimiter = ')';
SET format_custom_field_delimiter = ';';
SET format_custom_row_between_delimiter = ',';
SET format_custom_escaping_rule = 'Quoted';
```

Inserta los datos:

```sql
INSERT INTO football FROM INFILE 'football.txt' FORMAT CustomSeparated;
```

<div id="reading-data">
  ### Lectura de datos
</div>

Configura los ajustes del delimitador personalizado:

```sql
SET format_custom_row_before_delimiter = 'row(';
SET format_custom_row_after_delimiter = ')';
SET format_custom_field_delimiter = ';';
SET format_custom_row_between_delimiter = ',';
SET format_custom_escaping_rule = 'Quoted';
```

Lea datos con el formato `CustomSeparated`:

```sql
SELECT *
FROM football
FORMAT CustomSeparated
```

La salida estará en el formato personalizado configurado:

```text
row('2022-04-30';2021;'Sutton United';'Bradford City';1;4),row('2022-04-30';2021;'Swindon Town';'Barrow';2;1),row('2022-04-30';2021;'Tranmere Rovers';'Oldham Athletic';2;0),row('2022-05-02';2021;'Port Vale';'Newport County';1;2),row('2022-05-02';2021;'Salford City';'Mansfield Town';2;2),row('2022-05-07';2021;'Barrow';'Northampton Town';1;3),row('2022-05-07';2021;'Bradford City';'Carlisle United';2;0),row('2022-05-07';2021;'Bristol Rovers';'Scunthorpe United';7;0),row('2022-05-07';2021;'Exeter City';'Port Vale';0;1),row('2022-05-07';2021;'Harrogate Town A.F.C.';'Sutton United';0;2),row('2022-05-07';2021;'Hartlepool United';'Colchester United';0;2),row('2022-05-07';2021;'Leyton Orient';'Tranmere Rovers';0;1),row('2022-05-07';2021;'Mansfield Town';'Forest Green Rovers';2;2),row('2022-05-07';2021;'Newport County';'Rochdale';0;2),row('2022-05-07';2021;'Oldham Athletic';'Crawley Town';3;3),row('2022-05-07';2021;'Stevenage Borough';'Salford City';4;2),row('2022-05-07';2021;'Walsall';'Swindon Town';0;3)
```

<div id="format-settings">
  ## Configuración del formato
</div>

Configuraciones adicionales:

| Configuración                                                                                                                                                                              | Descripción                                                                                                                                                       | Predeterminado |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------- |
| [input&#95;format&#95;custom&#95;detect&#95;header](../../../operations/settings/settings-formats.md/#input_format_custom_detect_header)                                                   | habilita la detección automática de una cabecera con nombres y tipos, si existe.                                                                                  | `true`         |
| [input&#95;format&#95;custom&#95;skip&#95;trailing&#95;empty&#95;lines](../../../operations/settings/settings-formats.md/#input_format_custom_skip_trailing_empty_lines)                   | omite las líneas vacías finales del archivo.                                                                                                                      | `false`        |
| [input&#95;format&#95;custom&#95;allow&#95;variable&#95;number&#95;of&#95;columns](../../../operations/settings/settings-formats.md/#input_format_custom_allow_variable_number_of_columns) | permite un número variable de columnas en el formato CustomSeparated, ignora las columnas adicionales y usa valores predeterminados para las columnas que faltan. | `false`        |