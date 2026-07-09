---
alias: []
description: 'Documentación del formato TemplateIgnoreSpaces'
input_format: true
keywords: ['TemplateIgnoreSpaces']
output_format: false
slug: /interfaces/formats/TemplateIgnoreSpaces
title: 'TemplateIgnoreSpaces'
doc_type: 'reference'
---

| Entrada | Salida | Alias |
| ------- | ------ | ----- |
| ✔       | ✗      |       |

<div id="description">
  ## Descripción
</div>

Similar a [`Template`], pero omite los caracteres de espacio en blanco entre los delimitadores y los valores en el flujo de entrada.
Sin embargo, si las cadenas de formato contienen caracteres de espacio en blanco, estos caracteres se esperarán en el flujo de entrada.
También permite especificar marcadores de posición vacíos (`${}` o `${:None}`) para dividir un delimitador en partes separadas e ignorar los espacios entre ellas.
Estos marcadores de posición se usan solo para omitir caracteres de espacio en blanco.
Es posible leer `JSON` con este formato si los valores de las columnas tienen el mismo orden en todas las filas.

:::note
Este formato es adecuado solo para la entrada.
:::

<div id="example-usage">
  ## Ejemplo de uso
</div>

La siguiente solicitud se puede usar para insertar datos a partir del ejemplo de salida en formato [JSON](/es/interfaces/formats/JSON):

```sql
INSERT INTO table_name 
SETTINGS
    format_template_resultset = '/some/path/resultset.format',
    format_template_row = '/some/path/row.format',
    format_template_rows_between_delimiter = ','
FORMAT TemplateIgnoreSpaces
```

```text title="/some/path/resultset.format"
{${}"meta"${}:${:JSON},${}"data"${}:${}[${data}]${},${}"totals"${}:${:JSON},${}"extremes"${}:${:JSON},${}"rows"${}:${:JSON},${}"rows_before_limit_at_least"${}:${:JSON}${}}
```

```text title="/some/path/row.format"
{${}"SearchPhrase"${}:${}${phrase:JSON}${},${}"c"${}:${}${cnt:JSON}${}}
```

<div id="format-settings">
  ## Configuración del formato
</div>
