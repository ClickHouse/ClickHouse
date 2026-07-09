---
alias: []
description: 'Documentación sobre el formato Regexp'
input_format: true
keywords: ['Regexp']
output_format: false
slug: /interfaces/formats/Regexp
title: 'Regexp'
doc_type: 'reference'
---

| Entrada | Salida | Alias |
| ------- | ------ | ----- |
| ✔       | ✗      |       |

<div id="description">
  ## Descripción
</div>

El formato `Regex` analiza cada línea de los datos importados según la expresión regular proporcionada.

**Uso**

La expresión regular de la configuración [format&#95;regexp](/es/operations/settings/settings-formats.md/#format_regexp) se aplica a cada línea de los datos importados. El número de subpatrones de la expresión regular debe ser igual al número de columnas del conjunto de datos importado.

Las líneas de los datos importados deben estar separadas por el carácter de salto de línea `'\n'` o por el salto de línea de estilo DOS `"\r\n"`.

El contenido de cada subpatrón coincidente se analiza mediante el método del tipo de dato correspondiente, de acuerdo con la configuración [format&#95;regexp&#95;escaping&#95;rule](/es/operations/settings/settings-formats.md/#format_regexp_escaping_rule).

Si la expresión regular no coincide con la línea y [format&#95;regexp&#95;skip&#95;unmatched](/es/operations/settings/settings-formats.md/#format_regexp_escaping_rule) está configurado en 1, la línea se omite silenciosamente. En caso contrario, se genera una excepción.

<div id="example-usage">
  ## Ejemplo de uso
</div>

Considere el archivo `data.tsv`:

```text title="data.tsv"
id: 1 array: [1,2,3] string: str1 date: 2020-01-01
id: 2 array: [1,2,3] string: str2 date: 2020-01-02
id: 3 array: [1,2,3] string: str3 date: 2020-01-03
```

y la tabla `imp_regex_table`:

```sql title="Query"
CREATE TABLE imp_regex_table (id UInt32, array Array(UInt32), string String, date Date) ENGINE = Memory;
```

Insertaremos en la tabla anterior los datos del archivo mencionado anteriormente mediante la siguiente consulta:

```bash title="Query"
$ cat data.tsv | clickhouse-client  --query "INSERT INTO imp_regex_table SETTINGS format_regexp='id: (.+?) array: (.+?) string: (.+?) date: (.+?)', format_regexp_escaping_rule='Escaped', format_regexp_skip_unmatched=0 FORMAT Regexp;"
```

Ahora podemos hacer `SELECT` de los datos de la tabla para ver cómo el formato `Regex` analizó los datos del archivo:

```sql title="Query"
SELECT * FROM imp_regex_table;
```

```text title="Response"
┌─id─┬─array───┬─string─┬───────date─┐
│  1 │ [1,2,3] │ str1   │ 2020-01-01 │
│  2 │ [1,2,3] │ str2   │ 2020-01-02 │
│  3 │ [1,2,3] │ str3   │ 2020-01-03 │
└────┴─────────┴────────┴────────────┘
```

<div id="format-settings">
  ## Configuración del formato
</div>

Al trabajar con el formato `Regexp`, puede usar la siguiente configuración:

* `format_regexp` — [String](/es/sql-reference/data-types/string.md). Contiene una expresión regular en formato [re2](https://github.com/google/re2/wiki/Syntax).

* `format_regexp_escaping_rule` — [String](/es/sql-reference/data-types/string.md). Se admiten las siguientes reglas de escape:

  * CSV (de forma similar a [CSV](/es/interfaces/formats/CSV)
  * JSON (de forma similar a [JSONEachRow](/es/interfaces/formats/JSONEachRow)
  * Escaped (de forma similar a [TSV](/es/interfaces/formats/TabSeparated)
  * Quoted (de forma similar a [Values](/es/interfaces/formats/Values)
  * Raw (extrae los subpatrones completos, sin reglas de escape, de forma similar a [TSVRaw](/es/interfaces/formats/TabSeparated)

* `format_regexp_skip_unmatched` — [UInt8](/es/sql-reference/data-types/int-uint.md). Define si se debe lanzar una excepción en caso de que la expresión `format_regexp` no coincida con los datos importados. Puede establecerse en `0` o `1`.