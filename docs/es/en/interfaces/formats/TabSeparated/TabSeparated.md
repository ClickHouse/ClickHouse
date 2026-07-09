---
alias: ['TSV']
description: 'Documentación sobre el formato TSV'
input_format: true
keywords: ['TabSeparated', 'TSV']
output_format: true
slug: /interfaces/formats/TabSeparated
title: 'TabSeparated'
doc_type: 'reference'
---

| Entrada | Salida | Alias |
| ------- | ------ | ----- |
| ✔       | ✔      | `TSV` |

<div id="description">
  ## Descripción
</div>

En el formato TabSeparated, los datos se escriben por fila. Cada fila contiene valores separados por tabulaciones. Cada valor va seguido de una tabulación, excepto el último valor de la fila, que va seguido de un salto de línea. En todo momento se asume el uso estricto de saltos de línea Unix. La última fila también debe contener un salto de línea al final. Los valores se escriben en formato de texto, sin comillas envolventes y con los caracteres especiales escapados.

Este formato también está disponible con el nombre `TSV`.

El formato `TabSeparated` es práctico para procesar datos mediante programas y scripts personalizados. Se utiliza de forma predeterminada en la interfaz HTTP y en el modo por lotes del client de línea de comandos. Este formato también permite transferir datos entre distintos DBMS. Por ejemplo, puede obtener un dump de MySQL y cargarlo en ClickHouse, o viceversa.

El formato `TabSeparated` admite la salida de valores totales (cuando se usa WITH TOTALS) y valores extremos (cuando &#39;extremes&#39; se establece en 1). En estos casos, los valores totales y los extremos se muestran después de los datos principales. El resultado principal, los valores totales y los extremos se separan entre sí mediante una línea vacía. Ejemplo:

```sql
SELECT EventDate, count() AS c FROM test.hits GROUP BY EventDate WITH TOTALS ORDER BY EventDate FORMAT TabSeparated

2014-03-17      1406958
2014-03-18      1383658
2014-03-19      1405797
2014-03-20      1353623
2014-03-21      1245779
2014-03-22      1031592
2014-03-23      1046491

1970-01-01      8873898

2014-03-17      1031592
2014-03-23      1406958
```

<div id="tabseparated-data-formatting">
  ## Formateo de datos
</div>

Los números enteros se escriben en forma decimal. Los números pueden contener un carácter &quot;+&quot; adicional al principio (se ignora al analizarlos y no se registra al formatearlos). Los números no negativos no pueden contener el signo negativo. Al leer, se permite interpretar una cadena vacía como cero o (para los tipos con signo) una cadena compuesta solo por un signo menos como cero. Los números que no caben en el tipo de dato correspondiente pueden interpretarse como un número diferente, sin mostrar un mensaje de error.

Los números de coma flotante se escriben en forma decimal. El punto se utiliza como separador decimal. Se admiten entradas exponenciales, así como &#39;inf&#39;, &#39;+inf&#39;, &#39;-inf&#39; y &#39;nan&#39;. Una entrada de números de coma flotante puede empezar o terminar con un punto decimal.
Durante el formateo, puede perderse exactitud en los números de coma flotante.
Durante el análisis, no es estrictamente necesario leer el número representable por la máquina más cercano.

Las fechas se escriben en formato YYYY-MM-DD y se analizan con ese mismo formato, pero con cualquier carácter como separador.
Las fechas con hora se escriben en el formato `YYYY-MM-DD hh:mm:ss` y se analizan con ese mismo formato, pero con cualquier carácter como separador.
Todo esto ocurre en la zona horaria del sistema en el momento en que se inicia el client o el server (según cuál de ellos formatee los datos). Para las fechas con hora, no se especifica el horario de verano. Por lo tanto, si un dump contiene horas durante el horario de verano, el dump no coincide de forma inequívoca con los datos, y el análisis seleccionará una de las dos horas.
Durante una operación de lectura, las fechas incorrectas y las fechas con hora pueden analizarse con desbordamiento natural o como fechas y horas nulas, sin mostrar un mensaje de error.

Como excepción, también se admite analizar fechas con hora en formato Unix timestamp, si consta de exactamente 10 dígitos decimales. El resultado no depende de la zona horaria. Los formatos `YYYY-MM-DD hh:mm:ss` y `NNNNNNNNNN` se diferencian automáticamente.

Las Strings se generan con caracteres especiales escapados con barra invertida. Para la salida se utilizan las siguientes secuencias de escape: `\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\'`, `\\`. El análisis también admite las secuencias `\a`, `\v` y `\xHH` (secuencias de escape hexadecimales), así como cualquier secuencia `\c`, donde `c` es cualquier carácter (estas secuencias se convierten en `c`). Por lo tanto, la lectura de datos admite formatos en los que un salto de línea puede escribirse como `\n` o `\`, o como un salto de línea. Por ejemplo, la cadena `Hello world` con un salto de línea entre las palabras en lugar de un espacio puede analizarse en cualquiera de las siguientes variantes:

```text
Hello\nworld

Hello\
world
```

La segunda variante es compatible porque MySQL la usa al escribir dumps separados por tabulaciones.

El conjunto mínimo de caracteres que debe escapar al pasar datos en formato TabSeparated: tabulación, salto de línea (LF) y barra invertida.

Solo se escapa un pequeño conjunto de símbolos. Es fácil encontrarse con un valor de cadena que su terminal mostrará incorrectamente en la salida.

Los arrays se escriben como una lista de valores separados por comas entre `[]`. Los elementos numéricos del array se formatean de forma normal. Los tipos `Date` y `DateTime` se escriben entre comillas simples. Las cadenas se escriben entre comillas simples con las mismas reglas de escape indicadas anteriormente.

[NULL](/es/sql-reference/syntax.md) se formatea según el ajuste [format&#95;tsv&#95;null&#95;representation](/es/operations/settings/settings-formats.md/#format_tsv_null_representation) (el valor predeterminado es `\N`).

En los datos de entrada, los valores ENUM se pueden representar como nombres o como identificadores. Primero, intentamos hacer coincidir el valor de entrada con el nombre del ENUM. Si no lo logramos y el valor de entrada es un número, intentamos hacer coincidir ese número con el identificador del ENUM.
Si los datos de entrada contienen solo identificadores de ENUM, se recomienda habilitar el ajuste [input&#95;format&#95;tsv&#95;enum&#95;as&#95;number](/es/operations/settings/settings-formats.md/#input_format_tsv_enum_as_number) para optimizar el análisis de ENUM.

Cada elemento de las estructuras [Nested](/es/sql-reference/data-types/nested-data-structures/index.md) se representa como un array.

Por ejemplo:

```sql
CREATE TABLE nestedt
(
    `id` UInt8,
    `aux` Nested(
        a UInt8,
        b String
    )
)
ENGINE = TinyLog
```

```sql
INSERT INTO nestedt VALUES ( 1, [1], ['a'])
```

```sql
SELECT * FROM nestedt FORMAT TSV
```

```response
1  [1]    ['a']
```

<div id="example-usage">
  ## Ejemplo de uso
</div>

<div id="inserting-data">
  ### Inserción de datos
</div>

Utiliza el siguiente archivo TSV, llamado `football.tsv`:

```tsv
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

Inserta los datos:

```sql
INSERT INTO football FROM INFILE 'football.tsv' FORMAT TabSeparated;
```

<div id="reading-data">
  ### Leer datos
</div>

Lea los datos con el formato `TabSeparated`:

```sql
SELECT *
FROM football
FORMAT TabSeparated
```

La salida estará en un formato separado por tabuladores:

```tsv
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

<div id="format-settings">
  ## Configuración del formato
</div>

| Configuración                                                                                                                                            | Descripción                                                                                                                                                                                                                                                                                           | Predeterminado |
| -------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------- |
| [`format_tsv_null_representation`](/es/operations/settings/settings-formats.md/#format_tsv_null_representation)                                             | Representación personalizada de NULL en el formato TSV.                                                                                                                                                                                                                                               | `\N`           |
| [`input_format_tsv_empty_as_default`](/es/operations/settings/settings-formats.md/#input_format_tsv_empty_as_default)                                       | Trata los campos vacíos de la entrada TSV como valores predeterminados. Para expresiones predeterminadas complejas, también debe estar habilitado [input&#95;format&#95;defaults&#95;for&#95;omitted&#95;fields](/es/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields). | `false`        |
| [`input_format_tsv_enum_as_number`](/es/operations/settings/settings-formats.md/#input_format_tsv_enum_as_number)                                           | Trata los valores enum insertados en formatos TSV como índices de enum.                                                                                                                                                                                                                               | `false`        |
| [`input_format_tsv_use_best_effort_in_schema_inference`](/es/operations/settings/settings-formats.md/#input_format_tsv_use_best_effort_in_schema_inference) | Usa algunos ajustes y heurísticas para inferir el esquema en el formato TSV. Si está deshabilitado, todos los campos se inferirán como Strings.                                                                                                                                                       | `true`         |
| [`output_format_tsv_crlf_end_of_line`](/es/operations/settings/settings-formats.md/#output_format_tsv_crlf_end_of_line)                                     | Si se establece en true, el final de línea en el formato de salida TSV será `\r\n` en lugar de `\n`.                                                                                                                                                                                                  | `false`        |
| [`input_format_tsv_crlf_end_of_line`](/es/operations/settings/settings-formats.md/#input_format_tsv_crlf_end_of_line)                                       | Si se establece en true, el final de línea en el formato de entrada TSV será `\r\n` en lugar de `\n`.                                                                                                                                                                                                 | `false`        |
| [`input_format_tsv_skip_first_lines`](/es/operations/settings/settings-formats.md/#input_format_tsv_skip_first_lines)                                       | Omite el número especificado de líneas al principio de los datos.                                                                                                                                                                                                                                     | `0`            |
| [`input_format_tsv_detect_header`](/es/operations/settings/settings-formats.md/#input_format_tsv_detect_header)                                             | Detecta automáticamente el encabezado con nombres y types en el formato TSV.                                                                                                                                                                                                                          | `true`         |
| [`input_format_tsv_skip_trailing_empty_lines`](/es/operations/settings/settings-formats.md/#input_format_tsv_skip_trailing_empty_lines)                     | Omite las líneas vacías finales.                                                                                                                                                                                                                                                                      | `false`        |
| [`input_format_tsv_allow_variable_number_of_columns`](/es/operations/settings/settings-formats.md/#input_format_tsv_allow_variable_number_of_columns)       | Permite un número variable de columnas en el formato TSV, ignora las columnas adicionales y usa valores predeterminados para las columnas que faltan.                                                                                                                                                 | `false`        |