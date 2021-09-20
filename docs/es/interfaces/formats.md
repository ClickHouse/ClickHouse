---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 21
toc_title: Formatos de entrada y salida
---

# Formatos para datos de entrada y salida {#formats}

ClickHouse puede aceptar y devolver datos en varios formatos. Se puede utilizar un formato admitido para la entrada para analizar los datos proporcionados a `INSERT`s, para llevar a cabo `SELECT`s de una tabla respaldada por archivos como File, URL o HDFS, o para leer un diccionario externo. Se puede utilizar un formato compatible con la salida para organizar el
resultados de un `SELECT`, y realizar `INSERT`s en una tabla respaldada por archivos.

Los formatos soportados son:

| Formato                                                         | Entrada | Salida |
|-----------------------------------------------------------------|---------|--------|
| [TabSeparated](#tabseparated)                                   | ✔       | ✔      |
| [TabSeparatedRaw](#tabseparatedraw)                             | ✗       | ✔      |
| [TabSeparatedWithNames](#tabseparatedwithnames)                 | ✔       | ✔      |
| [TabSeparatedWithNamesAndTypes](#tabseparatedwithnamesandtypes) | ✔       | ✔      |
| [Plantilla](#format-template)                                   | ✔       | ✔      |
| [TemplateIgnoreSpaces](#templateignorespaces)                   | ✔       | ✗      |
| [CSV](#csv)                                                     | ✔       | ✔      |
| [CSVWithNames](#csvwithnames)                                   | ✔       | ✔      |
| [CustomSeparated](#format-customseparated)                      | ✔       | ✔      |
| [Valor](#data-format-values)                                    | ✔       | ✔      |
| [Vertical](#vertical)                                           | ✗       | ✔      |
| [VerticalRaw](#verticalraw)                                     | ✗       | ✔      |
| [JSON](#json)                                                   | ✗       | ✔      |
| [JSONCompact](#jsoncompact)                                     | ✗       | ✔      |
| [JSONEachRow](#jsoneachrow)                                     | ✔       | ✔      |
| [TSKV](#tskv)                                                   | ✔       | ✔      |
| [Bastante](#pretty)                                             | ✗       | ✔      |
| [PrettyCompact](#prettycompact)                                 | ✗       | ✔      |
| [PrettyCompactMonoBlock](#prettycompactmonoblock)               | ✗       | ✔      |
| [PrettyNoEscapes](#prettynoescapes)                             | ✗       | ✔      |
| [Bienvenido a WordPress.](#prettyspace)                         | ✗       | ✔      |
| [Protobuf](#protobuf)                                           | ✔       | ✔      |
| [Avro](#data-format-avro)                                       | ✔       | ✔      |
| [AvroConfluent](#data-format-avro-confluent)                    | ✔       | ✗      |
| [Parquet](#data-format-parquet)                                 | ✔       | ✔      |
| [ORC](#data-format-orc)                                         | ✔       | ✗      |
| [RowBinary](#rowbinary)                                         | ✔       | ✔      |
| [RowBinaryWithNamesAndTypes](#rowbinarywithnamesandtypes)       | ✔       | ✔      |
| [Nativo](#native)                                               | ✔       | ✔      |
| [Nulo](#null)                                                   | ✗       | ✔      |
| [XML](#xml)                                                     | ✗       | ✔      |
| [CapnProto](#capnproto)                                         | ✔       | ✗      |

Puede controlar algunos parámetros de procesamiento de formato con la configuración de ClickHouse. Para obtener más información, lea el [Configuración](../operations/settings/settings.md) apartado.

## TabSeparated {#tabseparated}

En el formato TabSeparated, los datos se escriben por fila. Cada fila contiene valores separados por pestañas. Cada valor es seguido por una ficha, excepto el último valor de la fila, que es seguido por un avance de línea. Estrictamente las fuentes de línea Unix se asumen en todas partes. La última fila también debe contener un avance de línea al final. Los valores se escriben en formato de texto, sin incluir comillas y con caracteres especiales escapados.

Este formato también está disponible bajo el nombre `TSV`.

El `TabSeparated` es conveniente para procesar datos utilizando programas y scripts personalizados. Se usa de forma predeterminada en la interfaz HTTP y en el modo por lotes del cliente de línea de comandos. Este formato también permite transferir datos entre diferentes DBMS. Por ejemplo, puede obtener un volcado de MySQL y subirlo a ClickHouse, o viceversa.

El `TabSeparated` el formato admite la salida de valores totales (cuando se usa WITH TOTALS) y valores extremos (cuando ‘extremes’ se establece en 1). En estos casos, los valores totales y los extremos se emiten después de los datos principales. El resultado principal, los valores totales y los extremos están separados entre sí por una línea vacía. Ejemplo:

``` sql
SELECT EventDate, count() AS c FROM test.hits GROUP BY EventDate WITH TOTALS ORDER BY EventDate FORMAT TabSeparated``
```

``` text
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

### Formato de datos {#data-formatting}

Los números enteros se escriben en forma decimal. Los números pueden contener un extra “+” carácter al principio (ignorado al analizar y no grabado al formatear). Los números no negativos no pueden contener el signo negativo. Al leer, se permite analizar una cadena vacía como cero, o (para tipos con signo) una cadena que consiste en solo un signo menos como cero. Los números que no encajan en el tipo de datos correspondiente se pueden analizar como un número diferente, sin un mensaje de error.

Los números de punto flotante se escriben en forma decimal. El punto se usa como separador decimal. Las entradas exponenciales son compatibles, al igual que ‘inf’, ‘+inf’, ‘-inf’, y ‘nan’. Una entrada de números de coma flotante puede comenzar o terminar con un punto decimal.
Durante el formateo, la precisión puede perderse en los números de coma flotante.
Durante el análisis, no es estrictamente necesario leer el número representable de la máquina más cercano.

Las fechas se escriben en formato AAAA-MM-DD y se analizan en el mismo formato, pero con los caracteres como separadores.
Las fechas con horas se escriben en el formato `YYYY-MM-DD hh:mm:ss` y analizado en el mismo formato, pero con cualquier carácter como separadores.
Todo esto ocurre en la zona horaria del sistema en el momento en que se inicia el cliente o servidor (dependiendo de cuál de ellos formatea los datos). Para fechas con horarios, no se especifica el horario de verano. Por lo tanto, si un volcado tiene tiempos durante el horario de verano, el volcado no coincide inequívocamente con los datos, y el análisis seleccionará una de las dos veces.
Durante una operación de lectura, las fechas incorrectas y las fechas con horas se pueden analizar con desbordamiento natural o como fechas y horas nulas, sin un mensaje de error.

Como excepción, el análisis de fechas con horas también se admite en el formato de marca de tiempo Unix, si consta de exactamente 10 dígitos decimales. El resultado no depende de la zona horaria. Los formatos AAAA-MM-DD hh:mm:ss y NNNNNNNNNN se diferencian automáticamente.

Las cadenas se generan con caracteres especiales de escape de barra invertida. Las siguientes secuencias de escape se utilizan para la salida: `\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\'`, `\\`. El análisis también admite las secuencias `\a`, `\v`, y `\xHH` (secuencias de escape hexagonales) y cualquier `\c` secuencias, donde `c` es cualquier carácter (estas secuencias se convierten en `c`). Por lo tanto, la lectura de datos admite formatos donde un avance de línea se puede escribir como `\n` o `\` o como un avance de línea. Por ejemplo, la cadena `Hello world` con un avance de línea entre las palabras en lugar de espacio se puede analizar en cualquiera de las siguientes variaciones:

``` text
Hello\nworld

Hello\
world
```

La segunda variante es compatible porque MySQL la usa al escribir volcados separados por tabuladores.

El conjunto mínimo de caracteres que debe escapar al pasar datos en formato TabSeparated: tabulación, salto de línea (LF) y barra invertida.

Solo se escapa un pequeño conjunto de símbolos. Puede tropezar fácilmente con un valor de cadena que su terminal arruinará en la salida.

Las matrices se escriben como una lista de valores separados por comas entre corchetes. Los elementos numéricos de la matriz tienen el formato normal. `Date` y `DateTime` están escritos entre comillas simples. Las cadenas se escriben entre comillas simples con las mismas reglas de escape que las anteriores.

[NULL](../sql-reference/syntax.md) se formatea como `\N`.

Cada elemento de [Anidar](../sql-reference/data-types/nested-data-structures/nested.md) estructuras se representa como una matriz.

Por ejemplo:

``` sql
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

``` sql
INSERT INTO nestedt Values ( 1, [1], ['a'])
```

``` sql
SELECT * FROM nestedt FORMAT TSV
```

``` text
1  [1]    ['a']
```

## TabSeparatedRaw {#tabseparatedraw}

Difiere de `TabSeparated` formato en que las filas se escriben sin escapar.
Este formato solo es apropiado para generar un resultado de consulta, pero no para analizar (recuperar datos para insertar en una tabla).

Este formato también está disponible bajo el nombre `TSVRaw`.

## TabSeparatedWithNames {#tabseparatedwithnames}

Difiere de la `TabSeparated` formato en que los nombres de columna se escriben en la primera fila.
Durante el análisis, la primera fila se ignora por completo. No puede usar nombres de columna para determinar su posición o para comprobar su corrección.
(Se puede agregar soporte para analizar la fila de encabezado en el futuro.)

Este formato también está disponible bajo el nombre `TSVWithNames`.

## TabSeparatedWithNamesAndTypes {#tabseparatedwithnamesandtypes}

Difiere de la `TabSeparated` formato en que los nombres de columna se escriben en la primera fila, mientras que los tipos de columna están en la segunda fila.
Durante el análisis, la primera y la segunda filas se ignoran por completo.

Este formato también está disponible bajo el nombre `TSVWithNamesAndTypes`.

## Plantilla {#format-template}

Este formato permite especificar una cadena de formato personalizado con marcadores de posición para los valores con una regla de escape especificada.

Utiliza la configuración `format_template_resultset`, `format_template_row`, `format_template_rows_between_delimiter` and some settings of other formats (e.g. `output_format_json_quote_64bit_integers` cuando se utiliza `JSON` escapar, ver más)

Configuración `format_template_row` especifica la ruta de acceso al archivo, que contiene una cadena de formato para las filas con la siguiente sintaxis:

`delimiter_1${column_1:serializeAs_1}delimiter_2${column_2:serializeAs_2} ... delimiter_N`,

donde `delimiter_i` es un delimitador entre valores (`$` símbolo se puede escapar como `$$`),
`column_i` es un nombre o índice de una columna cuyos valores se deben seleccionar o insertar (si está vacío, se omitirá la columna),
`serializeAs_i` es una regla de escape para los valores de columna. Se admiten las siguientes reglas de escape:

-   `CSV`, `JSON`, `XML` (similar a los formatos de los mismos nombres)
-   `Escaped` (similar a `TSV`)
-   `Quoted` (similar a `Values`)
-   `Raw` (sin escapar, de manera similar a `TSVRaw`)
-   `None` (sin regla de escape, ver más)

Si se omite una regla de escape, entonces `None` se utilizará. `XML` y `Raw` son adecuados sólo para la salida.

Entonces, para la siguiente cadena de formato:

      `Search phrase: ${SearchPhrase:Quoted}, count: ${c:Escaped}, ad price: $$${price:JSON};`

los valores de `SearchPhrase`, `c` y `price` columnas, que se escapan como `Quoted`, `Escaped` y `JSON` se imprimirá (para seleccionar) o se esperará (para insertar) entre `Search phrase:`, `, count:`, `, ad price: $` y `;` delimitadores respectivamente. Por ejemplo:

`Search phrase: 'bathroom interior design', count: 2166, ad price: $3;`

El `format_template_rows_between_delimiter` setting especifica el delimitador entre filas, que se imprime (o se espera) después de cada fila, excepto la última (`\n` predeterminada)

Configuración `format_template_resultset` especifica la ruta al archivo, que contiene una cadena de formato para el conjunto de resultados. La cadena de formato para el conjunto de resultados tiene la misma sintaxis que una cadena de formato para la fila y permite especificar un prefijo, un sufijo y una forma de imprimir información adicional. Contiene los siguientes marcadores de posición en lugar de nombres de columna:

-   `data` son las filas con datos en `format_template_row` formato, separados por `format_template_rows_between_delimiter`. Este marcador de posición debe ser el primer marcador de posición en la cadena de formato.
-   `totals` es la fila con valores totales en `format_template_row` formato (cuando se usa WITH TOTALS)
-   `min` es la fila con valores mínimos en `format_template_row` formato (cuando los extremos se establecen en 1)
-   `max` es la fila con valores máximos en `format_template_row` formato (cuando los extremos se establecen en 1)
-   `rows` es el número total de filas de salida
-   `rows_before_limit` es el número mínimo de filas que habría habido sin LIMIT. Salida solo si la consulta contiene LIMIT. Si la consulta contiene GROUP BY, rows_before_limit_at_least es el número exacto de filas que habría habido sin un LIMIT .
-   `time` es el tiempo de ejecución de la solicitud en segundos
-   `rows_read` es el número de filas que se ha leído
-   `bytes_read` es el número de bytes (sin comprimir) que se ha leído

Marcador `data`, `totals`, `min` y `max` no debe tener una regla de escape especificada (o `None` debe especificarse explícitamente). Los marcadores de posición restantes pueden tener cualquier regla de escape especificada.
Si el `format_template_resultset` valor es una cadena vacía, `${data}` se utiliza como valor predeterminado.
Para el formato de consultas de inserción permite omitir algunas columnas o algunos campos si prefijo o sufijo (ver ejemplo).

Seleccionar ejemplo:

``` sql
SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase ORDER BY c DESC LIMIT 5 FORMAT Template SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format', format_template_rows_between_delimiter = '\n    '
```

`/some/path/resultset.format`:

``` text
<!DOCTYPE HTML>
<html> <head> <title>Search phrases</title> </head>
 <body>
  <table border="1"> <caption>Search phrases</caption>
    <tr> <th>Search phrase</th> <th>Count</th> </tr>
    ${data}
  </table>
  <table border="1"> <caption>Max</caption>
    ${max}
  </table>
  <b>Processed ${rows_read:XML} rows in ${time:XML} sec</b>
 </body>
</html>
```

`/some/path/row.format`:

``` text
<tr> <td>${0:XML}</td> <td>${1:XML}</td> </tr>
```

Resultado:

``` html
<!DOCTYPE HTML>
<html> <head> <title>Search phrases</title> </head>
 <body>
  <table border="1"> <caption>Search phrases</caption>
    <tr> <th>Search phrase</th> <th>Count</th> </tr>
    <tr> <td></td> <td>8267016</td> </tr>
    <tr> <td>bathroom interior design</td> <td>2166</td> </tr>
    <tr> <td>yandex</td> <td>1655</td> </tr>
    <tr> <td>spring 2014 fashion</td> <td>1549</td> </tr>
    <tr> <td>freeform photos</td> <td>1480</td> </tr>
  </table>
  <table border="1"> <caption>Max</caption>
    <tr> <td></td> <td>8873898</td> </tr>
  </table>
  <b>Processed 3095973 rows in 0.1569913 sec</b>
 </body>
</html>
```

Insertar ejemplo:

``` text
Some header
Page views: 5, User id: 4324182021466249494, Useless field: hello, Duration: 146, Sign: -1
Page views: 6, User id: 4324182021466249494, Useless field: world, Duration: 185, Sign: 1
Total rows: 2
```

``` sql
INSERT INTO UserActivity FORMAT Template SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format'
```

`/some/path/resultset.format`:

``` text
Some header\n${data}\nTotal rows: ${:CSV}\n
```

`/some/path/row.format`:

``` text
Page views: ${PageViews:CSV}, User id: ${UserID:CSV}, Useless field: ${:CSV}, Duration: ${Duration:CSV}, Sign: ${Sign:CSV}
```

`PageViews`, `UserID`, `Duration` y `Sign` dentro de los marcadores de posición son nombres de columnas en la tabla. Valores después `Useless field` en filas y después `\nTotal rows:` en el sufijo será ignorado.
Todos los delimitadores de los datos de entrada deben ser estrictamente iguales a los delimitadores de las cadenas de formato especificadas.

## TemplateIgnoreSpaces {#templateignorespaces}

Este formato es adecuado sólo para la entrada.
Similar a `Template`, pero omite caracteres de espacio en blanco entre delimitadores y valores en la secuencia de entrada. Sin embargo, si las cadenas de formato contienen caracteres de espacio en blanco, se esperarán estos caracteres en la secuencia de entrada. También permite especificar marcadores de posición vacíos (`${}` o `${:None}`) para dividir algún delimitador en partes separadas para ignorar los espacios entre ellos. Dichos marcadores de posición se usan solo para omitir caracteres de espacio en blanco.
Es posible leer `JSON` usando este formato, si los valores de las columnas tienen el mismo orden en todas las filas. Por ejemplo, la siguiente solicitud se puede utilizar para insertar datos del ejemplo de salida de formato [JSON](#json):

``` sql
INSERT INTO table_name FORMAT TemplateIgnoreSpaces SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format', format_template_rows_between_delimiter = ','
```

`/some/path/resultset.format`:

``` text
{${}"meta"${}:${:JSON},${}"data"${}:${}[${data}]${},${}"totals"${}:${:JSON},${}"extremes"${}:${:JSON},${}"rows"${}:${:JSON},${}"rows_before_limit_at_least"${}:${:JSON}${}}
```

`/some/path/row.format`:

``` text
{${}"SearchPhrase"${}:${}${phrase:JSON}${},${}"c"${}:${}${cnt:JSON}${}}
```

## TSKV {#tskv}

Similar a TabSeparated , pero genera un valor en formato name=value . Los nombres se escapan de la misma manera que en el formato TabSeparated, y el símbolo = también se escapa.

``` text
SearchPhrase=   count()=8267016
SearchPhrase=bathroom interior design    count()=2166
SearchPhrase=yandex     count()=1655
SearchPhrase=2014 spring fashion    count()=1549
SearchPhrase=freeform photos       count()=1480
SearchPhrase=angelina jolie    count()=1245
SearchPhrase=omsk       count()=1112
SearchPhrase=photos of dog breeds    count()=1091
SearchPhrase=curtain designs        count()=1064
SearchPhrase=baku       count()=1000
```

[NULL](../sql-reference/syntax.md) se formatea como `\N`.

``` sql
SELECT * FROM t_null FORMAT TSKV
```

``` text
x=1    y=\N
```

Cuando hay una gran cantidad de columnas pequeñas, este formato no es efectivo y generalmente no hay razón para usarlo. Sin embargo, no es peor que JSONEachRow en términos de eficiencia.

Both data output and parsing are supported in this format. For parsing, any order is supported for the values of different columns. It is acceptable for some values to be omitted – they are treated as equal to their default values. In this case, zeros and blank rows are used as default values. Complex values that could be specified in the table are not supported as defaults.

El análisis permite la presencia del campo adicional `tskv` sin el signo igual o un valor. Este campo se ignora.

## CSV {#csv}

Formato de valores separados por comas ([RFC](https://tools.ietf.org/html/rfc4180)).

Al formatear, las filas están encerradas en comillas dobles. Una comilla doble dentro de una cadena se genera como dos comillas dobles en una fila. No hay otras reglas para escapar de los personajes. Fecha y fecha-hora están encerrados en comillas dobles. Los números se emiten sin comillas. Los valores están separados por un carácter delimitador, que es `,` predeterminada. El carácter delimitador se define en la configuración [Formato_csv_delimiter](../operations/settings/settings.md#settings-format_csv_delimiter). Las filas se separan usando el avance de línea Unix (LF). Las matrices se serializan en CSV de la siguiente manera: primero, la matriz se serializa en una cadena como en el formato TabSeparated, y luego la cadena resultante se envía a CSV en comillas dobles. Las tuplas en formato CSV se serializan como columnas separadas (es decir, se pierde su anidamiento en la tupla).

``` bash
$ clickhouse-client --format_csv_delimiter="|" --query="INSERT INTO test.csv FORMAT CSV" < data.csv
```

\*De forma predeterminada, el delimitador es `,`. Ver el [Formato_csv_delimiter](../operations/settings/settings.md#settings-format_csv_delimiter) para obtener más información.

Al analizar, todos los valores se pueden analizar con o sin comillas. Ambas comillas dobles y simples son compatibles. Las filas también se pueden organizar sin comillas. En este caso, se analizan hasta el carácter delimitador o el avance de línea (CR o LF). En violación del RFC, al analizar filas sin comillas, se ignoran los espacios y pestañas iniciales y finales. Para el avance de línea, se admiten los tipos Unix (LF), Windows (CR LF) y Mac OS Classic (CR LF).

Los valores de entrada vacíos sin comillas se sustituyen por valores predeterminados para las columnas respectivas, si
[Entrada_format_defaults_for_omitted_fields](../operations/settings/settings.md#session_settings-input_format_defaults_for_omitted_fields)
está habilitado.

`NULL` se formatea como `\N` o `NULL` o una cadena vacía sin comillas (consulte la configuración [input_format_csv_unquoted_null_literal_as_null](../operations/settings/settings.md#settings-input_format_csv_unquoted_null_literal_as_null) y [Entrada_format_defaults_for_omitted_fields](../operations/settings/settings.md#session_settings-input_format_defaults_for_omitted_fields)).

El formato CSV admite la salida de totales y extremos de la misma manera que `TabSeparated`.

## CSVWithNames {#csvwithnames}

También imprime la fila del encabezado, similar a `TabSeparatedWithNames`.

## CustomSeparated {#format-customseparated}

Similar a [Plantilla](#format-template), pero imprime o lee todas las columnas y usa la regla de escape de la configuración `format_custom_escaping_rule` y delimitadores desde la configuración `format_custom_field_delimiter`, `format_custom_row_before_delimiter`, `format_custom_row_after_delimiter`, `format_custom_row_between_delimiter`, `format_custom_result_before_delimiter` y `format_custom_result_after_delimiter`, no de cadenas de formato.
También hay `CustomSeparatedIgnoreSpaces` formato, que es similar a `TemplateIgnoreSpaces`.

## JSON {#json}

Salida de datos en formato JSON. Además de las tablas de datos, también genera nombres y tipos de columnas, junto con información adicional: el número total de filas de salida y el número de filas que podrían haberse generado si no hubiera un LIMIT . Ejemplo:

``` sql
SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase WITH TOTALS ORDER BY c DESC LIMIT 5 FORMAT JSON
```

``` json
{
        "meta":
        [
                {
                        "name": "SearchPhrase",
                        "type": "String"
                },
                {
                        "name": "c",
                        "type": "UInt64"
                }
        ],

        "data":
        [
                {
                        "SearchPhrase": "",
                        "c": "8267016"
                },
                {
                        "SearchPhrase": "bathroom interior design",
                        "c": "2166"
                },
                {
                        "SearchPhrase": "yandex",
                        "c": "1655"
                },
                {
                        "SearchPhrase": "spring 2014 fashion",
                        "c": "1549"
                },
                {
                        "SearchPhrase": "freeform photos",
                        "c": "1480"
                }
        ],

        "totals":
        {
                "SearchPhrase": "",
                "c": "8873898"
        },

        "extremes":
        {
                "min":
                {
                        "SearchPhrase": "",
                        "c": "1480"
                },
                "max":
                {
                        "SearchPhrase": "",
                        "c": "8267016"
                }
        },

        "rows": 5,

        "rows_before_limit_at_least": 141137
}
```

El JSON es compatible con JavaScript. Para garantizar esto, algunos caracteres se escapan adicionalmente: la barra inclinada `/` se escapa como `\/`; saltos de línea alternativos `U+2028` y `U+2029`, que rompen algunos navegadores, se escapan como `\uXXXX`. Los caracteres de control ASCII se escapan: retroceso, avance de formulario, avance de línea, retorno de carro y tabulación horizontal se reemplazan con `\b`, `\f`, `\n`, `\r`, `\t` , así como los bytes restantes en el rango 00-1F usando `\uXXXX` sequences. Invalid UTF-8 sequences are changed to the replacement character � so the output text will consist of valid UTF-8 sequences. For compatibility with JavaScript, Int64 and UInt64 integers are enclosed in double-quotes by default. To remove the quotes, you can set the configuration parameter [output_format_json_quote_64bit_integers](../operations/settings/settings.md#session_settings-output_format_json_quote_64bit_integers) a 0.

`rows` – The total number of output rows.

`rows_before_limit_at_least` El número mínimo de filas habría sido sin LIMIT . Salida solo si la consulta contiene LIMIT.
Si la consulta contiene GROUP BY, rows_before_limit_at_least es el número exacto de filas que habría habido sin un LIMIT .

`totals` – Total values (when using WITH TOTALS).

`extremes` – Extreme values (when extremes are set to 1).

Este formato solo es apropiado para generar un resultado de consulta, pero no para analizar (recuperar datos para insertar en una tabla).

Soporta ClickHouse [NULL](../sql-reference/syntax.md), que se muestra como `null` en la salida JSON.

Ver también el [JSONEachRow](#jsoneachrow) formato.

## JSONCompact {#jsoncompact}

Difiere de JSON solo en que las filas de datos se generan en matrices, no en objetos.

Ejemplo:

``` json
{
        "meta":
        [
                {
                        "name": "SearchPhrase",
                        "type": "String"
                },
                {
                        "name": "c",
                        "type": "UInt64"
                }
        ],

        "data":
        [
                ["", "8267016"],
                ["bathroom interior design", "2166"],
                ["yandex", "1655"],
                ["fashion trends spring 2014", "1549"],
                ["freeform photo", "1480"]
        ],

        "totals": ["","8873898"],

        "extremes":
        {
                "min": ["","1480"],
                "max": ["","8267016"]
        },

        "rows": 5,

        "rows_before_limit_at_least": 141137
}
```

Este formato solo es apropiado para generar un resultado de consulta, pero no para analizar (recuperar datos para insertar en una tabla).
Ver también el `JSONEachRow` formato.

## JSONEachRow {#jsoneachrow}

Al usar este formato, ClickHouse genera filas como objetos JSON separados, delimitados por nuevas líneas, pero los datos en su conjunto no son JSON válidos.

``` json
{"SearchPhrase":"curtain designs","count()":"1064"}
{"SearchPhrase":"baku","count()":"1000"}
{"SearchPhrase":"","count()":"8267016"}
```

Al insertar los datos, debe proporcionar un objeto JSON independiente para cada fila.

### Insertar datos {#inserting-data}

``` sql
INSERT INTO UserActivity FORMAT JSONEachRow {"PageViews":5, "UserID":"4324182021466249494", "Duration":146,"Sign":-1} {"UserID":"4324182021466249494","PageViews":6,"Duration":185,"Sign":1}
```

ClickHouse permite:

-   Cualquier orden de pares clave-valor en el objeto.
-   Omitiendo algunos valores.

ClickHouse ignora los espacios entre los elementos y las comas después de los objetos. Puede pasar todos los objetos en una línea. No tiene que separarlos con saltos de línea.

**Procesamiento de valores omitidos**

ClickHouse sustituye los valores omitidos por los valores predeterminados para el [tipos de datos](../sql-reference/data-types/index.md).

Si `DEFAULT expr` se especifica, ClickHouse utiliza diferentes reglas de sustitución dependiendo de la [Entrada_format_defaults_for_omitted_fields](../operations/settings/settings.md#session_settings-input_format_defaults_for_omitted_fields) configuración.

Considere la siguiente tabla:

``` sql
CREATE TABLE IF NOT EXISTS example_table
(
    x UInt32,
    a DEFAULT x * 2
) ENGINE = Memory;
```

-   Si `input_format_defaults_for_omitted_fields = 0`, entonces el valor predeterminado para `x` y `a` igual `0` (como el valor predeterminado para el `UInt32` tipo de datos).
-   Si `input_format_defaults_for_omitted_fields = 1`, entonces el valor predeterminado para `x` igual `0` pero el valor predeterminado de `a` igual `x * 2`.

!!! note "Advertencia"
    Al insertar datos con `insert_sample_with_metadata = 1`, ClickHouse consume más recursos computacionales, en comparación con la inserción con `insert_sample_with_metadata = 0`.

### Selección de datos {#selecting-data}

Considere el `UserActivity` tabla como un ejemplo:

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

Consulta `SELECT * FROM UserActivity FORMAT JSONEachRow` devoluciones:

``` text
{"UserID":"4324182021466249494","PageViews":5,"Duration":146,"Sign":-1}
{"UserID":"4324182021466249494","PageViews":6,"Duration":185,"Sign":1}
```

A diferencia de la [JSON](#json) formato, no hay sustitución de secuencias UTF-8 no válidas. Los valores se escapan de la misma manera que para `JSON`.

!!! note "Nota"
    Cualquier conjunto de bytes se puede generar en las cadenas. Utilice el `JSONEachRow` si está seguro de que los datos de la tabla se pueden formatear como JSON sin perder ninguna información.

### Uso de estructuras anidadas {#jsoneachrow-nested}

Si tienes una mesa con [Anidar](../sql-reference/data-types/nested-data-structures/nested.md) columnas de tipo de datos, puede insertar datos JSON con la misma estructura. Habilite esta función con el [Entrada_format_import_nested_json](../operations/settings/settings.md#settings-input_format_import_nested_json) configuración.

Por ejemplo, considere la siguiente tabla:

``` sql
CREATE TABLE json_each_row_nested (n Nested (s String, i Int32) ) ENGINE = Memory
```

Como se puede ver en el `Nested` descripción del tipo de datos, ClickHouse trata cada componente de la estructura anidada como una columna separada (`n.s` y `n.i` para nuestra mesa). Puede insertar datos de la siguiente manera:

``` sql
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n.s": ["abc", "def"], "n.i": [1, 23]}
```

Para insertar datos como un objeto JSON jerárquico, establezca [input_format_import_nested_json=1](../operations/settings/settings.md#settings-input_format_import_nested_json).

``` json
{
    "n": {
        "s": ["abc", "def"],
        "i": [1, 23]
    }
}
```

Sin esta configuración, ClickHouse produce una excepción.

``` sql
SELECT name, value FROM system.settings WHERE name = 'input_format_import_nested_json'
```

``` text
┌─name────────────────────────────┬─value─┐
│ input_format_import_nested_json │ 0     │
└─────────────────────────────────┴───────┘
```

``` sql
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n": {"s": ["abc", "def"], "i": [1, 23]}}
```

``` text
Code: 117. DB::Exception: Unknown field found while parsing JSONEachRow format: n: (at row 1)
```

``` sql
SET input_format_import_nested_json=1
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n": {"s": ["abc", "def"], "i": [1, 23]}}
SELECT * FROM json_each_row_nested
```

``` text
┌─n.s───────────┬─n.i────┐
│ ['abc','def'] │ [1,23] │
└───────────────┴────────┘
```

## Nativo {#native}

El formato más eficiente. Los datos son escritos y leídos por bloques en formato binario. Para cada bloque, el número de filas, número de columnas, nombres y tipos de columnas y partes de columnas de este bloque se registran una tras otra. En otras palabras, este formato es “columnar” – it doesn't convert columns to rows. This is the format used in the native interface for interaction between servers, for using the command-line client, and for C++ clients.

Puede utilizar este formato para generar rápidamente volcados que sólo pueden ser leídos por el DBMS de ClickHouse. No tiene sentido trabajar con este formato usted mismo.

## Nulo {#null}

Nada es salida. Sin embargo, la consulta se procesa y, cuando se utiliza el cliente de línea de comandos, los datos se transmiten al cliente. Esto se usa para pruebas, incluidas las pruebas de rendimiento.
Obviamente, este formato solo es apropiado para la salida, no para el análisis.

## Bastante {#pretty}

Salidas de datos como tablas de arte Unicode, también utilizando secuencias de escape ANSI para establecer colores en el terminal.
Se dibuja una cuadrícula completa de la tabla, y cada fila ocupa dos líneas en la terminal.
Cada bloque de resultados se muestra como una tabla separada. Esto es necesario para que los bloques se puedan generar sin resultados de almacenamiento en búfer (el almacenamiento en búfer sería necesario para calcular previamente el ancho visible de todos los valores).

[NULL](../sql-reference/syntax.md) se emite como `ᴺᵁᴸᴸ`.

Ejemplo (mostrado para el [PrettyCompact](#prettycompact) formato):

``` sql
SELECT * FROM t_null
```

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

Las filas no se escapan en formatos Pretty \*. Se muestra un ejemplo para el [PrettyCompact](#prettycompact) formato:

``` sql
SELECT 'String with \'quotes\' and \t character' AS Escaping_test
```

``` text
┌─Escaping_test────────────────────────┐
│ String with 'quotes' and      character │
└──────────────────────────────────────┘
```

Para evitar volcar demasiados datos al terminal, solo se imprimen las primeras 10.000 filas. Si el número de filas es mayor o igual que 10.000, el mensaje “Showed first 10 000” se imprime.
Este formato solo es apropiado para generar un resultado de consulta, pero no para analizar (recuperar datos para insertar en una tabla).

El formato Pretty admite la salida de valores totales (cuando se usa WITH TOTALS) y extremos (cuando ‘extremes’ se establece en 1). En estos casos, los valores totales y los valores extremos se generan después de los datos principales, en tablas separadas. Ejemplo (mostrado para el [PrettyCompact](#prettycompact) formato):

``` sql
SELECT EventDate, count() AS c FROM test.hits GROUP BY EventDate WITH TOTALS ORDER BY EventDate FORMAT PrettyCompact
```

``` text
┌──EventDate─┬───────c─┐
│ 2014-03-17 │ 1406958 │
│ 2014-03-18 │ 1383658 │
│ 2014-03-19 │ 1405797 │
│ 2014-03-20 │ 1353623 │
│ 2014-03-21 │ 1245779 │
│ 2014-03-22 │ 1031592 │
│ 2014-03-23 │ 1046491 │
└────────────┴─────────┘

Totals:
┌──EventDate─┬───────c─┐
│ 1970-01-01 │ 8873898 │
└────────────┴─────────┘

Extremes:
┌──EventDate─┬───────c─┐
│ 2014-03-17 │ 1031592 │
│ 2014-03-23 │ 1406958 │
└────────────┴─────────┘
```

## PrettyCompact {#prettycompact}

Difiere de [Bastante](#pretty) en que la cuadrícula se dibuja entre filas y el resultado es más compacto.
Este formato se usa de forma predeterminada en el cliente de línea de comandos en modo interactivo.

## PrettyCompactMonoBlock {#prettycompactmonoblock}

Difiere de [PrettyCompact](#prettycompact) en que hasta 10,000 filas se almacenan en búfer, luego se salen como una sola tabla, no por bloques.

## PrettyNoEscapes {#prettynoescapes}

Difiere de Pretty en que las secuencias de escape ANSI no se usan. Esto es necesario para mostrar este formato en un navegador, así como para usar el ‘watch’ utilidad de línea de comandos.

Ejemplo:

``` bash
$ watch -n1 "clickhouse-client --query='SELECT event, value FROM system.events FORMAT PrettyCompactNoEscapes'"
```

Puede usar la interfaz HTTP para mostrar en el navegador.

### PrettyCompactNoEscapes {#prettycompactnoescapes}

Lo mismo que el ajuste anterior.

### PrettySpaceNoEscapes {#prettyspacenoescapes}

Lo mismo que el ajuste anterior.

## Bienvenido a WordPress {#prettyspace}

Difiere de [PrettyCompact](#prettycompact) en ese espacio en blanco (caracteres de espacio) se usa en lugar de la cuadrícula.

## RowBinary {#rowbinary}

Formatea y analiza datos por fila en formato binario. Las filas y los valores se enumeran consecutivamente, sin separadores.
Este formato es menos eficiente que el formato nativo, ya que está basado en filas.

Los integradores usan una representación little-endian de longitud fija. Por ejemplo, UInt64 usa 8 bytes.
DateTime se representa como UInt32 que contiene la marca de tiempo Unix como el valor.
Date se representa como un objeto UInt16 que contiene el número de días desde 1970-01-01 como el valor.
La cadena se representa como una longitud varint (sin signo [LEB128](https://en.wikipedia.org/wiki/LEB128)), seguido de los bytes de la cadena.
FixedString se representa simplemente como una secuencia de bytes.

La matriz se representa como una longitud varint (sin signo [LEB128](https://en.wikipedia.org/wiki/LEB128)), seguido de elementos sucesivos de la matriz.

Para [NULL](../sql-reference/syntax.md#null-literal) soporte, se añade un byte adicional que contiene 1 o 0 antes de cada [NULL](../sql-reference/data-types/nullable.md) valor. Si 1, entonces el valor es `NULL` y este byte se interpreta como un valor separado. Si es 0, el valor después del byte no es `NULL`.

## RowBinaryWithNamesAndTypes {#rowbinarywithnamesandtypes}

Similar a [RowBinary](#rowbinary), pero con encabezado añadido:

-   [LEB128](https://en.wikipedia.org/wiki/LEB128)-número codificado de columnas (N)
-   N `String`s especificando nombres de columna
-   N `String`s especificando tipos de columna

## Valor {#data-format-values}

Imprime cada fila entre paréntesis. Las filas están separadas por comas. No hay coma después de la última fila. Los valores dentro de los corchetes también están separados por comas. Los números se emiten en formato decimal sin comillas. Las matrices se emiten entre corchetes. Las cadenas, fechas y fechas con horas se generan entre comillas. Las reglas de escape y el análisis son similares a las [TabSeparated](#tabseparated) formato. Durante el formateo, los espacios adicionales no se insertan, pero durante el análisis, se permiten y omiten (excepto los espacios dentro de los valores de la matriz, que no están permitidos). [NULL](../sql-reference/syntax.md) se representa como `NULL`.

The minimum set of characters that you need to escape when passing data in Values ​​format: single quotes and backslashes.

Este es el formato que se utiliza en `INSERT INTO t VALUES ...`, pero también puede usarlo para formatear los resultados de la consulta.

Ver también: [input_format_values_interpret_expressions](../operations/settings/settings.md#settings-input_format_values_interpret_expressions) y [input_format_values_deduce_templates_of_expressions](../operations/settings/settings.md#settings-input_format_values_deduce_templates_of_expressions) configuración.

## Vertical {#vertical}

Imprime cada valor en una línea independiente con el nombre de columna especificado. Este formato es conveniente para imprimir solo una o varias filas si cada fila consta de un gran número de columnas.

[NULL](../sql-reference/syntax.md) se emite como `ᴺᵁᴸᴸ`.

Ejemplo:

``` sql
SELECT * FROM t_null FORMAT Vertical
```

``` text
Row 1:
──────
x: 1
y: ᴺᵁᴸᴸ
```

Las filas no se escapan en formato vertical:

``` sql
SELECT 'string with \'quotes\' and \t with some special \n characters' AS test FORMAT Vertical
```

``` text
Row 1:
──────
test: string with 'quotes' and      with some special
 characters
```

Este formato solo es apropiado para generar un resultado de consulta, pero no para analizar (recuperar datos para insertar en una tabla).

## VerticalRaw {#verticalraw}

Similar a [Vertical](#vertical), pero con escapar deshabilitado. Este formato solo es adecuado para generar resultados de consultas, no para analizar (recibir datos e insertarlos en la tabla).

## XML {#xml}

El formato XML es adecuado solo para la salida, no para el análisis. Ejemplo:

``` xml
<?xml version='1.0' encoding='UTF-8' ?>
<result>
        <meta>
                <columns>
                        <column>
                                <name>SearchPhrase</name>
                                <type>String</type>
                        </column>
                        <column>
                                <name>count()</name>
                                <type>UInt64</type>
                        </column>
                </columns>
        </meta>
        <data>
                <row>
                        <SearchPhrase></SearchPhrase>
                        <field>8267016</field>
                </row>
                <row>
                        <SearchPhrase>bathroom interior design</SearchPhrase>
                        <field>2166</field>
                </row>
                <row>
                        <SearchPhrase>yandex</SearchPhrase>
                        <field>1655</field>
                </row>
                <row>
                        <SearchPhrase>2014 spring fashion</SearchPhrase>
                        <field>1549</field>
                </row>
                <row>
                        <SearchPhrase>freeform photos</SearchPhrase>
                        <field>1480</field>
                </row>
                <row>
                        <SearchPhrase>angelina jolie</SearchPhrase>
                        <field>1245</field>
                </row>
                <row>
                        <SearchPhrase>omsk</SearchPhrase>
                        <field>1112</field>
                </row>
                <row>
                        <SearchPhrase>photos of dog breeds</SearchPhrase>
                        <field>1091</field>
                </row>
                <row>
                        <SearchPhrase>curtain designs</SearchPhrase>
                        <field>1064</field>
                </row>
                <row>
                        <SearchPhrase>baku</SearchPhrase>
                        <field>1000</field>
                </row>
        </data>
        <rows>10</rows>
        <rows_before_limit_at_least>141137</rows_before_limit_at_least>
</result>
```

Si el nombre de la columna no tiene un formato aceptable, simplemente ‘field’ se utiliza como el nombre del elemento. En general, la estructura XML sigue la estructura JSON.
Just as for JSON, invalid UTF-8 sequences are changed to the replacement character � so the output text will consist of valid UTF-8 sequences.

En los valores de cadena, los caracteres `<` y `&` se escaparon como `<` y `&`.

Las matrices se emiten como `<array><elem>Hello</elem><elem>World</elem>...</array>`y tuplas como `<tuple><elem>Hello</elem><elem>World</elem>...</tuple>`.

## CapnProto {#capnproto}

Cap'n Proto es un formato de mensaje binario similar a Protocol Buffers y Thrift, pero no como JSON o MessagePack.

Los mensajes de Cap'n Proto están estrictamente escritos y no autodescribidos, lo que significa que necesitan una descripción de esquema externo. El esquema se aplica sobre la marcha y se almacena en caché para cada consulta.

``` bash
$ cat capnproto_messages.bin | clickhouse-client --query "INSERT INTO test.hits FORMAT CapnProto SETTINGS format_schema='schema:Message'"
```

Donde `schema.capnp` se ve así:

``` capnp
struct Message {
  SearchPhrase @0 :Text;
  c @1 :Uint64;
}
```

La deserialización es efectiva y generalmente no aumenta la carga del sistema.

Ver también [Esquema de formato](#formatschema).

## Protobuf {#protobuf}

Protobuf - es un [Búferes de protocolo](https://developers.google.com/protocol-buffers/) formato.

Este formato requiere un esquema de formato externo. El esquema se almacena en caché entre las consultas.
ClickHouse soporta ambos `proto2` y `proto3` sintaxis. Se admiten campos repetidos / opcionales / requeridos.

Ejemplos de uso:

``` sql
SELECT * FROM test.table FORMAT Protobuf SETTINGS format_schema = 'schemafile:MessageType'
```

``` bash
cat protobuf_messages.bin | clickhouse-client --query "INSERT INTO test.table FORMAT Protobuf SETTINGS format_schema='schemafile:MessageType'"
```

donde el archivo `schemafile.proto` se ve así:

``` capnp
syntax = "proto3";

message MessageType {
  string name = 1;
  string surname = 2;
  uint32 birthDate = 3;
  repeated string phoneNumbers = 4;
};
```

Para encontrar la correspondencia entre las columnas de la tabla y los campos del tipo de mensaje de Protocol Buffers, ClickHouse compara sus nombres.
Esta comparación no distingue entre mayúsculas y minúsculas y los caracteres `_` (subrayado) y `.` (punto) se consideran iguales.
Si los tipos de una columna y un campo del mensaje de Protocol Buffers son diferentes, se aplica la conversión necesaria.

Los mensajes anidados son compatibles. Por ejemplo, para el campo `z` en el siguiente tipo de mensaje

``` capnp
message MessageType {
  message XType {
    message YType {
      int32 z;
    };
    repeated YType y;
  };
  XType x;
};
```

ClickHouse intenta encontrar una columna llamada `x.y.z` (o `x_y_z` o `X.y_Z` y así sucesivamente).
Los mensajes anidados son adecuados para [estructuras de datos anidados](../sql-reference/data-types/nested-data-structures/nested.md).

Valores predeterminados definidos en un esquema protobuf como este

``` capnp
syntax = "proto2";

message MessageType {
  optional int32 result_per_page = 3 [default = 10];
}
```

no se aplican; el [valores predeterminados de la tabla](../sql-reference/statements/create.md#create-default-values) se utilizan en lugar de ellos.

ClickHouse entra y emite mensajes protobuf en el `length-delimited` formato.
Significa que antes de cada mensaje debe escribirse su longitud como un [varint](https://developers.google.com/protocol-buffers/docs/encoding#varints).
Ver también [cómo leer / escribir mensajes protobuf delimitados por longitud en idiomas populares](https://cwiki.apache.org/confluence/display/GEODE/Delimiting+Protobuf+Messages).

## Avro {#data-format-avro}

[Más información](http://avro.apache.org/) es un marco de serialización de datos orientado a filas desarrollado dentro del proyecto Hadoop de Apache.

El formato ClickHouse Avro admite lectura y escritura [Archivos de datos Avro](http://avro.apache.org/docs/current/spec.html#Object+Container+Files).

### Coincidencia de tipos de datos {#data_types-matching}

La siguiente tabla muestra los tipos de datos admitidos y cómo coinciden con ClickHouse [tipos de datos](../sql-reference/data-types/index.md) en `INSERT` y `SELECT` consulta.

| Tipo de datos Avro `INSERT`                 | Tipo de datos ClickHouse                                                                                                | Tipo de datos Avro `SELECT`  |
|---------------------------------------------|-------------------------------------------------------------------------------------------------------------------------|------------------------------|
| `boolean`, `int`, `long`, `float`, `double` | [¿Cómo funciona?)](../sql-reference/data-types/int-uint.md), [UInt(8\|16\|32)](../sql-reference/data-types/int-uint.md) | `int`                        |
| `boolean`, `int`, `long`, `float`, `double` | [Int64](../sql-reference/data-types/int-uint.md), [UInt64](../sql-reference/data-types/int-uint.md)                     | `long`                       |
| `boolean`, `int`, `long`, `float`, `double` | [Float32](../sql-reference/data-types/float.md)                                                                         | `float`                      |
| `boolean`, `int`, `long`, `float`, `double` | [Float64](../sql-reference/data-types/float.md)                                                                         | `double`                     |
| `bytes`, `string`, `fixed`, `enum`          | [Cadena](../sql-reference/data-types/string.md)                                                                         | `bytes`                      |
| `bytes`, `string`, `fixed`                  | [Cadena fija (N)](../sql-reference/data-types/fixedstring.md)                                                           | `fixed(N)`                   |
| `enum`                                      | [Enum (8\|16)](../sql-reference/data-types/enum.md)                                                                     | `enum`                       |
| `array(T)`                                  | [Matriz (T)](../sql-reference/data-types/array.md)                                                                      | `array(T)`                   |
| `union(null, T)`, `union(T, null)`          | [Nivel de Cifrado WEP)](../sql-reference/data-types/date.md)                                                            | `union(null, T)`             |
| `null`                                      | [Nullable (nada)](../sql-reference/data-types/special-data-types/nothing.md)                                            | `null`                       |
| `int (date)` \*                             | [Fecha](../sql-reference/data-types/date.md)                                                                            | `int (date)` \*              |
| `long (timestamp-millis)` \*                | [¿Qué puedes encontrar en Neodigit)](../sql-reference/data-types/datetime.md)                                           | `long (timestamp-millis)` \* |
| `long (timestamp-micros)` \*                | [Cómo hacer esto?)](../sql-reference/data-types/datetime.md)                                                            | `long (timestamp-micros)` \* |

\* [Tipos lógicos Avro](http://avro.apache.org/docs/current/spec.html#Logical+Types)

Tipos de datos Avro no admitidos: `record` (no root), `map`

Tipos de datos lógicos Avro no admitidos: `uuid`, `time-millis`, `time-micros`, `duration`

### Insertar datos {#inserting-data-1}

Para insertar datos de un archivo Avro en la tabla ClickHouse:

``` bash
$ cat file.avro | clickhouse-client --query="INSERT INTO {some_table} FORMAT Avro"
```

El esquema raíz del archivo Avro de entrada debe ser de `record` tipo.

Para encontrar la correspondencia entre las columnas de la tabla y los campos de Avro esquema ClickHouse compara sus nombres. Esta comparación distingue entre mayúsculas y minúsculas.
Los campos no utilizados se omiten.

Los tipos de datos de las columnas de tabla ClickHouse pueden diferir de los campos correspondientes de los datos de Avro insertados. Al insertar datos, ClickHouse interpreta los tipos de datos de acuerdo con la tabla anterior y luego [elenco](../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) los datos al tipo de columna correspondiente.

### Selección de datos {#selecting-data-1}

Para seleccionar datos de la tabla ClickHouse en un archivo Avro:

``` bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Avro" > file.avro
```

Los nombres de columna deben:

-   comenzar con `[A-Za-z_]`
-   posteriormente contienen sólo `[A-Za-z0-9_]`

La compresión de archivos Avro de salida y el intervalo de sincronización se pueden configurar con [Sistema abierto.](../operations/settings/settings.md#settings-output_format_avro_codec) y [Sistema abierto.](../operations/settings/settings.md#settings-output_format_avro_sync_interval) respectivamente.

## AvroConfluent {#data-format-avro-confluent}

AvroConfluent admite la decodificación de mensajes Avro de un solo objeto comúnmente utilizados con [Kafka](https://kafka.apache.org/) y [Registro de Esquemas Confluentes](https://docs.confluent.io/current/schema-registry/index.html).

Cada mensaje de Avro incrusta un id de esquema que se puede resolver en el esquema real con la ayuda del Registro de esquemas.

Los esquemas se almacenan en caché una vez resueltos.

La URL del registro de esquemas se configura con [Todos los derechos reservados.](../operations/settings/settings.md#settings-format_avro_schema_registry_url)

### Coincidencia de tipos de datos {#data_types-matching-1}

Lo mismo que [Avro](#data-format-avro)

### Uso {#usage}

Para verificar rápidamente la resolución del esquema, puede usar [Método de codificación de datos:](https://github.com/edenhill/kafkacat) con [Sistema abierto.](../operations/utilities/clickhouse-local.md#clickhouse-local):

``` bash
$ kafkacat -b kafka-broker  -C -t topic1 -o beginning -f '%s' -c 3 | clickhouse-local   --input-format AvroConfluent --format_avro_schema_registry_url 'http://schema-registry' -S "field1 Int64, field2 String"  -q 'select *  from table'
1 a
2 b
3 c
```

Utilizar `AvroConfluent` con [Kafka](../engines/table-engines/integrations/kafka.md):

``` sql
CREATE TABLE topic1_stream
(
    field1 String,
    field2 String
)
ENGINE = Kafka()
SETTINGS
kafka_broker_list = 'kafka-broker',
kafka_topic_list = 'topic1',
kafka_group_name = 'group1',
kafka_format = 'AvroConfluent';

SET format_avro_schema_registry_url = 'http://schema-registry';

SELECT * FROM topic1_stream;
```

!!! note "Advertencia"
    Configuración `format_avro_schema_registry_url` necesita ser configurado en `users.xml` para mantener su valor después de un reinicio.

## Parquet {#data-format-parquet}

[Apache Parquet](http://parquet.apache.org/) es un formato de almacenamiento columnar generalizado en el ecosistema Hadoop. ClickHouse admite operaciones de lectura y escritura para este formato.

### Coincidencia de tipos de datos {#data_types-matching-2}

La siguiente tabla muestra los tipos de datos admitidos y cómo coinciden con ClickHouse [tipos de datos](../sql-reference/data-types/index.md) en `INSERT` y `SELECT` consulta.

| Tipo de datos de parquet (`INSERT`) | Tipo de datos ClickHouse                                  | Tipo de datos de parquet (`SELECT`) |
|-------------------------------------|-----------------------------------------------------------|-------------------------------------|
| `UINT8`, `BOOL`                     | [UInt8](../sql-reference/data-types/int-uint.md)          | `UINT8`                             |
| `INT8`                              | [Int8](../sql-reference/data-types/int-uint.md)           | `INT8`                              |
| `UINT16`                            | [UInt16](../sql-reference/data-types/int-uint.md)         | `UINT16`                            |
| `INT16`                             | [Int16](../sql-reference/data-types/int-uint.md)          | `INT16`                             |
| `UINT32`                            | [UInt32](../sql-reference/data-types/int-uint.md)         | `UINT32`                            |
| `INT32`                             | [Int32](../sql-reference/data-types/int-uint.md)          | `INT32`                             |
| `UINT64`                            | [UInt64](../sql-reference/data-types/int-uint.md)         | `UINT64`                            |
| `INT64`                             | [Int64](../sql-reference/data-types/int-uint.md)          | `INT64`                             |
| `FLOAT`, `HALF_FLOAT`               | [Float32](../sql-reference/data-types/float.md)           | `FLOAT`                             |
| `DOUBLE`                            | [Float64](../sql-reference/data-types/float.md)           | `DOUBLE`                            |
| `DATE32`                            | [Fecha](../sql-reference/data-types/date.md)              | `UINT16`                            |
| `DATE64`, `TIMESTAMP`               | [FechaHora](../sql-reference/data-types/datetime.md)      | `UINT32`                            |
| `STRING`, `BINARY`                  | [Cadena](../sql-reference/data-types/string.md)           | `STRING`                            |
| —                                   | [Cadena fija](../sql-reference/data-types/fixedstring.md) | `STRING`                            |
| `DECIMAL`                           | [Decimal](../sql-reference/data-types/decimal.md)         | `DECIMAL`                           |

ClickHouse admite una precisión configurable de `Decimal` tipo. El `INSERT` consulta trata el Parquet `DECIMAL` tipo como el ClickHouse `Decimal128` tipo.

Tipos de datos de parquet no admitidos: `DATE32`, `TIME32`, `FIXED_SIZE_BINARY`, `JSON`, `UUID`, `ENUM`.

Los tipos de datos de las columnas de tabla ClickHouse pueden diferir de los campos correspondientes de los datos de Parquet insertados. Al insertar datos, ClickHouse interpreta los tipos de datos de acuerdo con la tabla anterior y luego [elenco](../query_language/functions/type_conversion_functions/#type_conversion_function-cast) los datos de ese tipo de datos que se establece para la columna de tabla ClickHouse.

### Insertar y seleccionar datos {#inserting-and-selecting-data}

Puede insertar datos de Parquet desde un archivo en la tabla ClickHouse mediante el siguiente comando:

``` bash
$ cat {filename} | clickhouse-client --query="INSERT INTO {some_table} FORMAT Parquet"
```

Puede seleccionar datos de una tabla ClickHouse y guardarlos en algún archivo en el formato Parquet mediante el siguiente comando:

``` bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Parquet" > {some_file.pq}
```

Para intercambiar datos con Hadoop, puede usar [Motor de mesa HDFS](../engines/table-engines/integrations/hdfs.md).

## ORC {#data-format-orc}

[Apache ORC](https://orc.apache.org/) es un formato de almacenamiento columnar generalizado en el ecosistema Hadoop. Solo puede insertar datos en este formato en ClickHouse.

### Coincidencia de tipos de datos {#data_types-matching-3}

La siguiente tabla muestra los tipos de datos admitidos y cómo coinciden con ClickHouse [tipos de datos](../sql-reference/data-types/index.md) en `INSERT` consulta.

| Tipo de datos ORC (`INSERT`) | Tipo de datos ClickHouse                             |
|------------------------------|------------------------------------------------------|
| `UINT8`, `BOOL`              | [UInt8](../sql-reference/data-types/int-uint.md)     |
| `INT8`                       | [Int8](../sql-reference/data-types/int-uint.md)      |
| `UINT16`                     | [UInt16](../sql-reference/data-types/int-uint.md)    |
| `INT16`                      | [Int16](../sql-reference/data-types/int-uint.md)     |
| `UINT32`                     | [UInt32](../sql-reference/data-types/int-uint.md)    |
| `INT32`                      | [Int32](../sql-reference/data-types/int-uint.md)     |
| `UINT64`                     | [UInt64](../sql-reference/data-types/int-uint.md)    |
| `INT64`                      | [Int64](../sql-reference/data-types/int-uint.md)     |
| `FLOAT`, `HALF_FLOAT`        | [Float32](../sql-reference/data-types/float.md)      |
| `DOUBLE`                     | [Float64](../sql-reference/data-types/float.md)      |
| `DATE32`                     | [Fecha](../sql-reference/data-types/date.md)         |
| `DATE64`, `TIMESTAMP`        | [FechaHora](../sql-reference/data-types/datetime.md) |
| `STRING`, `BINARY`           | [Cadena](../sql-reference/data-types/string.md)      |
| `DECIMAL`                    | [Decimal](../sql-reference/data-types/decimal.md)    |

ClickHouse soporta la precisión configurable de la `Decimal` tipo. El `INSERT` consulta trata el ORC `DECIMAL` tipo como el ClickHouse `Decimal128` tipo.

Tipos de datos ORC no admitidos: `DATE32`, `TIME32`, `FIXED_SIZE_BINARY`, `JSON`, `UUID`, `ENUM`.

Los tipos de datos de las columnas de tabla ClickHouse no tienen que coincidir con los campos de datos ORC correspondientes. Al insertar datos, ClickHouse interpreta los tipos de datos de acuerdo con la tabla anterior y luego [elenco](../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) los datos al tipo de datos establecido para la columna de tabla ClickHouse.

### Insertar datos {#inserting-data-2}

Puede insertar datos ORC de un archivo en la tabla ClickHouse mediante el siguiente comando:

``` bash
$ cat filename.orc | clickhouse-client --query="INSERT INTO some_table FORMAT ORC"
```

Para intercambiar datos con Hadoop, puede usar [Motor de mesa HDFS](../engines/table-engines/integrations/hdfs.md).

## Esquema de formato {#formatschema}

El valor establece el nombre de archivo que contiene el esquema de formato `format_schema`.
Es necesario establecer esta configuración cuando se utiliza uno de los formatos `Cap'n Proto` y `Protobuf`.
El esquema de formato es una combinación de un nombre de archivo y el nombre de un tipo de mensaje en este archivo, delimitado por dos puntos,
e.g. `schemafile.proto:MessageType`.
Si el archivo tiene la extensión estándar para el formato (por ejemplo, `.proto` para `Protobuf`),
se puede omitir y en este caso, el esquema de formato se ve así `schemafile:MessageType`.

Si introduce o emite datos a través del [cliente](../interfaces/cli.md) en el [modo interactivo](../interfaces/cli.md#cli_usage), el nombre de archivo especificado en el esquema de formato
puede contener una ruta absoluta o una ruta relativa al directorio actual en el cliente.
Si utiliza el cliente en el [modo por lotes](../interfaces/cli.md#cli_usage), la ruta de acceso al esquema debe ser relativa por razones de seguridad.

Si introduce o emite datos a través del [Interfaz HTTP](../interfaces/http.md) el nombre de archivo especificado en el esquema de formato
debe estar ubicado en el directorio especificado en [format_schema_path](../operations/server-configuration-parameters/settings.md#server_configuration_parameters-format_schema_path)
en la configuración del servidor.

## Salto de errores {#skippingerrors}

Algunos formatos como `CSV`, `TabSeparated`, `TSKV`, `JSONEachRow`, `Template`, `CustomSeparated` y `Protobuf` puede omitir la fila rota si se produjo un error de análisis y continuar el análisis desde el comienzo de la siguiente fila. Ver [Entrada_format_allow_errors_num](../operations/settings/settings.md#settings-input_format_allow_errors_num) y
[Entrada_format_allow_errors_ratio](../operations/settings/settings.md#settings-input_format_allow_errors_ratio) configuración.
Limitacion:
- En caso de error de análisis `JSONEachRow` omite todos los datos hasta la nueva línea (o EOF), por lo que las filas deben estar delimitadas por `\n` para contar los errores correctamente.
- `Template` y `CustomSeparated` use el delimitador después de la última columna y el delimitador entre filas para encontrar el comienzo de la siguiente fila, por lo que omitir errores solo funciona si al menos uno de ellos no está vacío.

[Artículo Original](https://clickhouse.tech/docs/en/interfaces/formats/) <!--hide-->
