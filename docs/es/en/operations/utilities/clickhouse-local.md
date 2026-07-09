---
description: 'Guía para usar clickhouse-local para procesar datos sin necesidad de un servidor'
sidebar_label: 'clickhouse-local'
sidebar_position: 60
slug: /operations/utilities/clickhouse-local
title: 'clickhouse-local'
doc_type: 'referencia'
---

<div id="when-to-use-clickhouse-local-vs-clickhouse">
  ## Cuándo usar clickhouse-local vs. ClickHouse
</div>

`clickhouse-local` es una versión de ClickHouse fácil de usar, ideal para desarrolladores que necesitan procesar rápidamente archivos locales y remotos con SQL sin tener que instalar un servidor de base de datos completo. Con `clickhouse-local`, los desarrolladores pueden usar comandos SQL (con el [dialecto SQL de ClickHouse](../../sql-reference/index.md)) directamente desde la línea de comandos, lo que ofrece una forma sencilla y eficiente de acceder a las funcionalidades de ClickHouse sin necesidad de una instalación completa. Una de las principales ventajas de `clickhouse-local` es que ya se incluye al instalar [clickhouse-client](/es/operations/utilities/clickhouse-local). Esto significa que los desarrolladores pueden empezar a usar `clickhouse-local` rápidamente, sin pasar por un proceso de instalación complejo.

Aunque `clickhouse-local` es una excelente herramienta para tareas de desarrollo, pruebas y procesamiento de archivos, no es adecuada para dar servicio a usuarios finales ni a aplicaciones. En estos casos, se recomienda usar [ClickHouse](/es/install) de código abierto. ClickHouse es una potente base de datos OLAP diseñada para manejar cargas de trabajo analíticas a gran escala. Proporciona un procesamiento rápido y eficiente de consultas complejas sobre grandes conjuntos de datos, lo que la hace ideal para entornos de producción donde el alto rendimiento es fundamental. Además, ClickHouse ofrece una amplia gama de funcionalidades, como replicación, segmentación y alta disponibilidad, esenciales para escalar y manejar grandes conjuntos de datos, así como para dar servicio a aplicaciones. Si necesitas trabajar con conjuntos de datos más grandes o dar servicio a usuarios finales o aplicaciones, te recomendamos usar ClickHouse de código abierto en lugar de `clickhouse-local`.

Consulta la documentación a continuación, donde se muestran casos de uso de `clickhouse-local`, como [consultar un archivo local](#query_data_in_file) o [leer un archivo Parquet en S3](#query-data-in-a-parquet-file-in-aws-s3).

<div id="download-clickhouse-local">
  ## Descargar clickhouse-local
</div>

`clickhouse-local` se ejecuta con el mismo binario `clickhouse` que utiliza el servidor ClickHouse y `clickhouse-client`. La forma más sencilla de descargar la versión más reciente es con el siguiente comando:

```bash
curl https://clickhouse.com/ | sh
```

:::note
El binario que acabas de descargar puede ejecutar todo tipo de herramientas y utilidades de ClickHouse. Si quieres ejecutar ClickHouse como servidor de bases de datos, consulta la [Quick Start](/es/get-started/quick-start).
:::

<div id="query_data_in_file">
  ## Consultar datos en un archivo usando SQL
</div>

Un uso común de `clickhouse-local` es ejecutar consultas ad hoc sobre archivos, sin tener que insertar los datos en una tabla. `clickhouse-local` puede transmitir los datos desde un archivo a una tabla temporal y ejecutar tu SQL.

Si el archivo está en la misma máquina que `clickhouse-local`, puedes simplemente especificar el archivo que se va a cargar. El siguiente archivo `reviews.tsv` contiene una muestra de reseñas de productos de Amazon:

```bash
./clickhouse local -q "SELECT * FROM 'reviews.tsv'"
```

Este comando es un atajo de:

```bash
./clickhouse local -q "SELECT * FROM file('reviews.tsv')"
```

ClickHouse sabe que el archivo usa un formato tabulado por la extensión del nombre de archivo. Si necesita especificar el formato explícitamente, simplemente agregue uno de los [muchos formatos de entrada de ClickHouse](../../interfaces/formats.md):

```bash
./clickhouse local -q "SELECT * FROM file('reviews.tsv', 'TabSeparated')"
```

La función de tabla `file` crea una tabla y puedes usar `DESCRIBE` para ver el esquema inferido:

```bash
./clickhouse local -q "DESCRIBE file('reviews.tsv')"
```

:::tip
Puede usar globs en el nombre del archivo (consulte [sustituciones de glob](/es/sql-reference/table-functions/file.md/#globs-in-path)).

Ejemplos:

```bash
./clickhouse local -q "SELECT * FROM 'reviews*.jsonl'"
./clickhouse local -q "SELECT * FROM 'review_?.csv'"
./clickhouse local -q "SELECT * FROM 'review_{1..3}.csv'"
```

:::

```response
marketplace    Nullable(String)
customer_id    Nullable(Int64)
review_id    Nullable(String)
product_id    Nullable(String)
product_parent    Nullable(Int64)
product_title    Nullable(String)
product_category    Nullable(String)
star_rating    Nullable(Int64)
helpful_votes    Nullable(Int64)
total_votes    Nullable(Int64)
vine    Nullable(String)
verified_purchase    Nullable(String)
review_headline    Nullable(String)
review_body    Nullable(String)
review_date    Nullable(Date)
```

Busquemos el producto con la calificación más alta:

```bash
./clickhouse local -q "SELECT
    argMax(product_title,star_rating),
    max(star_rating)
FROM file('reviews.tsv')"
```

```response
Monopoly Junior Board Game    5
```

<div id="query-data-in-a-parquet-file-in-aws-s3">
  ## Consultar datos en un archivo Parquet en AWS S3
</div>

Si tiene un archivo en S3, use `clickhouse-local` y la función de tabla `s3` para consultar el archivo directamente (sin insertar los datos en una tabla de ClickHouse). Tenemos un archivo llamado `house_0.parquet` en un bucket público que contiene los precios de viviendas vendidas en el Reino Unido. Veamos cuántas filas tiene:

```bash
./clickhouse local -q "
SELECT count()
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/house_parquet/house_0.parquet')"
```

El archivo tiene 2,7 millones de filas:

```response
2772030
```

Siempre es útil ver cuál es el esquema inferido que ClickHouse obtiene del archivo:

```bash
./clickhouse local -q "DESCRIBE s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/house_parquet/house_0.parquet')"
```

```response
price    Nullable(Int64)
date    Nullable(UInt16)
postcode1    Nullable(String)
postcode2    Nullable(String)
type    Nullable(String)
is_new    Nullable(UInt8)
duration    Nullable(String)
addr1    Nullable(String)
addr2    Nullable(String)
street    Nullable(String)
locality    Nullable(String)
town    Nullable(String)
district    Nullable(String)
county    Nullable(String)
```

Veamos cuáles son los barrios más caros:

```bash
./clickhouse local -q "
SELECT
    town,
    district,
    count() AS c,
    round(avg(price)) AS price,
    bar(price, 0, 5000000, 100)
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/house_parquet/house_0.parquet')
GROUP BY
    town,
    district
HAVING c >= 100
ORDER BY price DESC
LIMIT 10"
```

```response
LONDON    CITY OF LONDON    886    2271305    █████████████████████████████████████████████▍
LEATHERHEAD    ELMBRIDGE    206    1176680    ███████████████████████▌
LONDON    CITY OF WESTMINSTER    12577    1108221    ██████████████████████▏
LONDON    KENSINGTON AND CHELSEA    8728    1094496    █████████████████████▉
HYTHE    FOLKESTONE AND HYTHE    130    1023980    ████████████████████▍
CHALFONT ST GILES    CHILTERN    113    835754    ████████████████▋
AMERSHAM    BUCKINGHAMSHIRE    113    799596    ███████████████▉
VIRGINIA WATER    RUNNYMEDE    356    789301    ███████████████▊
BARNET    ENFIELD    282    740514    ██████████████▊
NORTHWOOD    THREE RIVERS    184    731609    ██████████████▋
```

:::tip
Cuando esté listo para insertar sus archivos en ClickHouse, inicie un servidor de ClickHouse e inserte los resultados de las funciones de tabla `file` y `s3` en una tabla `MergeTree`. Consulte [Quick Start](/es/get-started/quick-start) para obtener más detalles.
:::

<div id="format-conversions">
  ## Conversión entre formatos
</div>

Puedes usar `clickhouse-local` para convertir datos entre distintos formatos. Ejemplo:

```bash
$ clickhouse-local --input-format JSONLines --output-format CSV --query "SELECT * FROM table" < data.json > data.csv
```

Los formatos se detectan automáticamente según las extensiones de archivo:

```bash
$ clickhouse-local --query "SELECT * FROM table" < data.json > data.csv
```

Como alternativa rápida, puedes escribirlo usando el argumento `--copy`:

```bash
$ clickhouse-local --copy < data.json > data.csv
```

<div id="usage">
  ## Uso
</div>

De forma predeterminada, `clickhouse-local` tiene acceso a los datos de un servidor ClickHouse en el mismo host y no depende de la configuración del servidor. También permite cargar la configuración del servidor mediante el argumento `--config-file`. Para los datos temporales, de forma predeterminada se crea un directorio de datos temporales único.

Uso básico (Linux):

```bash
$ clickhouse-local --structure "table_structure" --input-format "format_of_incoming_data" --query "query"
```

Uso básico (Mac):

```bash
$ ./clickhouse local --structure "table_structure" --input-format "format_of_incoming_data" --query "query"
```

:::note
`clickhouse-local` también se admite en Windows a través de WSL2.
:::

Argumentos:

* `-S`, `--structure` — estructura de la tabla para los datos de entrada.
* `--input-format` — formato de entrada; `TSV` de forma predeterminada.
* `-F`, `--file` — ruta de los datos; `stdin` de forma predeterminada.
* `-q`, `--query` — consultas que se ejecutarán usando `;` como delimitador. `--query` se puede especificar varias veces; por ejemplo, `--query "SELECT 1" --query "SELECT 2"`. No puede usarse simultáneamente con `--queries-file`.
* `--queries-file` - ruta del archivo con las consultas que se ejecutarán. `--queries-file` se puede especificar varias veces; por ejemplo, `--query queries1.sql --query queries2.sql`. No puede usarse simultáneamente con `--query`.
* `--multiquery, -n` – Si se especifica, se pueden indicar varias consultas separadas por punto y coma después de la opción `--query`. Para mayor comodidad, también es posible omitir `--query` y pasar las consultas directamente después de `--multiquery`.
* `-N`, `--table` — nombre de la tabla en la que se colocarán los datos de salida; `table` de forma predeterminada.
* `-f`, `--format`, `--output-format` — formato de salida; `TSV` de forma predeterminada.
* `-d`, `--database` — base de datos predeterminada; `_local` de forma predeterminada.
* `--stacktrace` — si se debe volcar la salida de depuración en caso de excepción.
* `--echo [ <bool> ]` — imprime cada consulta antes de ejecutarla. Acepta un valor booleano opcional. Está habilitado de forma predeterminada en modo interactivo y deshabilitado en batch mode. Nota: como `--echo` ahora acepta un valor opcional, una consulta posicional colocada inmediatamente después de un `--echo` sin valor se toma como su valor; use `--echo --query "..."`, `--echo -q "..."`, `--echo=false` o `stdin` canalizado en su lugar.
* `--echo-formatted [ <bool> ]` — da formato a las consultas mostradas. Acepta un valor booleano opcional. Está habilitado de forma predeterminada en modo interactivo y deshabilitado en batch mode.
* `--echo-query-id [ <bool> ]` — imprime el `query_id` antes de la ejecución. Acepta un valor booleano opcional. Está habilitado de forma predeterminada en modo interactivo y deshabilitado en batch mode.
* `--echo-query-separator <string>` — imprime este separador antes de la consulta mostrada con formato (requiere `--echo-formatted`), lo que facilita distinguir la consulta escrita de su versión reformateada. Está vacío de forma predeterminada (deshabilitado).
* `--highlight`, `--hilite` `<bool>` — activa o desactiva el resaltado de sintaxis del prompt de comandos y de las consultas mostradas. Está habilitado de forma predeterminada. El resaltado solo se aplica al escribir en una terminal.
* `--hints <bool>` — muestra sugerencias de autocompletado mientras escribe (texto &quot;fantasma&quot; en línea) para la sugerencia coincidente más adecuada cuando el cursor está al final de la entrada. Desplácese por las sugerencias con Arriba/Abajo (o Ctrl-Arriba/Ctrl-Abajo); acepte la sugerencia en línea con Tab o Derecha; `Enter` acepta una sugerencia solo después de que se haya seleccionado explícitamente y, en caso contrario, ejecuta la consulta; `Tab` también abre la lista clásica de autocompletado. Requiere `--highlight` (las sugerencias necesitan color) y el mecanismo de sugerencias (por lo que `--disable_suggestion` también las desactiva). Está habilitado de forma predeterminada.
* `--verbose` — muestra más detalles sobre la ejecución de la consulta.
* `--logger.console` — registrar en la consola.
* `--logger.log` — nombre del archivo de log.
* `--logger.level` — nivel de log.
* `--ignore-error` — no detener el procesamiento si una consulta falla.
* `-c`, `--config-file` — ruta al archivo de configuración en el mismo formato que para servidor ClickHouse; de forma predeterminada, la configuración está vacía.
* `--no-system-tables` — no adjuntar las tablas del sistema.
* `--help` — referencia de argumentos para `clickhouse-local`.
* `-V`, `--version` — imprime la información de la versión y sale.

Además, hay argumentos para cada variable de configuración de ClickHouse que suelen usarse en lugar de `--config-file`.

<div id="commands">
  ## Comandos
</div>

<div id="ls-command">
  ### Comando LS
</div>

Lista todos los archivos del directorio de trabajo actual a los que puede acceder clickhouse-local.

Puedes ejecutarlo en modo interactivo así:

```sql title="Query"
ClickHouse local version 26.3.1.1.

:) ls

SELECT _file AS file
FROM file('*', 'One')
ORDER BY file ASC
```

```text title="Response"
┌─file────────┐
│ file1.csv   │
│ file2.json  │
│ file3.xml   │
└─────────────┘
```

También puedes ejecutarlo como consulta con el argumento `-q`:

```sh
./clickhouse-local -q ls
```

```text title="Response"
file1.csv
file2.json
file3.xml
```

<div id="clear-command">
  ### Comando CLEAR
</div>

Limpia la pantalla del terminal (similar al comando `clear` en Linux o a Ctrl+L en muchos terminales). Esta es una acción del lado del client: no se envía al motor SQL.

En `clickhouse-local`, el metacomando se reconoce en modo **interactivo** y para la entrada de **`-q`** y **`--queries-file`** (la misma ruta del client que `-q`, la misma idea que `ls`), por lo que un `clear` sin más no produce un error `UNKNOWN_IDENTIFIER`. En el caso remoto de **`clickhouse-client --queries-file`**, no hay cambios: el contenido del archivo se ejecuta únicamente como SQL (sin metacomandos a nivel de texto).

En `clickhouse-client`, se reconoce solo en modo **interactivo**. Con **`-q`** o archivos de consultas, `clear` se sigue interpretando como SQL, por lo que la automatización mantiene el comportamiento de error anterior en lugar de convertir errores tipográficos en un no-op silencioso.

Formas admitidas: `clear`, `CLEAR`, `/clear` (se ignora un `;` final opcional). Si la salida estándar no es un terminal (por ejemplo, al enviar la salida por una tubería), el metacomando se acepta cuando se reconoce, pero no emite secuencias de control.

Con `clickhouse-local` y `-q`:

```sh
./clickhouse-local -q clear
```

<div id="examples">
  ## Ejemplos
</div>

```bash title="Query"
$ echo -e "1,2\n3,4" | clickhouse-local --structure "a Int64, b Int64" \
    --input-format "CSV" --query "SELECT * FROM table"
Read 2 rows, 32.00 B in 0.000 sec., 5182 rows/sec., 80.97 KiB/sec.
1   2
3   4
```

El ejemplo anterior es el mismo que:

```bash title="Query"
$ echo -e "1,2\n3,4" | clickhouse-local -n --query "
    CREATE TABLE table (a Int64, b Int64) ENGINE = File(CSV, stdin);
    SELECT a, b FROM table;
    DROP TABLE table;"
Read 2 rows, 32.00 B in 0.000 sec., 4987 rows/sec., 77.93 KiB/sec.
1   2
3   4
```

No es necesario usar `stdin` ni el argumento `--file`, y puedes abrir cualquier cantidad de archivos con la [función de tabla `file`](../../sql-reference/table-functions/file.md):

```bash title="Query"
$ echo 1 | tee 1.tsv
1

$ echo 2 | tee 2.tsv
2

$ clickhouse-local --query "
    select * from file('1.tsv', TSV, 'a int') t1
    cross join file('2.tsv', TSV, 'b int') t2"
1    2
```

Ahora mostremos el usuario memory de cada usuario de Unix:

```bash title="Query"
$ ps aux | tail -n +2 | awk '{ printf("%s\t%s\n", $1, $4) }' \
    | clickhouse-local --structure "user String, mem Float64" \
        --query "SELECT user, round(sum(mem), 2) as memTotal
            FROM table GROUP BY user ORDER BY memTotal DESC FORMAT Pretty"
```

```text title="Response"
Read 186 rows, 4.15 KiB in 0.035 sec., 5302 rows/sec., 118.34 KiB/sec.
┏━━━━━━━━━━┳━━━━━━━━━━┓
┃ user     ┃ memTotal ┃
┡━━━━━━━━━━╇━━━━━━━━━━┩
│ bayonet  │    113.5 │
├──────────┼──────────┤
│ root     │      8.8 │
├──────────┼──────────┤
...
```

<div id="starting-listeners">
  ## Inicio de listeners TCP y HTTP
</div>

`clickhouse-local` puede convertirse en un servidor ligero que acepta conexiones TCP (protocolo nativo) y HTTP. Esto resulta útil cuando desea dar a otras herramientas o aplicaciones de ClickHouse acceso a las bases de datos y tablas de una instancia de `clickhouse-local` en ejecución. Tenga en cuenta que cada conexión entrante obtiene su propia sesión: las tablas temporales y la configuración a nivel de sesión de la sesión interactiva de `clickhouse-local` no son visibles para las conexiones externas.

Use `SYSTEM START LISTEN` para abrir un listener y `SYSTEM STOP LISTEN` para cerrarlo:

```bash
clickhouse-local \
    --listen_host 127.0.0.1 \
    --tcp_port 9000 \
    --http_port 8123 \
    --query "
        SYSTEM START LISTEN TCP;
        SYSTEM START LISTEN HTTP;
        SELECT * FROM url('http://127.0.0.1:8123/?query=SELECT+42', LineAsString);
        SYSTEM STOP LISTEN TCP;
        SYSTEM STOP LISTEN HTTP;
    "
```

Las opciones `--listen_host`, `--tcp_port` y `--http_port` configuran la dirección de escucha y los puertos. Los puertos predeterminados son `9000` para TCP y `8123` para HTTP.

:::warning Seguridad
De forma predeterminada, `clickhouse-local` se ejecuta con una configuración temporal de usuarios, por lo que cualquier listener que abra queda sin autenticación. Vincúlelo a una dirección de loopback (`127.0.0.1` o `::1`), a menos que haya configurado explícitamente usuarios y control de acceso haciendo que la opción `users_config` apunte a un `users.xml` personalizado (por ejemplo, mediante `--config-file`). Escuchar en una dirección distinta de loopback sin autenticación expone los datos de la instancia local a cualquiera que pueda acceder al puerto elegido.
:::

<div id="related-content-1">
  ## Contenido relacionado
</div>

* [Extraer, convertir y consultar datos en archivos locales con clickhouse-local](https://clickhouse.com/blog/extracting-converting-querying-local-files-with-sql-clickhouse-local)
* [Ingesta de datos en ClickHouse - Parte 1](https://clickhouse.com/blog/getting-data-into-clickhouse-part-1)
* [Explorando conjuntos de datos masivos del mundo real: más de 100 años de registros meteorológicos en ClickHouse](https://clickhouse.com/blog/real-world-data-noaa-climate-data)
* Blog: [Extraer, convertir y consultar datos en archivos locales con clickhouse-local](https://clickhouse.com/blog/extracting-converting-querying-local-files-with-sql-clickhouse-local)