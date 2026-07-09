---
description: 'Página que describe la inferencia automática del esquema a partir de los datos de entrada en ClickHouse'
sidebar_label: 'Inferencia de esquemas'
slug: /interfaces/schema-inference
title: 'Inferencia automática del esquema a partir de los datos de entrada'
doc_type: 'reference'
---

ClickHouse puede determinar automáticamente la estructura de los datos de entrada en casi todos los [formatos de entrada](formats.md) compatibles.
En este documento se describe cuándo se usa la inferencia de esquemas, cómo funciona con distintos formatos de entrada y qué ajustes
pueden controlarla.

<div id="usage">
  ## Uso
</div>

La inferencia de esquemas se utiliza cuando ClickHouse necesita leer datos en un formato de datos específico y se desconoce la estructura.

<div id="table-functions-file-s3-url-hdfs-azureblobstorage">
  ## Funciones de tabla [file](../sql-reference/table-functions/file.md), [s3](../sql-reference/table-functions/s3.md), [url](../sql-reference/table-functions/url.md), [hdfs](../sql-reference/table-functions/hdfs.md), [azureBlobStorage](../sql-reference/table-functions/azureBlobStorage.md).
</div>

Estas funciones de tabla tienen el argumento opcional `structure`, con la estructura de los datos de entrada. Si no se especifica este argumento o se establece en `auto`, la estructura se inferirá de los datos.

**Ejemplo:**

Supongamos que tenemos un archivo `hobbies.jsonl` en formato JSONEachRow en el directorio `user_files` con este contenido:

```json
{"id" :  1, "age" :  25, "name" :  "Josh", "hobbies" :  ["football", "cooking", "music"]}
{"id" :  2, "age" :  19, "name" :  "Alan", "hobbies" :  ["tennis", "art"]}
{"id" :  3, "age" :  32, "name" :  "Lana", "hobbies" :  ["fitness", "reading", "shopping"]}
{"id" :  4, "age" :  47, "name" :  "Brayan", "hobbies" :  ["movies", "skydiving"]}
```

ClickHouse puede leer estos datos sin que tengas que especificar su estructura:

```sql
SELECT * FROM file('hobbies.jsonl')
```

```response
┌─id─┬─age─┬─name───┬─hobbies──────────────────────────┐
│  1 │  25 │ Josh   │ ['football','cooking','music']   │
│  2 │  19 │ Alan   │ ['tennis','art']                 │
│  3 │  32 │ Lana   │ ['fitness','reading','shopping'] │
│  4 │  47 │ Brayan │ ['movies','skydiving']           │
└────┴─────┴────────┴──────────────────────────────────┘
```

Nota: el formato `JSONEachRow` se determinó automáticamente por la extensión de archivo `.jsonl`.

Puede ver la estructura determinada automáticamente usando la consulta `DESCRIBE`:

```sql
DESCRIBE file('hobbies.jsonl')
```

```response
┌─name────┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ id      │ Nullable(Int64)         │              │                    │         │                  │                │
│ age     │ Nullable(Int64)         │              │                    │         │                  │                │
│ name    │ Nullable(String)        │              │                    │         │                  │                │
│ hobbies │ Array(Nullable(String)) │              │                    │         │                  │                │
└─────────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="table-engines-file-s3-url-hdfs-azureblobstorage">
  ## Motores de tabla [File](../engines/table-engines/special/file.md), [S3](../engines/table-engines/integrations/s3.md), [URL](../engines/table-engines/special/url.md), [HDFS](../engines/table-engines/integrations/hdfs.md), [azureBlobStorage](../engines/table-engines/integrations/azureBlobStorage.md)
</div>

Si no se especifica la lista de columnas en la consulta `CREATE TABLE`, la estructura de la tabla se inferirá automáticamente a partir de los datos.

**Ejemplo:**

Vamos a usar el archivo `hobbies.jsonl`. Podemos crear una tabla con el motor `File` usando los datos de este archivo:

```sql
CREATE TABLE hobbies ENGINE=File(JSONEachRow, 'hobbies.jsonl')
```

```response
Ok.
```

```sql
SELECT * FROM hobbies
```

```response
┌─id─┬─age─┬─name───┬─hobbies──────────────────────────┐
│  1 │  25 │ Josh   │ ['football','cooking','music']   │
│  2 │  19 │ Alan   │ ['tennis','art']                 │
│  3 │  32 │ Lana   │ ['fitness','reading','shopping'] │
│  4 │  47 │ Brayan │ ['movies','skydiving']           │
└────┴─────┴────────┴──────────────────────────────────┘
```

```sql
DESCRIBE TABLE hobbies
```

```response
┌─name────┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ id      │ Nullable(Int64)         │              │                    │         │                  │                │
│ age     │ Nullable(Int64)         │              │                    │         │                  │                │
│ name    │ Nullable(String)        │              │                    │         │                  │                │
│ hobbies │ Array(Nullable(String)) │              │                    │         │                  │                │
└─────────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="clickhouse-local">
  ## clickhouse-local
</div>

`clickhouse-local` tiene un parámetro opcional `-S/--structure` que indica la estructura de los datos de entrada. Si este parámetro no se especifica o se establece en `auto`, la estructura se inferirá a partir de los datos.

**Ejemplo:**

Usemos el archivo `hobbies.jsonl`. Podemos consultar los datos de este archivo con `clickhouse-local`:

```shell
clickhouse-local --file='hobbies.jsonl' --table='hobbies' --query='DESCRIBE TABLE hobbies'
```

```response
id    Nullable(Int64)
age    Nullable(Int64)
name    Nullable(String)
hobbies    Array(Nullable(String))
```

```shell
clickhouse-local --file='hobbies.jsonl' --table='hobbies' --query='SELECT * FROM hobbies'
```

```response
1    25    Josh    ['football','cooking','music']
2    19    Alan    ['tennis','art']
3    32    Lana    ['fitness','reading','shopping']
4    47    Brayan    ['movies','skydiving']
```

<div id="using-structure-from-insertion-table">
  ## Uso de la estructura de la tabla de inserción
</div>

Cuando las funciones de tabla `file/s3/url/hdfs` se usan para insertar datos en una tabla,
existe la opción de usar la estructura de la tabla de inserción en lugar de extraerla de los datos.
Esto puede mejorar el rendimiento de la inserción, ya que la inferencia de esquemas puede llevar algo de tiempo. Además, resulta útil cuando la tabla tiene un esquema optimizado, de modo que
no se realizarán conversiones entre tipos.

Existe una configuración especial [use&#95;structure&#95;from&#95;insertion&#95;table&#95;in&#95;table&#95;functions](/es/operations/settings/settings.md/#use_structure_from_insertion_table_in_table_functions)
que controla este comportamiento. Tiene 3 posibles valores:

* 0 - la función de tabla extraerá la estructura de los datos.
* 1 - la función de tabla usará la estructura de la tabla de inserción.
* 2 - ClickHouse determinará automáticamente si es posible usar la estructura de la tabla de inserción o la inferencia de esquemas. Valor predeterminado.

**Ejemplo 1:**

Vamos a crear la tabla `hobbies1` con la siguiente estructura:

```sql
CREATE TABLE hobbies1
(
    `id` UInt64,
    `age` LowCardinality(UInt8),
    `name` String,
    `hobbies` Array(String)
)
ENGINE = MergeTree
ORDER BY id;
```

E inserte los datos del archivo `hobbies.jsonl`:

```sql
INSERT INTO hobbies1 SELECT * FROM file(hobbies.jsonl)
```

En este caso, todas las columnas del archivo se insertan en la tabla sin cambios, por lo que ClickHouse usará la estructura de la tabla de destino de la inserción en lugar de la inferencia de esquemas.

**Ejemplo 2:**

Vamos a crear la tabla `hobbies2` con la siguiente estructura:

```sql
CREATE TABLE hobbies2
(
  `id` UInt64,
  `age` LowCardinality(UInt8),
  `hobbies` Array(String)
)
  ENGINE = MergeTree
ORDER BY id;
```

E inserta datos del archivo `hobbies.jsonl`:

```sql
INSERT INTO hobbies2 SELECT id, age, hobbies FROM file(hobbies.jsonl)
```

En este caso, todas las columnas de la consulta `SELECT` están presentes en la tabla, por lo que ClickHouse utilizará la estructura de la tabla de inserción.
Ten en cuenta que esto solo funcionará con formatos de entrada que admitan leer un subconjunto de columnas, como JSONEachRow, TSKV, Parquet, etc. (por lo que, por ejemplo, no funcionará con el formato TSV).

**Ejemplo 3:**

Vamos a crear la tabla `hobbies3` con la siguiente estructura:

```sql
CREATE TABLE hobbies3
(
  `identifier` UInt64,
  `age` LowCardinality(UInt8),
  `hobbies` Array(String)
)
  ENGINE = MergeTree
ORDER BY identifier;
```

E inserta los datos del archivo `hobbies.jsonl`:

```sql
INSERT INTO hobbies3 SELECT id, age, hobbies FROM file(hobbies.jsonl)
```

En este caso, la columna `id` se usa en la consulta `SELECT`, pero la tabla no tiene esa columna (tiene una columna llamada `identifier`),
por lo que ClickHouse no puede usar la estructura de la tabla de inserción y se utilizará la inferencia de esquemas.

**Ejemplo 4:**

Vamos a crear la tabla `hobbies4` con la siguiente estructura:

```sql
CREATE TABLE hobbies4
(
  `id` UInt64,
  `any_hobby` Nullable(String)
)
  ENGINE = MergeTree
ORDER BY id;
```

E inserte los datos del archivo `hobbies.jsonl`:

```sql
INSERT INTO hobbies4 SELECT id, empty(hobbies) ? NULL : hobbies[1] FROM file(hobbies.jsonl)
```

En este caso, se realizan algunas operaciones sobre la columna `hobbies` en la consulta `SELECT` para insertarla en la tabla, por lo que ClickHouse no puede usar la estructura de la tabla de inserción y se utilizará la inferencia de esquemas.

<div id="schema-inference-cache">
  ## Caché de inferencia de esquemas
</div>

Para la mayoría de los formatos de entrada, la inferencia de esquemas lee algunos datos para determinar su estructura, y este proceso puede llevar algo de tiempo.
Para evitar inferir el mismo esquema cada vez que ClickHouse lee datos del mismo archivo, el esquema inferido se almacena en caché y, cuando se vuelve a acceder al mismo archivo, ClickHouse usará el esquema de la caché.

Hay ajustes especiales que controlan esta caché:

* `schema_inference_cache_max_elements_for_{file/s3/hdfs/url/azure}` - el número máximo de esquemas almacenados en caché para la función de tabla correspondiente. El valor predeterminado es `4096`. Estos ajustes deben configurarse en la configuración del servidor.
* `schema_inference_use_cache_for_{file,s3,hdfs,url,azure}` - permite activar o desactivar el uso de la caché para la inferencia de esquemas. Estos ajustes se pueden usar en las consultas.

El esquema del archivo puede cambiar al modificar los datos o al cambiar los ajustes del formato.
Por esta razón, la caché de inferencia de esquemas identifica el esquema por el origen del archivo, el nombre del formato, los ajustes de formato usados y la hora de la última modificación del archivo.

Nota: es posible que algunos archivos a los que se accede mediante URL en la función de tabla `url` no contengan información sobre la hora de la última modificación; para este caso, existe un ajuste especial
`schema_inference_cache_require_modification_time_for_url`. Desactivar este ajuste permite usar el esquema de la caché sin la hora de la última modificación para esos archivos.

También hay una tabla del sistema [schema&#95;inference&#95;cache](../operations/system-tables/schema_inference_cache.md) con todos los esquemas actualmente almacenados en caché y la consulta del sistema `SYSTEM CLEAR SCHEMA CACHE [FOR File/S3/URL/HDFS]`
que permite limpiar la caché de esquemas para todos los orígenes o para un origen específico.

**Ejemplos:**

Intentemos inferir la estructura de un conjunto de datos de ejemplo de s3 `github-2022.ndjson.gz` y veamos cómo funciona la caché de inferencia de esquemas:

```sql
DESCRIBE TABLE s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/github/github-2022.ndjson.gz')
```

```response
┌─name───────┬─type─────────────────────────────────────────┐
│ type       │ Nullable(String)                             │
│ actor      │ Tuple(                                      ↴│
│            │↳    avatar_url Nullable(String),            ↴│
│            │↳    display_login Nullable(String),         ↴│
│            │↳    id Nullable(Int64),                     ↴│
│            │↳    login Nullable(String),                 ↴│
│            │↳    url Nullable(String))                    │
│ repo       │ Tuple(                                      ↴│
│            │↳    id Nullable(Int64),                     ↴│
│            │↳    name Nullable(String),                  ↴│
│            │↳    url Nullable(String))                    │
│ created_at │ Nullable(String)                             │
│ payload    │ Tuple(                                      ↴│
│            │↳    action Nullable(String),                ↴│
│            │↳    distinct_size Nullable(Int64),          ↴│
│            │↳    pull_request Tuple(                     ↴│
│            │↳        author_association Nullable(String),↴│
│            │↳        base Tuple(                         ↴│
│            │↳            ref Nullable(String),           ↴│
│            │↳            sha Nullable(String)),          ↴│
│            │↳        head Tuple(                         ↴│
│            │↳            ref Nullable(String),           ↴│
│            │↳            sha Nullable(String)),          ↴│
│            │↳        number Nullable(Int64),             ↴│
│            │↳        state Nullable(String),             ↴│
│            │↳        title Nullable(String),             ↴│
│            │↳        updated_at Nullable(String),        ↴│
│            │↳        user Tuple(                         ↴│
│            │↳            login Nullable(String))),       ↴│
│            │↳    ref Nullable(String),                   ↴│
│            │↳    ref_type Nullable(String),              ↴│
│            │↳    size Nullable(Int64))                    │
└────────────┴──────────────────────────────────────────────┘
5 rows in set. Elapsed: 0.601 sec.
```

```sql
DESCRIBE TABLE s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/github/github-2022.ndjson.gz')
```

```response
┌─name───────┬─type─────────────────────────────────────────┐
│ type       │ Nullable(String)                             │
│ actor      │ Tuple(                                      ↴│
│            │↳    avatar_url Nullable(String),            ↴│
│            │↳    display_login Nullable(String),         ↴│
│            │↳    id Nullable(Int64),                     ↴│
│            │↳    login Nullable(String),                 ↴│
│            │↳    url Nullable(String))                    │
│ repo       │ Tuple(                                      ↴│
│            │↳    id Nullable(Int64),                     ↴│
│            │↳    name Nullable(String),                  ↴│
│            │↳    url Nullable(String))                    │
│ created_at │ Nullable(String)                             │
│ payload    │ Tuple(                                      ↴│
│            │↳    action Nullable(String),                ↴│
│            │↳    distinct_size Nullable(Int64),          ↴│
│            │↳    pull_request Tuple(                     ↴│
│            │↳        author_association Nullable(String),↴│
│            │↳        base Tuple(                         ↴│
│            │↳            ref Nullable(String),           ↴│
│            │↳            sha Nullable(String)),          ↴│
│            │↳        head Tuple(                         ↴│
│            │↳            ref Nullable(String),           ↴│
│            │↳            sha Nullable(String)),          ↴│
│            │↳        number Nullable(Int64),             ↴│
│            │↳        state Nullable(String),             ↴│
│            │↳        title Nullable(String),             ↴│
│            │↳        updated_at Nullable(String),        ↴│
│            │↳        user Tuple(                         ↴│
│            │↳            login Nullable(String))),       ↴│
│            │↳    ref Nullable(String),                   ↴│
│            │↳    ref_type Nullable(String),              ↴│
│            │↳    size Nullable(Int64))                    │
└────────────┴──────────────────────────────────────────────┘

5 rows in set. Elapsed: 0.059 sec.
```

Como puede ver, la segunda consulta se completó casi al instante.

Intentemos cambiar algunas opciones de configuración que pueden afectar al esquema inferido:

```sql
DESCRIBE TABLE s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/github/github-2022.ndjson.gz')
SETTINGS input_format_json_try_infer_named_tuples_from_objects=0, input_format_json_read_objects_as_strings = 1

┌─name───────┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ type       │ Nullable(String) │              │                    │         │                  │                │
│ actor      │ Nullable(String) │              │                    │         │                  │                │
│ repo       │ Nullable(String) │              │                    │         │                  │                │
│ created_at │ Nullable(String) │              │                    │         │                  │                │
│ payload    │ Nullable(String) │              │                    │         │                  │                │
└────────────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘

5 rows in set. Elapsed: 0.611 sec
```

Como puedes ver, no se utilizó el esquema de la caché para el mismo archivo porque se cambió la configuración que puede afectar al esquema inferido.

Veamos el contenido de la tabla `system.schema_inference_cache`:

```sql
SELECT schema, format, source FROM system.schema_inference_cache WHERE storage='S3'
```

```response
┌─schema──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─format─┬─source───────────────────────────────────────────────────────────────────────────────────────────────────┐
│ type Nullable(String), actor Tuple(avatar_url Nullable(String), display_login Nullable(String), id Nullable(Int64), login Nullable(String), url Nullable(String)), repo Tuple(id Nullable(Int64), name Nullable(String), url Nullable(String)), created_at Nullable(String), payload Tuple(action Nullable(String), distinct_size Nullable(Int64), pull_request Tuple(author_association Nullable(String), base Tuple(ref Nullable(String), sha Nullable(String)), head Tuple(ref Nullable(String), sha Nullable(String)), number Nullable(Int64), state Nullable(String), title Nullable(String), updated_at Nullable(String), user Tuple(login Nullable(String))), ref Nullable(String), ref_type Nullable(String), size Nullable(Int64)) │ NDJSON │ datasets-documentation.s3.eu-west-3.amazonaws.com443/datasets-documentation/github/github-2022.ndjson.gz │
│ type Nullable(String), actor Nullable(String), repo Nullable(String), created_at Nullable(String), payload Nullable(String)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 │ NDJSON │ datasets-documentation.s3.eu-west-3.amazonaws.com443/datasets-documentation/github/github-2022.ndjson.gz │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴────────┴──────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

Como puede ver, hay dos esquemas diferentes para el mismo archivo.

Podemos borrar la caché de esquemas con una consulta de sistema:

```sql
SYSTEM CLEAR SCHEMA CACHE FOR S3
```

```response
Ok.
```

```sql
SELECT count() FROM system.schema_inference_cache WHERE storage='S3'
```

```response
┌─count()─┐
│       0 │
└─────────┘
```

<div id="text-formats">
  ## Formatos de texto
</div>

En los formatos de texto, ClickHouse lee los datos fila por fila, extrae los valores de las columnas según el formato
y luego utiliza analizadores sintácticos recursivos y heurísticas para determinar el tipo de cada valor. El número máximo de filas y bytes leídos de los datos durante la inferencia de esquemas
se controla mediante los ajustes `input_format_max_rows_to_read_for_schema_inference` (25000 de forma predeterminada) e `input_format_max_bytes_to_read_for_schema_inference` (32Mb de forma predeterminada).
De forma predeterminada, todos los tipos inferidos son [Nullable](../sql-reference/data-types/nullable.md), pero puede cambiarlo configurando `schema_inference_make_columns_nullable` (consulte ejemplos en la sección de [ajustes](#settings-for-text-formats)).

<div id="json-formats">
  ### Formatos JSON
</div>

En los formatos JSON, ClickHouse analiza los valores según la especificación JSON e intenta determinar el tipo de dato más adecuado para cada uno.

Veamos cómo funciona, qué tipos se pueden inferir y qué configuraciones específicas se pueden utilizar en los formatos JSON.

**Ejemplos**

En este apartado y en los siguientes, se utilizará la función de tabla [format](../sql-reference/table-functions/format.md) en los ejemplos.

Integers, Floats, Bools, Strings:

```sql
DESC format(JSONEachRow, '{"int" : 42, "float" : 42.42, "string" : "Hello, World!"}');
```

```response
┌─name───┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ int    │ Nullable(Int64)   │              │                    │         │                  │                │
│ float  │ Nullable(Float64) │              │                    │         │                  │                │
│ bool   │ Nullable(Bool)    │              │                    │         │                  │                │
│ string │ Nullable(String)  │              │                    │         │                  │                │
└────────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Dates, DateTimes:

```sql
DESC format(JSONEachRow, '{"date" : "2022-01-01", "datetime" : "2022-01-01 00:00:00", "datetime64" : "2022-01-01 00:00:00.000"}')
```

```response
┌─name───────┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ date       │ Nullable(Date)          │              │                    │         │                  │                │
│ datetime   │ Nullable(DateTime)      │              │                    │         │                  │                │
│ datetime64 │ Nullable(DateTime64(9)) │              │                    │         │                  │                │
└────────────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Arrays:

```sql
DESC format(JSONEachRow, '{"arr" : [1, 2, 3], "nested_arrays" : [[1, 2, 3], [4, 5, 6], []]}')
```

```response
┌─name──────────┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ arr           │ Array(Nullable(Int64))        │              │                    │         │                  │                │
│ nested_arrays │ Array(Array(Nullable(Int64))) │              │                    │         │                  │                │
└───────────────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Si un array contiene `null`, ClickHouse utilizará los tipos de los demás elementos del array:

```sql
DESC format(JSONEachRow, '{"arr" : [null, 42, null]}')
```

```response
┌─name─┬─type───────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ arr  │ Array(Nullable(Int64)) │              │                    │         │                  │                │
└──────┴────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Si un array contiene valores de diferentes tipos y el parámetro `input_format_json_infer_array_of_dynamic_from_array_of_different_types` está habilitado (habilitado por defecto), tendrá el tipo `Array(Dynamic)`:

```sql
SET input_format_json_infer_array_of_dynamic_from_array_of_different_types=1;
DESC format(JSONEachRow, '{"arr" : [42, "hello", [1, 2, 3]]}');
```

```response
┌─name─┬─type───────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ arr  │ Array(Dynamic) │              │                    │         │                  │                │
└──────┴────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Tuplas con nombre:

Cuando está habilitada la configuración `input_format_json_try_infer_named_tuples_from_objects`, ClickHouse intentará inferir un Tuple con nombre a partir de objetos JSON durante la inferencia del esquema.
El Tuple con nombre resultante contendrá todos los elementos de todos los objetos JSON correspondientes presentes en los datos de muestra.

```sql
SET input_format_json_try_infer_named_tuples_from_objects = 1;
DESC format(JSONEachRow, '{"obj" : {"a" : 42, "b" : "Hello"}}, {"obj" : {"a" : 43, "c" : [1, 2, 3]}}, {"obj" : {"d" : {"e" : 42}}}')
```

```response
┌─name─┬─type───────────────────────────────────────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ obj  │ Tuple(a Nullable(Int64), b Nullable(String), c Array(Nullable(Int64)), d Tuple(e Nullable(Int64))) │              │                    │         │                  │                │
└──────┴────────────────────────────────────────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Tuples sin nombre:

Si la configuración `input_format_json_infer_array_of_dynamic_from_array_of_different_types` está desactivada, consideramos los Arrays con elementos de distintos tipos como Tuples sin nombre en los formatos JSON.

```sql
SET input_format_json_infer_array_of_dynamic_from_array_of_different_types = 0;
DESC format(JSONEachRow, '{"tuple" : [1, "Hello, World!", [1, 2, 3]]}')
```

```response
┌─name──┬─type─────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ tuple │ Tuple(Nullable(Int64), Nullable(String), Array(Nullable(Int64))) │              │                    │         │                  │                │
└───────┴──────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Si algunos valores son `null` o están vacíos, tomamos los tipos de los valores correspondientes de las demás filas:

```sql
SET input_format_json_infer_array_of_dynamic_from_array_of_different_types=0;
DESC format(JSONEachRow, $$
                              {"tuple" : [1, null, null]}
                              {"tuple" : [null, "Hello, World!", []]}
                              {"tuple" : [null, null, [1, 2, 3]]}
                         $$)
```

```response
┌─name──┬─type─────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ tuple │ Tuple(Nullable(Int64), Nullable(String), Array(Nullable(Int64))) │              │                    │         │                  │                │
└───────┴──────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Mapas:

En JSON, podemos leer objetos con valores del mismo tipo que el tipo Map.
Nota: esto solo funcionará cuando las opciones `input_format_json_read_objects_as_strings` e `input_format_json_try_infer_named_tuples_from_objects` estén deshabilitadas.

```sql
SET input_format_json_read_objects_as_strings = 0, input_format_json_try_infer_named_tuples_from_objects = 0;
DESC format(JSONEachRow, '{"map" : {"key1" : 42, "key2" : 24, "key3" : 4}}')
```

```response
┌─name─┬─type─────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ map  │ Map(String, Nullable(Int64)) │              │                    │         │                  │                │
└──────┴──────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Tipos complejos Nested:

```sql
DESC format(JSONEachRow, '{"value" : [[[42, 24], []], {"key1" : 42, "key2" : 24}]}')
```

```response
┌─name──┬─type─────────────────────────────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ value │ Tuple(Array(Array(Nullable(String))), Tuple(key1 Nullable(Int64), key2 Nullable(Int64))) │              │                    │         │                  │                │
└───────┴──────────────────────────────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Si ClickHouse no puede determinar el tipo de alguna clave porque los datos solo contienen NULL/objetos vacíos/arrays vacíos, se usará el tipo `String` si la configuración `input_format_json_infer_incomplete_types_as_strings` está habilitada; de lo contrario, se lanzará una excepción:

```sql
DESC format(JSONEachRow, '{"arr" : [null, null]}') SETTINGS input_format_json_infer_incomplete_types_as_strings = 1;
```

```response
┌─name─┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ arr  │ Array(Nullable(String)) │              │                    │         │                  │                │
└──────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
DESC format(JSONEachRow, '{"arr" : [null, null]}') SETTINGS input_format_json_infer_incomplete_types_as_strings = 0;
```

```response
Code: 652. DB::Exception: Received from localhost:9000. DB::Exception:
Cannot determine type for column 'arr' by first 1 rows of data,
most likely this column contains only Nulls or empty Arrays/Maps.
...
```

<div id="json-settings">
  #### Configuración de JSON
</div>

<div id="input_format_json_try_infer_numbers_from_strings">
  ##### input_format_json_try_infer_numbers_from_strings
</div>

Al habilitar esta configuración, se pueden inferir números a partir de valores de tipo String.

Esta configuración está deshabilitada de forma predeterminada.

**Ejemplo:**

```sql
SET input_format_json_try_infer_numbers_from_strings = 1;
DESC format(JSONEachRow, $$
                              {"value" : "42"}
                              {"value" : "424242424242"}
                         $$)
```

```response
┌─name──┬─type────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ value │ Nullable(Int64) │              │                    │         │                  │                │
└───────┴─────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="input_format_json_try_infer_named_tuples_from_objects">
  ##### input_format_json_try_infer_named_tuples_from_objects
</div>

Al habilitar esta configuración, se pueden inferir Tuples con nombre a partir de objetos JSON. El Tuple con nombre resultante contendrá todos los elementos de todos los objetos JSON correspondientes de la muestra de datos.
Puede resultar útil cuando los datos JSON no son dispersos, de modo que la muestra de datos contenga todas las claves de objeto posibles.

Esta configuración está habilitada de forma predeterminada.

**Ejemplo**

```sql title="Query"
SET input_format_json_try_infer_named_tuples_from_objects = 1;
DESC format(JSONEachRow, '{"obj" : {"a" : 42, "b" : "Hello"}}, {"obj" : {"a" : 43, "c" : [1, 2, 3]}}, {"obj" : {"d" : {"e" : 42}}}')
```

```response title="Response"
┌─name─┬─type───────────────────────────────────────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ obj  │ Tuple(a Nullable(Int64), b Nullable(String), c Array(Nullable(Int64)), d Tuple(e Nullable(Int64))) │              │                    │         │                  │                │
└──────┴────────────────────────────────────────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql title="Query"
SET input_format_json_try_infer_named_tuples_from_objects = 1;
DESC format(JSONEachRow, '{"array" : [{"a" : 42, "b" : "Hello"}, {}, {"c" : [1,2,3]}, {"d" : "2020-01-01"}]}')
```

```markdown title="Response"
┌─name──┬─type────────────────────────────────────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ array │ Array(Tuple(a Nullable(Int64), b Nullable(String), c Array(Nullable(Int64)), d Nullable(Date))) │              │                    │         │                  │                │
└───────┴─────────────────────────────────────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="input_format_json_use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects">
  ##### input_format_json_use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects
</div>

Al habilitar esta configuración, se permite usar el tipo String para las rutas ambiguas durante la inferencia de tuplas con nombre a partir de objetos JSON (cuando `input_format_json_try_infer_named_tuples_from_objects` está habilitada), en lugar de generar una excepción.
Permite leer objetos JSON como Tuples con nombre incluso cuando hay rutas ambiguas.

Deshabilitada de forma predeterminada.

**Ejemplos**

Con la configuración deshabilitada:

```sql title="Query"
SET input_format_json_try_infer_named_tuples_from_objects = 1;
SET input_format_json_use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects = 0;
DESC format(JSONEachRow, '{"obj" : {"a" : 42}}, {"obj" : {"a" : {"b" : "Hello"}}}');
```

```response title="Response"
Code: 636. DB::Exception: The table structure cannot be extracted from a JSONEachRow format file. Error:
Code: 117. DB::Exception: JSON objects have ambiguous data: in some objects path 'a' has type 'Int64' and in some - 'Tuple(b String)'. You can enable setting input_format_json_use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects to use String type for path 'a'. (INCORRECT_DATA) (version 24.3.1.1).
You can specify the structure manually. (CANNOT_EXTRACT_TABLE_STRUCTURE)
```

Con la opción habilitada:

```sql title="Query"
SET input_format_json_try_infer_named_tuples_from_objects = 1;
SET input_format_json_use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects = 1;
DESC format(JSONEachRow, '{"obj" : "a" : 42}, {"obj" : {"a" : {"b" : "Hello"}}}');
SELECT * FROM format(JSONEachRow, '{"obj" : {"a" : 42}}, {"obj" : {"a" : {"b" : "Hello"}}}');
```

```response title="Response"
┌─name─┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ obj  │ Tuple(a Nullable(String))     │              │                    │         │                  │                │
└──────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
┌─obj─────────────────┐
│ ('42')              │
│ ('{"b" : "Hello"}') │
└─────────────────────┘
```

<div id="input_format_json_read_objects_as_strings">
  ##### input_format_json_read_objects_as_strings
</div>

Al habilitar esta configuración, se pueden leer objetos JSON anidados como cadenas.
Esta configuración puede usarse para leer objetos JSON anidados sin utilizar el tipo de objeto JSON.

Esta configuración está habilitada de forma predeterminada.

Nota: esta configuración solo surtirá efecto si la configuración `input_format_json_try_infer_named_tuples_from_objects` está deshabilitada.

```sql
SET input_format_json_read_objects_as_strings = 1, input_format_json_try_infer_named_tuples_from_objects = 0;
DESC format(JSONEachRow, $$
                             {"obj" : {"key1" : 42, "key2" : [1,2,3,4]}}
                             {"obj" : {"key3" : {"nested_key" : 1}}}
                         $$)
```

```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ obj  │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="input_format_json_read_numbers_as_strings">
  ##### input_format_json_read_numbers_as_strings
</div>

Al habilitar esta configuración, se pueden leer los valores numéricos como cadenas.

Esta configuración está habilitada de forma predeterminada.

**Ejemplo**

```sql
SET input_format_json_read_numbers_as_strings = 1;
DESC format(JSONEachRow, $$
                                {"value" : 1055}
                                {"value" : "unknown"}
                         $$)
```

```response
┌─name──┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ value │ Nullable(String) │              │                    │         │                  │                │
└───────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="input_format_json_read_bools_as_numbers">
  ##### input_format_json_read_bools_as_numbers
</div>

Al habilitar esta opción, se pueden leer valores Bool como números.

Esta opción está habilitada de forma predeterminada.

**Ejemplo:**

```sql
SET input_format_json_read_bools_as_numbers = 1;
DESC format(JSONEachRow, $$
                                {"value" : true}
                                {"value" : 42}
                         $$)
```

```response
┌─name──┬─type────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ value │ Nullable(Int64) │              │                    │         │                  │                │
└───────┴─────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="input_format_json_read_bools_as_strings">
  ##### input_format_json_read_bools_as_strings
</div>

Al habilitar esta configuración, se pueden leer valores Bool como cadenas.

Esta configuración está habilitada de forma predeterminada.

**Ejemplo:**

```sql
SET input_format_json_read_bools_as_strings = 1;
DESC format(JSONEachRow, $$
                                {"value" : true}
                                {"value" : "Hello, World"}
                         $$)
```

```response
┌─name──┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ value │ Nullable(String) │              │                    │         │                  │                │
└───────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="input_format_json_read_arrays_as_strings">
  ##### input_format_json_read_arrays_as_strings
</div>

Al activar esta configuración, se pueden leer los valores de los arrays JSON como cadenas.

Esta configuración está activada de forma predeterminada.

**Ejemplo**

```sql
SET input_format_json_read_arrays_as_strings = 1;
SELECT arr, toTypeName(arr), JSONExtractArrayRaw(arr)[3] from format(JSONEachRow, 'arr String', '{"arr" : [1, "Hello", [1,2,3]]}');
```

```response
┌─arr───────────────────┬─toTypeName(arr)─┬─arrayElement(JSONExtractArrayRaw(arr), 3)─┐
│ [1, "Hello", [1,2,3]] │ String          │ [1,2,3]                                   │
└───────────────────────┴─────────────────┴───────────────────────────────────────────┘
```

<div id="input_format_json_infer_incomplete_types_as_strings">
  ##### input_format_json_infer_incomplete_types_as_strings
</div>

Al habilitar esta configuración, se puede usar el tipo String para las claves de JSON que contienen únicamente `Null`/`{}`/`[]` en la muestra de datos durante la inferencia de esquemas.
En los formatos JSON, cualquier valor puede leerse como String si todas las configuraciones correspondientes están habilitadas (todas lo están de forma predeterminada), y así se pueden evitar errores como `Cannot determine type for column 'column_name' by first 25000 rows of data, most likely this column contains only Nulls or empty Arrays/Maps` durante la inferencia de esquemas
usando el tipo String para las claves con tipos desconocidos.

Ejemplo:

```sql title="Query"
SET input_format_json_infer_incomplete_types_as_strings = 1, input_format_json_try_infer_named_tuples_from_objects = 1;
DESCRIBE format(JSONEachRow, '{"obj" : {"a" : [1,2,3], "b" : "hello", "c" : null, "d" : {}, "e" : []}}');
SELECT * FROM format(JSONEachRow, '{"obj" : {"a" : [1,2,3], "b" : "hello", "c" : null, "d" : {}, "e" : []}}');
```

```markdown title="Response"
┌─name─┬─type───────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ obj  │ Tuple(a Array(Nullable(Int64)), b Nullable(String), c Nullable(String), d Nullable(String), e Array(Nullable(String))) │              │                    │         │                  │                │
└──────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘

┌─obj────────────────────────────┐
│ ([1,2,3],'hello',NULL,'{}',[]) │
└────────────────────────────────┘
```

<div id="csv">
  ### CSV
</div>

En el formato CSV, ClickHouse extrae los valores de las columnas de la fila según los delimitadores. ClickHouse espera que todos los tipos, excepto los números y las cadenas, estén entre comillas dobles. Si el valor está entre comillas dobles, ClickHouse intenta analizar
los datos dentro de las comillas mediante el analizador recursivo y luego intenta encontrar el tipo de dato más adecuado para ellos. Si el valor no está entre comillas dobles, ClickHouse intenta analizarlo como un número
y, si el valor no es un número, ClickHouse lo trata como una cadena.

Si no quieres que ClickHouse intente determinar tipos complejos usando algunos analizadores y heurísticas, puedes deshabilitar la configuración `input_format_csv_use_best_effort_in_schema_inference`
y ClickHouse tratará todas las columnas como Strings.

Si la configuración `input_format_csv_detect_header` está habilitada, ClickHouse intentará detectar el encabezado con los nombres de las columnas (y quizá los tipos) mientras infiere el esquema. Esta configuración está habilitada de forma predeterminada.

**Ejemplos:**

Enteros, Floats, Bools, Strings:

```sql
DESC format(CSV, '42,42.42,true,"Hello,World!"')
```

```response
┌─name─┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Int64)   │              │                    │         │                  │                │
│ c2   │ Nullable(Float64) │              │                    │         │                  │                │
│ c3   │ Nullable(Bool)    │              │                    │         │                  │                │
│ c4   │ Nullable(String)  │              │                    │         │                  │                │
└──────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Cadenas sin comillas:

```sql
DESC format(CSV, 'Hello world!,World hello!')
```

```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(String) │              │                    │         │                  │                │
│ c2   │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Dates, DateTimes:

```sql
DESC format(CSV, '"2020-01-01","2020-01-01 00:00:00","2022-01-01 00:00:00.000"')
```

```response
┌─name─┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Date)          │              │                    │         │                  │                │
│ c2   │ Nullable(DateTime)      │              │                    │         │                  │                │
│ c3   │ Nullable(DateTime64(9)) │              │                    │         │                  │                │
└──────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Arrays:

```sql
DESC format(CSV, '"[1,2,3]","[[1, 2], [], [3, 4]]"')
```

```response
┌─name─┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Nullable(Int64))        │              │                    │         │                  │                │
│ c2   │ Array(Array(Nullable(Int64))) │              │                    │         │                  │                │
└──────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
DESC format(CSV, $$"['Hello', 'world']","[['Abc', 'Def'], []]"$$)
```

```response
┌─name─┬─type───────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Nullable(String))        │              │                    │         │                  │                │
│ c2   │ Array(Array(Nullable(String))) │              │                    │         │                  │                │
└──────┴────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Si un array contiene NULL, ClickHouse usará los tipos de los demás elementos del array:

```sql
DESC format(CSV, '"[NULL, 42, NULL]"')
```

```response
┌─name─┬─type───────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Nullable(Int64)) │              │                    │         │                  │                │
└──────┴────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Maps:

```sql
DESC format(CSV, $$"{'key1' : 42, 'key2' : 24}"$$)
```

```response
┌─name─┬─type─────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Map(String, Nullable(Int64)) │              │                    │         │                  │                │
└──────┴──────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Nested Arrays and Maps:

```sql
DESC format(CSV, $$"[{'key1' : [[42, 42], []], 'key2' : [[null], [42]]}]"$$)
```

```response
┌─name─┬─type──────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Map(String, Array(Array(Nullable(Int64))))) │              │                    │         │                  │                │
└──────┴───────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Si ClickHouse no puede determinar el tipo entre comillas porque los datos contienen solo NULL, lo tratará como String:

```sql
DESC format(CSV, '"[NULL, NULL]"')
```

```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Ejemplo con la opción `input_format_csv_use_best_effort_in_schema_inference` desactivada:

```sql
SET input_format_csv_use_best_effort_in_schema_inference = 0
DESC format(CSV, '"[1,2,3]",42.42,Hello World!')
```

```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(String) │              │                    │         │                  │                │
│ c2   │ Nullable(String) │              │                    │         │                  │                │
│ c3   │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Ejemplos de autodetección del encabezado (cuando `input_format_csv_detect_header` está activado):

Solo nombres:

```sql
SELECT * FROM format(CSV,
$$"number","string","array"
42,"Hello","[1, 2, 3]"
43,"World","[4, 5, 6]"
$$)
```

```response
┌─number─┬─string─┬─array───┐
│     42 │ Hello  │ [1,2,3] │
│     43 │ World  │ [4,5,6] │
└────────┴────────┴─────────┘
```

Nombres y tipos:

```sql
DESC format(CSV,
$$"number","string","array"
"UInt32","String","Array(UInt16)"
42,"Hello","[1, 2, 3]"
43,"World","[4, 5, 6]"
$$)
```

```response
┌─name───┬─type──────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ number │ UInt32        │              │                    │         │                  │                │
│ string │ String        │              │                    │         │                  │                │
│ array  │ Array(UInt16) │              │                    │         │                  │                │
└────────┴───────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Ten en cuenta que el encabezado solo puede detectarse si hay al menos una columna con un tipo distinto de String. Si todas las columnas son de tipo String, no se detecta el encabezado:

```sql
SELECT * FROM format(CSV,
$$"first_column","second_column"
"Hello","World"
"World","Hello"
$$)
```

```response
┌─c1───────────┬─c2────────────┐
│ first_column │ second_column │
│ Hello        │ World         │
│ World        │ Hello         │
└──────────────┴───────────────┘
```

<div id="csv-settings">
  #### Ajustes de CSV
</div>

<div id="input_format_csv_try_infer_numbers_from_strings">
  ##### input_format_csv_try_infer_numbers_from_strings
</div>

Al habilitar esta configuración, se pueden inferir números a partir de valores de tipo cadena.

Esta configuración está deshabilitada de forma predeterminada.

**Ejemplo:**

```sql
SET input_format_json_try_infer_numbers_from_strings = 1;
DESC format(CSV, '42,42.42');
```

```response
┌─name─┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Int64)   │              │                    │         │                  │                │
│ c2   │ Nullable(Float64) │              │                    │         │                  │                │
└──────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="tsv-tskv">
  ### TSV/TSKV
</div>

En los formatos TSV/TSKV, ClickHouse extrae el valor de la columna de la fila según los delimitadores tabulares y luego analiza el valor extraído con
el analizador recursivo para determinar el tipo más apropiado. Si no se puede determinar el tipo, ClickHouse trata este valor como String.

Si no quieres que ClickHouse intente determinar tipos complejos usando algunos analizadores y heurísticas, puedes deshabilitar la configuración `input_format_tsv_use_best_effort_in_schema_inference`
y ClickHouse tratará todas las columnas como Strings.

Si la configuración `input_format_tsv_detect_header` está habilitada, ClickHouse intentará detectar el encabezado con los nombres de las columnas (y quizá los tipos) mientras infiere el esquema. Esta configuración está habilitada de forma predeterminada.

**Ejemplos:**

Enteros, Floats, Bools, Strings:

```sql
DESC format(TSV, '42    42.42    true    Hello,World!')
```

```response
┌─name─┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Int64)   │              │                    │         │                  │                │
│ c2   │ Nullable(Float64) │              │                    │         │                  │                │
│ c3   │ Nullable(Bool)    │              │                    │         │                  │                │
│ c4   │ Nullable(String)  │              │                    │         │                  │                │
└──────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
DESC format(TSKV, 'int=42    float=42.42    bool=true    string=Hello,World!\n')
```

```response
┌─name───┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ int    │ Nullable(Int64)   │              │                    │         │                  │                │
│ float  │ Nullable(Float64) │              │                    │         │                  │                │
│ bool   │ Nullable(Bool)    │              │                    │         │                  │                │
│ string │ Nullable(String)  │              │                    │         │                  │                │
└────────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Fechas, DateTimes:

```sql
DESC format(TSV, '2020-01-01    2020-01-01 00:00:00    2022-01-01 00:00:00.000')
```

```response
┌─name─┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Date)          │              │                    │         │                  │                │
│ c2   │ Nullable(DateTime)      │              │                    │         │                  │                │
│ c3   │ Nullable(DateTime64(9)) │              │                    │         │                  │                │
└──────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Arrays:

```sql
DESC format(TSV, '[1,2,3]    [[1, 2], [], [3, 4]]')
```

```response
┌─name─┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Nullable(Int64))        │              │                    │         │                  │                │
│ c2   │ Array(Array(Nullable(Int64))) │              │                    │         │                  │                │
└──────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
DESC format(TSV, '[''Hello'', ''world'']    [[''Abc'', ''Def''], []]')
```

```response
┌─name─┬─type───────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Nullable(String))        │              │                    │         │                  │                │
│ c2   │ Array(Array(Nullable(String))) │              │                    │         │                  │                │
└──────┴────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Si un array contiene NULL, ClickHouse usará los tipos de los demás elementos del array:

```sql
DESC format(TSV, '[NULL, 42, NULL]')
```

```response
┌─name─┬─type───────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Nullable(Int64)) │              │                    │         │                  │                │
└──────┴────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Tuplas:

```sql
DESC format(TSV, $$(42, 'Hello, world!')$$)
```

```response
┌─name─┬─type─────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Tuple(Nullable(Int64), Nullable(String)) │              │                    │         │                  │                │
└──────┴──────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Maps:

```sql
DESC format(TSV, $${'key1' : 42, 'key2' : 24}$$)
```

```response
┌─name─┬─type─────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Map(String, Nullable(Int64)) │              │                    │         │                  │                │
└──────┴──────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Arrays, Tuples y Maps anidados:

```sql
DESC format(TSV, $$[{'key1' : [(42, 'Hello'), (24, NULL)], 'key2' : [(NULL, ','), (42, 'world!')]}]$$)
```

```response
┌─name─┬─type────────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Map(String, Array(Tuple(Nullable(Int64), Nullable(String))))) │              │                    │         │                  │                │
└──────┴─────────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Si ClickHouse no puede determinar el tipo porque los datos solo contienen NULL, ClickHouse los tratará como String:

```sql
DESC format(TSV, '[NULL, NULL]')
```

```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Ejemplo con la opción `input_format_tsv_use_best_effort_in_schema_inference` deshabilitada:

```sql
SET input_format_tsv_use_best_effort_in_schema_inference = 0
DESC format(TSV, '[1,2,3]    42.42    Hello World!')
```

```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(String) │              │                    │         │                  │                │
│ c2   │ Nullable(String) │              │                    │         │                  │                │
│ c3   │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Ejemplos de detección automática de encabezado (cuando `input_format_tsv_detect_header` está habilitado):

Solo nombres:

```sql
SELECT * FROM format(TSV,
$$number    string    array
42    Hello    [1, 2, 3]
43    World    [4, 5, 6]
$$);
```

```response
┌─number─┬─string─┬─array───┐
│     42 │ Hello  │ [1,2,3] │
│     43 │ World  │ [4,5,6] │
└────────┴────────┴─────────┘
```

Nombres y tipos:

```sql
DESC format(TSV,
$$number    string    array
UInt32    String    Array(UInt16)
42    Hello    [1, 2, 3]
43    World    [4, 5, 6]
$$)
```

```response
┌─name───┬─type──────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ number │ UInt32        │              │                    │         │                  │                │
│ string │ String        │              │                    │         │                  │                │
│ array  │ Array(UInt16) │              │                    │         │                  │                │
└────────┴───────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Ten en cuenta que el encabezado solo se puede detectar si hay al menos una columna con un tipo distinto de String. Si todas las columnas son de tipo String, el encabezado no se detecta:

```sql
SELECT * FROM format(TSV,
$$first_column    second_column
Hello    World
World    Hello
$$)
```

```response
┌─c1───────────┬─c2────────────┐
│ first_column │ second_column │
│ Hello        │ World         │
│ World        │ Hello         │
└──────────────┴───────────────┘
```

<div id="values">
  ### Valores
</div>

En el formato Values, ClickHouse extrae el valor de la columna de la fila y luego lo procesa mediante
el analizador recursivo, de forma similar a como se procesan los literales.

**Ejemplos:**

Integers, Floats, Bools, Strings:

```sql
DESC format(Values, $$(42, 42.42, true, 'Hello,World!')$$)
```

```response
┌─name─┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Int64)   │              │                    │         │                  │                │
│ c2   │ Nullable(Float64) │              │                    │         │                  │                │
│ c3   │ Nullable(Bool)    │              │                    │         │                  │                │
│ c4   │ Nullable(String)  │              │                    │         │                  │                │
└──────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Dates, DateTimes:

```sql
 DESC format(Values, $$('2020-01-01', '2020-01-01 00:00:00', '2022-01-01 00:00:00.000')$$)
```

```response
┌─name─┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Date)          │              │                    │         │                  │                │
│ c2   │ Nullable(DateTime)      │              │                    │         │                  │                │
│ c3   │ Nullable(DateTime64(9)) │              │                    │         │                  │                │
└──────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Arrays:

```sql
DESC format(Values, '([1,2,3], [[1, 2], [], [3, 4]])')
```

```response
┌─name─┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Nullable(Int64))        │              │                    │         │                  │                │
│ c2   │ Array(Array(Nullable(Int64))) │              │                    │         │                  │                │
└──────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Si un array contiene null, ClickHouse usará los tipos de los demás elementos del array:

```sql
DESC format(Values, '([NULL, 42, NULL])')
```

```response
┌─name─┬─type───────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Nullable(Int64)) │              │                    │         │                  │                │
└──────┴────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Tuplas:

```sql
DESC format(Values, $$((42, 'Hello, world!'))$$)
```

```response
┌─name─┬─type─────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Tuple(Nullable(Int64), Nullable(String)) │              │                    │         │                  │                │
└──────┴──────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Maps:

```sql
DESC format(Values, $$({'key1' : 42, 'key2' : 24})$$)
```

```response
┌─name─┬─type─────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Map(String, Nullable(Int64)) │              │                    │         │                  │                │
└──────┴──────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Arrays, Tuples y Maps anidados:

```sql
DESC format(Values, $$([{'key1' : [(42, 'Hello'), (24, NULL)], 'key2' : [(NULL, ','), (42, 'world!')]}])$$)
```

```response
┌─name─┬─type────────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Map(String, Array(Tuple(Nullable(Int64), Nullable(String))))) │              │                    │         │                  │                │
└──────┴─────────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Si ClickHouse no puede determinar el tipo porque los datos solo contienen valores NULL, se lanzará una excepción:

```sql
DESC format(Values, '([NULL, NULL])')
```

```response
Code: 652. DB::Exception: Received from localhost:9000. DB::Exception:
Cannot determine type for column 'c1' by first 1 rows of data,
most likely this column contains only Nulls or empty Arrays/Maps.
...
```

Ejemplo con la opción `input_format_tsv_use_best_effort_in_schema_inference` desactivada:

```sql
SET input_format_tsv_use_best_effort_in_schema_inference = 0
DESC format(TSV, '[1,2,3]    42.42    Hello World!')
```

```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(String) │              │                    │         │                  │                │
│ c2   │ Nullable(String) │              │                    │         │                  │                │
│ c3   │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="custom-separated">
  ### CustomSeparated
</div>

En el formato CustomSeparated, ClickHouse primero extrae todos los valores de columna de la fila según los delimitadores especificados y luego intenta inferir
el tipo de dato de cada valor según la regla de escape.

Si la configuración `input_format_custom_detect_header` está habilitada, ClickHouse intentará detectar el encabezado con los nombres de las columnas (y quizá los tipos) mientras infiere el esquema. Esta configuración está habilitada de forma predeterminada.

**Ejemplo**

```sql
SET format_custom_row_before_delimiter = '<row_before_delimiter>',
       format_custom_row_after_delimiter = '<row_after_delimiter>\n',
       format_custom_row_between_delimiter = '<row_between_delimiter>\n',
       format_custom_result_before_delimiter = '<result_before_delimiter>\n',
       format_custom_result_after_delimiter = '<result_after_delimiter>\n',
       format_custom_field_delimiter = '<field_delimiter>',
       format_custom_escaping_rule = 'Quoted'

DESC format(CustomSeparated, $$<result_before_delimiter>
<row_before_delimiter>42.42<field_delimiter>'Some string 1'<field_delimiter>[1, NULL, 3]<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>NULL<field_delimiter>'Some string 3'<field_delimiter>[1, 2, NULL]<row_after_delimiter>
<result_after_delimiter>
$$)
```

```response
┌─name─┬─type───────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Float64)      │              │                    │         │                  │                │
│ c2   │ Nullable(String)       │              │                    │         │                  │                │
│ c3   │ Array(Nullable(Int64)) │              │                    │         │                  │                │
└──────┴────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Ejemplo de detección automática del encabezado (cuando `input_format_custom_detect_header` está habilitado):

```sql
SET format_custom_row_before_delimiter = '<row_before_delimiter>',
       format_custom_row_after_delimiter = '<row_after_delimiter>\n',
       format_custom_row_between_delimiter = '<row_between_delimiter>\n',
       format_custom_result_before_delimiter = '<result_before_delimiter>\n',
       format_custom_result_after_delimiter = '<result_after_delimiter>\n',
       format_custom_field_delimiter = '<field_delimiter>',
       format_custom_escaping_rule = 'Quoted'

DESC format(CustomSeparated, $$<result_before_delimiter>
<row_before_delimiter>'number'<field_delimiter>'string'<field_delimiter>'array'<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>42.42<field_delimiter>'Some string 1'<field_delimiter>[1, NULL, 3]<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>NULL<field_delimiter>'Some string 3'<field_delimiter>[1, 2, NULL]<row_after_delimiter>
<result_after_delimiter>
$$)
```

```response
┌─number─┬─string────────┬─array──────┐
│  42.42 │ Some string 1 │ [1,NULL,3] │
│   ᴺᵁᴸᴸ │ Some string 3 │ [1,2,NULL] │
└────────┴───────────────┴────────────┘
```

<div id="template">
  ### Template
</div>

En el formato Template, ClickHouse primero extrae de la fila todos los valores de las columnas según la plantilla especificada y luego intenta inferir el tipo de dato de cada valor de acuerdo con su regla de escape.

**Ejemplo**

Supongamos que tenemos un archivo `resultset` con el siguiente contenido:

```bash
<result_before_delimiter>
${data}<result_after_delimiter>
```

Y un archivo `row_format` con el siguiente contenido:

```text
<row_before_delimiter>${column_1:CSV}<field_delimiter_1>${column_2:Quoted}<field_delimiter_2>${column_3:JSON}<row_after_delimiter>
```

A continuación, podemos realizar las siguientes consultas:

```sql
SET format_template_rows_between_delimiter = '<row_between_delimiter>\n',
       format_template_row = 'row_format',
       format_template_resultset = 'resultset_format'

DESC format(Template, $$<result_before_delimiter>
<row_before_delimiter>42.42<field_delimiter_1>'Some string 1'<field_delimiter_2>[1, null, 2]<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>\N<field_delimiter_1>'Some string 3'<field_delimiter_2>[1, 2, null]<row_after_delimiter>
<result_after_delimiter>
$$)
```

```response
┌─name─────┬─type───────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ column_1 │ Nullable(Float64)      │              │                    │         │                  │                │
│ column_2 │ Nullable(String)       │              │                    │         │                  │                │
│ column_3 │ Array(Nullable(Int64)) │              │                    │         │                  │                │
└──────────┴────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="regexp">
  ### Regexp
</div>

Al igual que Template, en el formato Regexp ClickHouse primero extrae todos los valores de las columnas de la fila según la expresión regular especificada y luego intenta inferir
el tipo de dato de cada valor según la regla de escape especificada.

**Ejemplo**

```sql
SET format_regexp = '^Line: value_1=(.+?), value_2=(.+?), value_3=(.+?)',
       format_regexp_escaping_rule = 'CSV'

DESC format(Regexp, $$Line: value_1=42, value_2="Some string 1", value_3="[1, NULL, 3]"
Line: value_1=2, value_2="Some string 2", value_3="[4, 5, NULL]"$$)
```

```response
┌─name─┬─type───────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Int64)        │              │                    │         │                  │                │
│ c2   │ Nullable(String)       │              │                    │         │                  │                │
│ c3   │ Array(Nullable(Int64)) │              │                    │         │                  │                │
└──────┴────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="settings-for-text-formats">
  ### Configuración de los formatos de texto
</div>

<div id="input-format-max-rows-to-read-for-schema-inference">
  #### input_format_max_rows_to_read_for_schema_inference/input_format_max_bytes_to_read_for_schema_inference
</div>

Estos ajustes controlan la cantidad de datos que se leerá durante la inferencia del esquema.
Cuantas más filas/bytes se lean, más tiempo se dedicará a la inferencia del esquema, pero mayor será la probabilidad de
determinar correctamente los tipos (especialmente cuando los datos contienen muchos NULL).

Valores predeterminados:

* `25000` para `input_format_max_rows_to_read_for_schema_inference`.
* `33554432` (32 Mb) para `input_format_max_bytes_to_read_for_schema_inference`.

<div id="column-names-for-schema-inference">
  #### column_names_for_schema_inference
</div>

La lista de nombres de columna que se utilizarán en la inferencia del esquema para formatos sin nombres de columna explícitos. Los nombres especificados se usarán en lugar de los predeterminados `c1,c2,c3,...`. El formato es: `column1,column2,column3,...`.

**Ejemplo**

```sql
DESC format(TSV, 'Hello, World!    42    [1, 2, 3]') settings column_names_for_schema_inference = 'str,int,arr'
```

```response
┌─name─┬─type───────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ str  │ Nullable(String)       │              │                    │         │                  │                │
│ int  │ Nullable(Int64)        │              │                    │         │                  │                │
│ arr  │ Array(Nullable(Int64)) │              │                    │         │                  │                │
└──────┴────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="schema-inference-hints">
  #### schema_inference_hints
</div>

La lista de nombres y tipos de columna que se usarán en la inferencia del esquema en lugar de los tipos determinados automáticamente. Formato: &#39;column&#95;name1 column&#95;type1, column&#95;name2 column&#95;type2, ...&#39;.
Esta configuración puede usarse para especificar los tipos de las columnas que no se hayan podido determinar automáticamente o para optimizar el esquema.

**Ejemplo**

```sql
DESC format(JSONEachRow, '{"id" : 1, "age" : 25, "name" : "Josh", "status" : null, "hobbies" : ["football", "cooking"]}') SETTINGS schema_inference_hints = 'age LowCardinality(UInt8), status Nullable(String)', allow_suspicious_low_cardinality_types=1
```

```response
┌─name────┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ id      │ Nullable(Int64)         │              │                    │         │                  │                │
│ age     │ LowCardinality(UInt8)   │              │                    │         │                  │                │
│ name    │ Nullable(String)        │              │                    │         │                  │                │
│ status  │ Nullable(String)        │              │                    │         │                  │                │
│ hobbies │ Array(Nullable(String)) │              │                    │         │                  │                │
└─────────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="schema-inference-make-columns-nullable">
  #### schema_inference_make_columns_nullable $
</div>

Controla si los tipos inferidos se marcan como `Nullable` durante la inferencia de esquema para los formatos que no incluyen información sobre nulabilidad. Posibles valores:

* 0 - el tipo inferido nunca será `Nullable`,
* 1 - todos los tipos inferidos serán `Nullable`,
* 2 o &#39;auto&#39; - en los formatos de texto, el tipo inferido será `Nullable` solo si la columna contiene `NULL` en una muestra analizada durante la inferencia del esquema; en los formatos con tipado fuerte (Parquet, ORC, Arrow), la información de nulabilidad se toma de los metadatos del archivo,
* 3 - en los formatos de texto, usa `Nullable`; en los formatos con tipado fuerte, usa los metadatos del archivo.

Valor predeterminado: 3.

**Ejemplos**

```sql
SET schema_inference_make_columns_nullable = 1;
DESC format(JSONEachRow, $$
                                {"id" :  1, "age" :  25, "name" : "Josh", "status" : null, "hobbies" : ["football", "cooking"]}
                                {"id" :  2, "age" :  19, "name" :  "Alan", "status" : "married", "hobbies" :  ["tennis", "art"]}
                         $$)
```

```response
┌─name────┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ id      │ Nullable(Int64)         │              │                    │         │                  │                │
│ age     │ Nullable(Int64)         │              │                    │         │                  │                │
│ name    │ Nullable(String)        │              │                    │         │                  │                │
│ status  │ Nullable(String)        │              │                    │         │                  │                │
│ hobbies │ Array(Nullable(String)) │              │                    │         │                  │                │
└─────────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
SET schema_inference_make_columns_nullable = 'auto';
DESC format(JSONEachRow, $$
                                {"id" :  1, "age" :  25, "name" : "Josh", "status" : null, "hobbies" : ["football", "cooking"]}
                                {"id" :  2, "age" :  19, "name" :  "Alan", "status" : "married", "hobbies" :  ["tennis", "art"]}
                         $$)
```

```response
┌─name────┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ id      │ Int64            │              │                    │         │                  │                │
│ age     │ Int64            │              │                    │         │                  │                │
│ name    │ String           │              │                    │         │                  │                │
│ status  │ Nullable(String) │              │                    │         │                  │                │
│ hobbies │ Array(String)    │              │                    │         │                  │                │
└─────────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
SET schema_inference_make_columns_nullable = 0;
DESC format(JSONEachRow, $$
                                {"id" :  1, "age" :  25, "name" : "Josh", "status" : null, "hobbies" : ["football", "cooking"]}
                                {"id" :  2, "age" :  19, "name" :  "Alan", "status" : "married", "hobbies" :  ["tennis", "art"]}
                         $$)
```

```response

┌─name────┬─type──────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ id      │ Int64         │              │                    │         │                  │                │
│ age     │ Int64         │              │                    │         │                  │                │
│ name    │ String        │              │                    │         │                  │                │
│ status  │ String        │              │                    │         │                  │                │
│ hobbies │ Array(String) │              │                    │         │                  │                │
└─────────┴───────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="input-format-try-infer-integers">
  #### input_format_try_infer_integers
</div>

:::note
Esta configuración no se aplica al tipo de dato `JSON`.
:::

Si está habilitada, ClickHouse intentará inferir enteros en lugar de números de coma flotante durante la inferencia del esquema en formatos de texto.
Si todos los números de la columna en los datos de muestra son enteros, el tipo resultante será `Int64`; si al menos un número es de coma flotante, el tipo resultante será `Float64`.
Si los datos de muestra contienen solo enteros y al menos uno es positivo y supera el límite de `Int64`, ClickHouse inferirá `UInt64`.

Habilitada de forma predeterminada.

**Ejemplos**

```sql
SET input_format_try_infer_integers = 0
DESC format(JSONEachRow, $$
                                {"number" : 1}
                                {"number" : 2}
                         $$)
```

```response
┌─name───┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ number │ Nullable(Float64) │              │                    │         │                  │                │
└────────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
SET input_format_try_infer_integers = 1
DESC format(JSONEachRow, $$
                                {"number" : 1}
                                {"number" : 2}
                         $$)
```

```response
┌─name───┬─type────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ number │ Nullable(Int64) │              │                    │         │                  │                │
└────────┴─────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
DESC format(JSONEachRow, $$
                                {"number" : 1}
                                {"number" : 18446744073709551615}
                         $$)
```

```response
┌─name───┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ number │ Nullable(UInt64) │              │                    │         │                  │                │
└────────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
DESC format(JSONEachRow, $$
                                {"number" : 1}
                                {"number" : 2.2}
                         $$)
```

```response
┌─name───┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ number │ Nullable(Float64) │              │                    │         │                  │                │
└────────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="input-format-try-infer-datetimes">
  #### input_format_try_infer_datetimes
</div>

Si está habilitado, ClickHouse intentará inferir el tipo `DateTime` o `DateTime64` a partir de string fields en la inferencia del esquema para formatos de texto.
Si todos los campos de una columna de los datos de muestra se analizaron correctamente como datetimes, el tipo resultante será `DateTime` o `DateTime64(9)` (si algún datetime tenía parte fraccionaria);
si al menos un campo no se analiza como datetime, el tipo resultante será `String`.

Habilitado de forma predeterminada.

**Ejemplos**

```sql
SET input_format_try_infer_datetimes = 0;
DESC format(JSONEachRow, $$
                                {"datetime" : "2021-01-01 00:00:00", "datetime64" : "2021-01-01 00:00:00.000"}
                                {"datetime" : "2022-01-01 00:00:00", "datetime64" : "2022-01-01 00:00:00.000"}
                         $$)
```

```response
┌─name───────┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ datetime   │ Nullable(String) │              │                    │         │                  │                │
│ datetime64 │ Nullable(String) │              │                    │         │                  │                │
└────────────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
SET input_format_try_infer_datetimes = 1;
DESC format(JSONEachRow, $$
                                {"datetime" : "2021-01-01 00:00:00", "datetime64" : "2021-01-01 00:00:00.000"}
                                {"datetime" : "2022-01-01 00:00:00", "datetime64" : "2022-01-01 00:00:00.000"}
                         $$)
```

```response
┌─name───────┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ datetime   │ Nullable(DateTime)      │              │                    │         │                  │                │
│ datetime64 │ Nullable(DateTime64(9)) │              │                    │         │                  │                │
└────────────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
DESC format(JSONEachRow, $$
                                {"datetime" : "2021-01-01 00:00:00", "datetime64" : "2021-01-01 00:00:00.000"}
                                {"datetime" : "unknown", "datetime64" : "unknown"}
                         $$)
```

```response
┌─name───────┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ datetime   │ Nullable(String) │              │                    │         │                  │                │
│ datetime64 │ Nullable(String) │              │                    │         │                  │                │
└────────────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="input-format-try-infer-datetimes-only-datetime64">
  #### input_format_try_infer_datetimes_only_datetime64
</div>

Si está habilitado, ClickHouse siempre inferirá `DateTime64(9)` cuando `input_format_try_infer_datetimes` esté habilitado, incluso si los valores de fecha y hora no contienen una parte fraccionaria.

Deshabilitado de forma predeterminada.

**Ejemplos**

```sql
SET input_format_try_infer_datetimes = 1;
SET input_format_try_infer_datetimes_only_datetime64 = 1;
DESC format(JSONEachRow, $$
                                {"datetime" : "2021-01-01 00:00:00", "datetime64" : "2021-01-01 00:00:00.000"}
                                {"datetime" : "2022-01-01 00:00:00", "datetime64" : "2022-01-01 00:00:00.000"}
                         $$)
```

```response
┌─name───────┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ datetime   │ Nullable(DateTime64(9)) │              │                    │         │                  │                │
│ datetime64 │ Nullable(DateTime64(9)) │              │                    │         │                  │                │
└────────────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Nota: Al analizar valores DateTime durante la inferencia del esquema, se respeta la configuración [date&#95;time&#95;input&#95;format](/es/operations/settings/settings-formats.md#date_time_input_format)

<div id="input-format-try-infer-dates">
  #### input_format_try_infer_dates
</div>

Si está habilitado, ClickHouse intentará inferir el tipo `Date` a partir de campos de texto durante la inferencia del esquema para formatos de texto.
Si todos los campos de una columna en los datos de muestra se analizaron correctamente como fechas, el tipo resultante será `Date`;
si al menos un campo no se analizó como fecha, el tipo resultante será `String`.

Habilitado de forma predeterminada.

**Ejemplos**

```sql
SET input_format_try_infer_datetimes = 0, input_format_try_infer_dates = 0
DESC format(JSONEachRow, $$
                                {"date" : "2021-01-01"}
                                {"date" : "2022-01-01"}
                         $$)
```

```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ date │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
SET input_format_try_infer_dates = 1
DESC format(JSONEachRow, $$
                                {"date" : "2021-01-01"}
                                {"date" : "2022-01-01"}
                         $$)
```

```response
┌─name─┬─type───────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ date │ Nullable(Date) │              │                    │         │                  │                │
└──────┴────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
DESC format(JSONEachRow, $$
                                {"date" : "2021-01-01"}
                                {"date" : "unknown"}
                         $$)
```

```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ date │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="input-format-try-infer-exponent-floats">
  #### input_format_try_infer_exponent_floats
</div>

Si está habilitada esta opción, ClickHouse intentará inferir valores de coma flotante en notación exponencial para los formatos de texto (excepto JSON, donde los números en notación exponencial siempre se infieren).

Deshabilitado de forma predeterminada.

**Ejemplo**

```sql
SET input_format_try_infer_exponent_floats = 1;
DESC format(CSV,
$$1.1E10
2.3e-12
42E00
$$)
```

```response
┌─name─┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Float64) │              │                    │         │                  │                │
└──────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="self-describing-formats">
  ## Formatos autodescriptivos
</div>

Los formatos autodescriptivos contienen información sobre la estructura de los datos dentro de los propios datos:
puede tratarse de un encabezado con una descripción, un árbol binario de tipos o algún tipo de tabla.
Para inferir automáticamente un esquema a partir de archivos en estos formatos, ClickHouse lee la parte de los datos que contiene
información sobre los tipos y la convierte en el esquema de la tabla de ClickHouse.

<div id="formats-with-names-and-types">
  ### Formatos con el sufijo -WithNamesAndTypes
</div>

ClickHouse admite algunos formatos de texto con el sufijo -WithNamesAndTypes. Este sufijo significa que los datos incluyen dos filas adicionales con nombres y tipos de columna antes de los datos reales.
Durante la inferencia del esquema para estos formatos, ClickHouse lee las dos primeras filas y extrae los nombres y tipos de columna.

**Ejemplo**

```sql
DESC format(TSVWithNamesAndTypes,
$$num    str    arr
UInt8    String    Array(UInt8)
42    Hello, World!    [1,2,3]
$$)
```

```response
┌─name─┬─type─────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ num  │ UInt8        │              │                    │         │                  │                │
│ str  │ String       │              │                    │         │                  │                │
│ arr  │ Array(UInt8) │              │                    │         │                  │                │
└──────┴──────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="json-with-metadata">
  ### Formatos JSON con metadatos
</div>

Algunos formatos de entrada JSON ([JSON](/es/interfaces/formats/JSON), [JSONCompact](/es/interfaces/formats/JSONCompact), [JSONColumnsWithMetadata](/es/interfaces/formats/JSONColumnsWithMetadata)) incluyen metadatos con los nombres y tipos de las columnas.
Para la inferencia del esquema en estos formatos, ClickHouse lee estos metadatos.

**Ejemplo**

```sql
DESC format(JSON, $$
{
    "meta":
    [
        {
            "name": "num",
            "type": "UInt8"
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
            "str": "Hello, World",
            "arr": [1,2,3]
        }
    ],

    "rows": 1,

    "statistics":
    {
        "elapsed": 0.005723915,
        "rows_read": 1,
        "bytes_read": 1
    }
}
$$)
```

```response
┌─name─┬─type─────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ num  │ UInt8        │              │                    │         │                  │                │
│ str  │ String       │              │                    │         │                  │                │
│ arr  │ Array(UInt8) │              │                    │         │                  │                │
└──────┴──────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="avro">
  ### Avro
</div>

En el formato Avro, ClickHouse lee el esquema de los datos y lo convierte al esquema de ClickHouse mediante las siguientes correspondencias de tipos:

| Tipo de dato de Avro               | Tipo de dato de ClickHouse                                                     |
| ---------------------------------- | ------------------------------------------------------------------------------ |
| `boolean`                          | [Bool](../sql-reference/data-types/boolean.md)                                 |
| `int`                              | [Int32](../sql-reference/data-types/int-uint.md)                               |
| `int (date)` *                     | [Date32](../sql-reference/data-types/date32.md)                                |
| `long`                             | [Int64](../sql-reference/data-types/int-uint.md)                               |
| `float`                            | [Float32](../sql-reference/data-types/float.md)                                |
| `double`                           | [Float64](../sql-reference/data-types/float.md)                                |
| `bytes`, `string`                  | [String](../sql-reference/data-types/string.md)                                |
| `fixed`                            | [FixedString(N)](../sql-reference/data-types/fixedstring.md)                   |
| `enum`                             | [Enum](../sql-reference/data-types/enum.md)                                    |
| `array(T)`                         | [Array(T)](../sql-reference/data-types/array.md)                               |
| `union(null, T)`, `union(T, null)` | [Nullable(T)](../sql-reference/data-types/date.md)                             |
| `null`                             | [Nullable(Nothing)](../sql-reference/data-types/special-data-types/nothing.md) |
| `string (uuid)` *                  | [UUID](../sql-reference/data-types/uuid.md)                                    |
| `binary (decimal)` *               | [Decimal(P, S)](../sql-reference/data-types/decimal.md)                        |

* [Tipos lógicos de Avro](https://avro.apache.org/docs/current/spec.html#Logical+Types)

No se admiten otros tipos de Avro.

<div id="parquet">
  ### Parquet
</div>

En formato Parquet, ClickHouse lee el esquema a partir de los datos y lo convierte al esquema de ClickHouse mediante las siguientes correspondencias de tipos:

| Tipo de dato de Parquet      | Tipo de dato de ClickHouse                              |
| ---------------------------- | ------------------------------------------------------- |
| `BOOL`                       | [Bool](../sql-reference/data-types/boolean.md)          |
| `UINT8`                      | [UInt8](../sql-reference/data-types/int-uint.md)        |
| `INT8`                       | [Int8](../sql-reference/data-types/int-uint.md)         |
| `UINT16`                     | [UInt16](../sql-reference/data-types/int-uint.md)       |
| `INT16`                      | [Int16](../sql-reference/data-types/int-uint.md)        |
| `UINT32`                     | [UInt32](../sql-reference/data-types/int-uint.md)       |
| `INT32`                      | [Int32](../sql-reference/data-types/int-uint.md)        |
| `UINT64`                     | [UInt64](../sql-reference/data-types/int-uint.md)       |
| `INT64`                      | [Int64](../sql-reference/data-types/int-uint.md)        |
| `FLOAT`                      | [Float32](../sql-reference/data-types/float.md)         |
| `DOUBLE`                     | [Float64](../sql-reference/data-types/float.md)         |
| `DATE`                       | [Date32](../sql-reference/data-types/date32.md)         |
| `TIME (ms)`                  | [DateTime](../sql-reference/data-types/datetime.md)     |
| `TIMESTAMP`, `TIME (us, ns)` | [DateTime64](../sql-reference/data-types/datetime64.md) |
| `STRING`, `BINARY`           | [String](../sql-reference/data-types/string.md)         |
| `DECIMAL`                    | [Decimal](../sql-reference/data-types/decimal.md)       |
| `LIST`                       | [Array](../sql-reference/data-types/array.md)           |
| `STRUCT`                     | [Tuple](../sql-reference/data-types/tuple.md)           |
| `MAP`                        | [Map](../sql-reference/data-types/map.md)               |

Otros tipos de Parquet no son compatibles.

<div id="arrow">
  ### Arrow
</div>

En formato Arrow, ClickHouse lee el esquema a partir de los datos y lo convierte al esquema de ClickHouse mediante las siguientes correspondencias de tipos:

| Tipo de dato de Arrow           | Tipo de dato de ClickHouse                              |
| ------------------------------- | ------------------------------------------------------- |
| `BOOL`                          | [Bool](../sql-reference/data-types/boolean.md)          |
| `UINT8`                         | [UInt8](../sql-reference/data-types/int-uint.md)        |
| `INT8`                          | [Int8](../sql-reference/data-types/int-uint.md)         |
| `UINT16`                        | [UInt16](../sql-reference/data-types/int-uint.md)       |
| `INT16`                         | [Int16](../sql-reference/data-types/int-uint.md)        |
| `UINT32`                        | [UInt32](../sql-reference/data-types/int-uint.md)       |
| `INT32`                         | [Int32](../sql-reference/data-types/int-uint.md)        |
| `UINT64`                        | [UInt64](../sql-reference/data-types/int-uint.md)       |
| `INT64`                         | [Int64](../sql-reference/data-types/int-uint.md)        |
| `FLOAT`, `HALF_FLOAT`           | [Float32](../sql-reference/data-types/float.md)         |
| `DOUBLE`                        | [Float64](../sql-reference/data-types/float.md)         |
| `DATE32`                        | [Date32](../sql-reference/data-types/date32.md)         |
| `DATE64`                        | [DateTime](../sql-reference/data-types/datetime.md)     |
| `TIMESTAMP`, `TIME32`, `TIME64` | [DateTime64](../sql-reference/data-types/datetime64.md) |
| `STRING`, `BINARY`              | [String](../sql-reference/data-types/string.md)         |
| `DECIMAL128`, `DECIMAL256`      | [Decimal](../sql-reference/data-types/decimal.md)       |
| `LIST`                          | [Array](../sql-reference/data-types/array.md)           |
| `STRUCT`                        | [Tuple](../sql-reference/data-types/tuple.md)           |
| `MAP`                           | [Map](../sql-reference/data-types/map.md)               |

Otros tipos de Arrow no son compatibles.

<div id="orc">
  ### ORC
</div>

En el formato ORC, ClickHouse lee el esquema a partir de los datos y lo convierte al esquema de ClickHouse según las siguientes correspondencias de tipos:

| tipo de dato ORC                     | tipo de dato de ClickHouse                              |
| ------------------------------------ | ------------------------------------------------------- |
| `Boolean`                            | [Bool](../sql-reference/data-types/boolean.md)          |
| `Tinyint`                            | [Int8](../sql-reference/data-types/int-uint.md)         |
| `Smallint`                           | [Int16](../sql-reference/data-types/int-uint.md)        |
| `Int`                                | [Int32](../sql-reference/data-types/int-uint.md)        |
| `Bigint`                             | [Int64](../sql-reference/data-types/int-uint.md)        |
| `Float`                              | [Float32](../sql-reference/data-types/float.md)         |
| `Double`                             | [Float64](../sql-reference/data-types/float.md)         |
| `Date`                               | [Date32](../sql-reference/data-types/date32.md)         |
| `Timestamp`                          | [DateTime64](../sql-reference/data-types/datetime64.md) |
| `String`, `Char`, `Varchar`,`BINARY` | [String](../sql-reference/data-types/string.md)         |
| `Decimal`                            | [Decimal](../sql-reference/data-types/decimal.md)       |
| `List`                               | [Array](../sql-reference/data-types/array.md)           |
| `Struct`                             | [Tuple](../sql-reference/data-types/tuple.md)           |
| `Map`                                | [Map](../sql-reference/data-types/map.md)               |

Otros tipos de ORC no son compatibles.

<div id="native">
  ### Native
</div>

El formato Native se utiliza dentro de ClickHouse y contiene el esquema en los datos.
En la inferencia de esquemas, ClickHouse lee el esquema a partir de los datos sin realizar ninguna transformación.

<div id="formats-with-external-schema">
  ## Formatos con esquema externo
</div>

Estos formatos requieren un esquema que describa los datos en un archivo independiente y en un lenguaje de esquemas específico.
Para inferir automáticamente un esquema a partir de archivos en estos formatos, ClickHouse lee el esquema externo de un archivo independiente y lo transforma en un esquema de tabla de ClickHouse.

<div id="protobuf">
  ### Protobuf
</div>

En la inferencia de esquemas para el formato Protobuf, ClickHouse utiliza las siguientes correspondencias de tipos:

| Tipo de datos de Protobuf     | Tipo de datos de ClickHouse                       |
| ----------------------------- | ------------------------------------------------- |
| `bool`                        | [UInt8](../sql-reference/data-types/int-uint.md)  |
| `float`                       | [Float32](../sql-reference/data-types/float.md)   |
| `double`                      | [Float64](../sql-reference/data-types/float.md)   |
| `int32`, `sint32`, `sfixed32` | [Int32](../sql-reference/data-types/int-uint.md)  |
| `int64`, `sint64`, `sfixed64` | [Int64](../sql-reference/data-types/int-uint.md)  |
| `uint32`, `fixed32`           | [UInt32](../sql-reference/data-types/int-uint.md) |
| `uint64`, `fixed64`           | [UInt64](../sql-reference/data-types/int-uint.md) |
| `string`, `bytes`             | [String](../sql-reference/data-types/string.md)   |
| `enum`                        | [Enum](../sql-reference/data-types/enum.md)       |
| `repeated T`                  | [Array(T)](../sql-reference/data-types/array.md)  |
| `message`, `group`            | [Tuple](../sql-reference/data-types/tuple.md)     |

<div id="capnproto">
  ### CapnProto
</div>

En la inferencia de esquema para el formato CapnProto, ClickHouse utiliza las siguientes correspondencias de tipos:

| Tipo de dato de CapnProto          | Tipo de dato de ClickHouse                             |
| ---------------------------------- | ------------------------------------------------------ |
| `Bool`                             | [UInt8](../sql-reference/data-types/int-uint.md)       |
| `Int8`                             | [Int8](../sql-reference/data-types/int-uint.md)        |
| `UInt8`                            | [UInt8](../sql-reference/data-types/int-uint.md)       |
| `Int16`                            | [Int16](../sql-reference/data-types/int-uint.md)       |
| `UInt16`                           | [UInt16](../sql-reference/data-types/int-uint.md)      |
| `Int32`                            | [Int32](../sql-reference/data-types/int-uint.md)       |
| `UInt32`                           | [UInt32](../sql-reference/data-types/int-uint.md)      |
| `Int64`                            | [Int64](../sql-reference/data-types/int-uint.md)       |
| `UInt64`                           | [UInt64](../sql-reference/data-types/int-uint.md)      |
| `Float32`                          | [Float32](../sql-reference/data-types/float.md)        |
| `Float64`                          | [Float64](../sql-reference/data-types/float.md)        |
| `Text`, `Data`                     | [String](../sql-reference/data-types/string.md)        |
| `enum`                             | [Enum](../sql-reference/data-types/enum.md)            |
| `List`                             | [Array](../sql-reference/data-types/array.md)          |
| `struct`                           | [Tuple](../sql-reference/data-types/tuple.md)          |
| `union(T, Void)`, `union(Void, T)` | [Nullable(T)](../sql-reference/data-types/nullable.md) |

<div id="strong-typed-binary-formats">
  ## Formatos binarios fuertemente tipados
</div>

En estos formatos, cada valor serializado contiene información sobre su tipo (y posiblemente sobre su nombre), pero no hay información sobre la tabla completa.
Para inferir el esquema en estos formatos, ClickHouse lee los datos fila por fila (hasta `input_format_max_rows_to_read_for_schema_inference` filas o `input_format_max_bytes_to_read_for_schema_inference` bytes) y extrae
el tipo (y posiblemente el nombre) de cada valor a partir de los datos; luego convierte esos tipos en tipos de ClickHouse.

<div id="msgpack">
  ### MsgPack
</div>

En el formato MsgPack no hay delimitadores entre filas; para usar la inferencia de esquema con este formato, debe especificar el número de columnas de la tabla
mediante la configuración `input_format_msgpack_number_of_columns`. ClickHouse utiliza las siguientes correspondencias de tipos:

| Tipo de dato de MessagePack (`INSERT`)                             | Tipo de dato de ClickHouse                            |
| ------------------------------------------------------------------ | ----------------------------------------------------- |
| `int N`, `uint N`, `negative fixint`, `positive fixint`            | [Int64](../sql-reference/data-types/int-uint.md)      |
| `bool`                                                             | [UInt8](../sql-reference/data-types/int-uint.md)      |
| `fixstr`, `str 8`, `str 16`, `str 32`, `bin 8`, `bin 16`, `bin 32` | [String](../sql-reference/data-types/string.md)       |
| `float 32`                                                         | [Float32](../sql-reference/data-types/float.md)       |
| `float 64`                                                         | [Float64](../sql-reference/data-types/float.md)       |
| `uint 16`                                                          | [Date](../sql-reference/data-types/date.md)           |
| `uint 32`                                                          | [DateTime](../sql-reference/data-types/datetime.md)   |
| `uint 64`                                                          | [DateTime64](../sql-reference/data-types/datetime.md) |
| `fixarray`, `array 16`, `array 32`                                 | [Array](../sql-reference/data-types/array.md)         |
| `fixmap`, `map 16`, `map 32`                                       | [Map](../sql-reference/data-types/map.md)             |

De forma predeterminada, todos los tipos inferidos se consideran `Nullable`, pero esto puede cambiarse mediante la configuración `schema_inference_make_columns_nullable`.

<div id="bsoneachrow">
  ### BSONEachRow
</div>

En BSONEachRow, cada fila de datos se presenta como un documento BSON. En la inferencia de esquema, ClickHouse lee los documentos BSON uno por uno y extrae
valores, nombres y tipos de los datos, y luego transforma estos tipos en tipos de ClickHouse usando las siguientes correspondencias de tipos:

| BSON Type                                                                                     | tipo de ClickHouse                                                                                                                 |
| --------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `\x08` boolean                                                                                | [Bool](../sql-reference/data-types/boolean.md)                                                                                     |
| `\x10` int32                                                                                  | [Int32](../sql-reference/data-types/int-uint.md)                                                                                   |
| `\x12` int64                                                                                  | [Int64](../sql-reference/data-types/int-uint.md)                                                                                   |
| `\x01` double                                                                                 | [Float64](../sql-reference/data-types/float.md)                                                                                    |
| `\x09` datetime                                                                               | [DateTime64](../sql-reference/data-types/datetime64.md)                                                                            |
| `\x05` binary with`\x00` binary subtype, `\x02` string, `\x0E` symbol, `\x0D` JavaScript code | [String](../sql-reference/data-types/string.md)                                                                                    |
| `\x07` ObjectId,                                                                              | [FixedString(12)](../sql-reference/data-types/fixedstring.md)                                                                      |
| `\x05` binary with `\x04` uuid subtype, size = 16                                             | [UUID](../sql-reference/data-types/uuid.md)                                                                                        |
| `\x04` array                                                                                  | [Array](../sql-reference/data-types/array.md)/[Tuple](../sql-reference/data-types/tuple.md) (si los tipos anidados son diferentes) |
| `\x03` document                                                                               | [Named Tuple](../sql-reference/data-types/tuple.md)/[Map](../sql-reference/data-types/map.md) (con claves String)                  |

De forma predeterminada, todos los tipos inferidos están dentro de `Nullable`, pero esto se puede cambiar usando la configuración `schema_inference_make_columns_nullable`.

<div id="formats-with-constant-schema">
  ## Formatos con esquema constante
</div>

Los datos en estos formatos siempre tienen el mismo esquema.

<div id="line-as-string">
  ### LineAsString
</div>

En este formato, ClickHouse lee la línea completa de los datos en una única columna con el tipo de dato `String`. El tipo inferido para este formato siempre es `String` y el nombre de la columna es `line`.

**Ejemplo**

```sql
DESC format(LineAsString, 'Hello\nworld!')
```

```response
┌─name─┬─type───┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ line │ String │              │                    │         │                  │                │
└──────┴────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="json-as-string">
  ### JSONAsString
</div>

En este formato, ClickHouse lee el objeto JSON completo de los datos en una única columna con el tipo de dato `String`. El tipo inferido para este formato siempre es `String` y el nombre de la columna es `json`.

**Ejemplo**

```sql
DESC format(JSONAsString, '{"x" : 42, "y" : "Hello, World!"}')
```

```response
┌─name─┬─type───┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ json │ String │              │                    │         │                  │                │
└──────┴────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="json-as-object">
  ### JSONAsObject
</div>

En este formato, ClickHouse lee el objeto JSON completo de los datos en una sola columna con el tipo de datos `JSON`. El tipo inferido para este formato es siempre `JSON` y el nombre de la columna es `json`.

**Ejemplo**

```sql
DESC format(JSONAsObject, '{"x" : 42, "y" : "Hello, World!"}');
```

```response
┌─name─┬─type─┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ json │ JSON │              │                    │         │                  │                │
└──────┴──────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="schema-inference-modes">
  ## Modos de inferencia de esquema
</div>

La inferencia de esquema a partir del conjunto de archivos de datos puede funcionar en 2 modos diferentes: `default` y `union`.
El modo se controla mediante la opción de configuración `schema_inference_mode`.

<div id="default-schema-inference-mode">
  ### Modo predeterminado
</div>

En el modo predeterminado, ClickHouse asume que todos los archivos tienen el mismo esquema e intenta inferirlo leyendo los archivos uno por uno hasta conseguirlo.

Ejemplo:

Supongamos que tenemos 3 archivos `data1.jsonl`, `data2.jsonl` y `data3.jsonl` con el siguiente contenido:

`data1.jsonl`:

```json
{"field1" :  1, "field2" :  null}
{"field1" :  2, "field2" :  null}
{"field1" :  3, "field2" :  null}
```

`data2.jsonl`:

```json
{"field1" :  4, "field2" :  "Data4"}
{"field1" :  5, "field2" :  "Data5"}
{"field1" :  6, "field2" :  "Data5"}
```

`data3.jsonl`:

```json
{"field1" :  7, "field2" :  "Data7", "field3" :  [1, 2, 3]}
{"field1" :  8, "field2" :  "Data8", "field3" :  [4, 5, 6]}
{"field1" :  9, "field2" :  "Data9", "field3" :  [7, 8, 9]}
```

Intentemos usar la inferencia de esquema con estos 3 archivos:

```sql title="Query"
:) DESCRIBE file('data{1,2,3}.jsonl') SETTINGS schema_inference_mode='default'
```

```response title="Response"
┌─name───┬─type─────────────┐
│ field1 │ Nullable(Int64)  │
│ field2 │ Nullable(String) │
└────────┴──────────────────┘
```

Como podemos ver, no tenemos `field3` del archivo `data3.jsonl`.
Esto sucede porque ClickHouse primero intentó inferir el esquema a partir del archivo `data1.jsonl`, falló porque el campo `field2` solo contenía NULL,
y luego intentó inferir el esquema a partir de `data2.jsonl` y lo consiguió, por lo que no se leyeron los datos del archivo `data3.jsonl`.

<div id="default-schema-inference-mode-1">
  ### Modo de unión
</div>

En el modo de unión, ClickHouse asume que los archivos pueden tener distintos esquemas, por lo que infiere los esquemas de todos los archivos y luego los combina en un esquema común.

Supongamos que tenemos 3 archivos `data1.jsonl`, `data2.jsonl` y `data3.jsonl` con el siguiente contenido:

`data1.jsonl`:

```json
{"field1" :  1}
{"field1" :  2}
{"field1" :  3}
```

`data2.jsonl`:

```json
{"field2" :  "Data4"}
{"field2" :  "Data5"}
{"field2" :  "Data5"}
```

`data3.jsonl`:

```json
{"field3" :  [1, 2, 3]}
{"field3" :  [4, 5, 6]}
{"field3" :  [7, 8, 9]}
```

Probemos la inferencia de esquema con estos 3 archivos:

```sql title="Query"
:) DESCRIBE file('data{1,2,3}.jsonl') SETTINGS schema_inference_mode='union'
```

```response title="Response"
┌─name───┬─type───────────────────┐
│ field1 │ Nullable(Int64)        │
│ field2 │ Nullable(String)       │
│ field3 │ Array(Nullable(Int64)) │
└────────┴────────────────────────┘
```

Como podemos ver, tenemos todos los campos de todos los archivos.

Nota:

* Dado que algunos archivos pueden no contener algunas de las columnas del esquema resultante, el modo de unión solo es compatible con formatos que permiten leer un subconjunto de columnas (como JSONEachRow, Parquet, TSVWithNames, etc.) y no funcionará con otros formatos (como CSV, TSV, JSONCompactEachRow, etc.).
* Si ClickHouse no puede inferir el esquema de uno de los archivos, se producirá una excepción.
* Si tiene muchos archivos, leer el esquema de todos ellos puede llevar mucho tiempo.

<div id="automatic-format-detection">
  ## Detección automática del formato
</div>

Si no se especifica el formato de los datos y no se puede determinar por la extensión del archivo, ClickHouse intentará detectar el formato del archivo por su contenido.

**Ejemplos:**

Supongamos que tenemos `data` con el siguiente contenido:

```csv
"a","b"
1,"Data1"
2,"Data2"
3,"Data3"
```

Podemos inspeccionar y consultar este archivo sin especificar ni el formato ni la estructura:

```sql
:) desc file(data);
```

```response
┌─name─┬─type─────────────┐
│ a    │ Nullable(Int64)  │
│ b    │ Nullable(String) │
└──────┴──────────────────┘
```

```sql
:) select * from file(data);
```

```response
┌─a─┬─b─────┐
│ 1 │ Data1 │
│ 2 │ Data2 │
│ 3 │ Data3 │
└───┴───────┘
```

:::note
ClickHouse solo puede detectar algunos formatos, y esa detección lleva algo de tiempo; siempre es mejor especificar el formato de forma explícita.
:::