---
description: 'Proporciona una interfaz tabular de solo lectura para tablas de Apache Iceberg en
  Amazon S3, Azure, HDFS o almacenadas localmente.'
sidebar_label: 'iceberg'
sidebar_position: 90
slug: /sql-reference/table-functions/iceberg
title: 'iceberg'
doc_type: 'reference'
---

Proporciona una interfaz tabular de solo lectura para tablas de Apache [Iceberg](https://iceberg.apache.org/) en Amazon S3, Azure, HDFS o almacenadas localmente.

<div id="syntax">
  ## Sintaxis
</div>

```sql
icebergS3(url [, NOSIGN | access_key_id, secret_access_key, [session_token]] [,format] [,compression_method] [,extra_credentials])
icebergS3(named_collection[, option=value [,..]])

icebergAzure(connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])
icebergAzure(named_collection[, option=value [,..]])

icebergHDFS(path_to_table, [,format] [,compression_method])
icebergHDFS(named_collection[, option=value [,..]])

icebergLocal(path_to_table, [,format] [,compression_method])
icebergLocal(named_collection[, option=value [,..]])
```

<div id="arguments">
  ## Argumentos
</div>

La descripción de los argumentos coincide con la de las funciones de tabla `s3`, `azureBlobStorage`, `HDFS` y `file`, respectivamente.
`format` se refiere al formato de los archivos de datos de la tabla Iceberg.

Para `icebergS3`, se puede usar un parámetro opcional `extra_credentials` para pasar un `role_arn` para el acceso basado en roles en ClickHouse Cloud. Consulte [Secure S3](/es/cloud/data-sources/secure-s3) para ver los pasos de configuración.

<div id="returned-value">
  ### Valor devuelto
</div>

Una tabla con la estructura especificada para leer datos de la tabla Iceberg especificada.

<div id="example">
  ### Ejemplo
</div>

```sql
SELECT * FROM icebergS3('http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

:::important
Actualmente, ClickHouse admite leer las versiones v1 y v2 del formato Iceberg mediante las funciones de tabla `icebergS3`, `icebergAzure`, `icebergHDFS` y `icebergLocal`, y los motores de tabla `IcebergS3`, `icebergAzure`, `IcebergHDFS` y `IcebergLocal`.
:::

<div id="defining-a-named-collection">
  ## Definir una named collection
</div>

A continuación se muestra un ejemplo de cómo configurar una named collection para almacenar la URL y las credenciales:

```xml
<clickhouse>
    <named_collections>
        <iceberg_conf>
            <url>http://test.s3.amazonaws.com/clickhouse-bucket/</url>
            <access_key_id>test</access_key_id>
            <secret_access_key>test</secret_access_key>
            <format>auto</format>
            <structure>auto</structure>
        </iceberg_conf>
    </named_collections>
</clickhouse>
```

```sql
SELECT * FROM icebergS3(iceberg_conf, filename = 'test_table')
DESCRIBE icebergS3(iceberg_conf, filename = 'test_table')
```

<div id="iceberg-writes-catalogs">
  ## Uso de un catálogo de datos
</div>

Las tablas Iceberg también pueden utilizarse con varios catálogos de datos, como [REST Catalog](https://iceberg.apache.org/rest-catalog-spec/), [AWS Glue Data Catalog](https://docs.aws.amazon.com/prescriptive-guidance/latest/serverless-etl-aws-glue/aws-glue-data-catalog.html) y [Unity Catalog](https://www.unitycatalog.io/).

:::important
Al usar un catalog, la mayoría de los usuarios querrán utilizar el motor de base de datos `DataLakeCatalog`, que conecta ClickHouse a su catalog para descubrir sus tablas. Puede usar este motor de base de datos en lugar de crear manualmente tablas individuales con el motor de tabla `IcebergS3`.
:::

Para ello, cree una tabla con el motor `IcebergS3` y proporcione la configuración necesaria.

Por ejemplo, al usar REST Catalog con almacenamiento MinIO:

```sql
CREATE TABLE `database_name.table_name`
ENGINE = IcebergS3(
  'http://minio:9000/warehouse-rest/table_name/',
  'minio_access_key',
  'minio_secret_key'
)
```

O bien, usando AWS Glue Data Catalog con S3:

```sql
CREATE TABLE `my_database.my_table`  
ENGINE = IcebergS3(
  's3://my-data-bucket/warehouse/my_database/my_table/',
  'aws_access_key',
  'aws_secret_key'
)
```

<div id="schema-evolution">
  ## Evolución del esquema
</div>

Por el momento, con ayuda de CH, puede leer tablas Iceberg cuyo esquema ha evolucionado con el tiempo. Actualmente admitimos la lectura de tablas en las que se han añadido y eliminado columnas, y cuyo orden ha cambiado. También puede convertir una columna obligatoria en otra que admita NULL. Además, admitimos las conversiones de tipos permitidas para tipos simples, en concreto:  

* int -&gt; long
* float -&gt; double
* decimal(P, S) -&gt; decimal(P&#39;, S) donde P&#39; &gt; P.

Actualmente, no es posible cambiar estructuras anidadas ni los tipos de los elementos dentro de arrays y maps.

<div id="partition-pruning">
  ## Poda de particiones
</div>

ClickHouse admite la poda de particiones en las consultas SELECT de tablas Iceberg, lo que ayuda a optimizar el rendimiento de las consultas al omitir archivos de datos irrelevantes. Para habilitar la poda de particiones, establezca `use_iceberg_partition_pruning = 1`. Para obtener más información sobre la poda de particiones de Iceberg, consulte https://iceberg.apache.org/spec/#partitioning

<div id="time-travel">
  ## Viaje en el tiempo
</div>

ClickHouse admite el viaje en el tiempo para las tablas Iceberg, lo que permite consultar datos históricos con una marca de tiempo específica o un ID de snapshot.

<div id="deleted-rows">
  ## Procesamiento de tablas con filas eliminadas
</div>

Actualmente, solo se admiten las tablas Iceberg con [eliminaciones por posición](https://iceberg.apache.org/spec/#position-delete-files).

Los siguientes métodos de eliminación **no se admiten**:

* [Eliminaciones por igualdad](https://iceberg.apache.org/spec/#equality-delete-files)
* [Vectores de eliminación](https://iceberg.apache.org/spec/#deletion-vectors) (introducidos en la versión 3)

<div id="basic-usage">
  ### Uso básico
</div>

```sql
 SELECT * FROM example_table ORDER BY 1 
 SETTINGS iceberg_timestamp_ms = 1714636800000
```

```sql
 SELECT * FROM example_table ORDER BY 1 
 SETTINGS iceberg_snapshot_id = 3547395809148285433
```

Nota: No puede especificar ambos parámetros, `iceberg_timestamp_ms` y `iceberg_snapshot_id`, en la misma consulta.

<div id="important-considerations">
  ### Consideraciones importantes
</div>

* **Las instantáneas** suelen crearse cuando:

* Se escriben datos nuevos en la tabla

* Se realiza algún tipo de compactación de datos

* **Los cambios de esquema normalmente no crean instantáneas** - Esto da lugar a comportamientos importantes al usar el viaje en el tiempo con tablas que han experimentado una evolución del esquema.

<div id="example-scenarios">
  ### Ejemplos de escenarios
</div>

Todos los escenarios están escritos en Spark porque CH aún no admite escribir en tablas Iceberg.

<div id="scenario-1">
  #### Escenario 1: Cambios de esquema sin nuevas instantáneas
</div>

Considere la siguiente secuencia de operaciones:

```sql
 -- Create a table with two columns
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example (
  order_number bigint, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2')

- - Insert data into the table
  INSERT INTO spark_catalog.db.time_travel_example VALUES 
    (1, 'Mars')

  ts1 = now() // A piece of pseudo code

- - Alter table to add a new column
  ALTER TABLE spark_catalog.db.time_travel_example ADD COLUMN (price double)
 
  ts2 = now()

- - Insert data into the table
  INSERT INTO spark_catalog.db.time_travel_example VALUES (2, 'Venus', 100)

   ts3 = now()

- - Query the table at each timestamp
  SELECT * FROM spark_catalog.db.time_travel_example TIMESTAMP AS OF ts1;

+------------+------------+
|order_number|product_code|
+------------+------------+
|           1|        Mars|
+------------+------------+
  SELECT * FROM spark_catalog.db.time_travel_example TIMESTAMP AS OF ts2;

+------------+------------+
|order_number|product_code|
+------------+------------+
|           1|        Mars|
+------------+------------+

  SELECT * FROM spark_catalog.db.time_travel_example TIMESTAMP AS OF ts3;

+------------+------------+-----+
|order_number|product_code|price|
+------------+------------+-----+
|           1|        Mars| NULL|
|           2|       Venus|100.0|
+------------+------------+-----+
```

Resultados de la consulta en distintas marcas temporales:

* En ts1 &amp; ts2: solo aparecen las dos columnas originales
* En ts3: aparecen las tres columnas, con NULL en el precio de la primera fila

<div id="scenario-2">
  #### Escenario 2:  Diferencias entre el esquema histórico y el actual
</div>

Una consulta de viaje en el tiempo realizada en el momento actual puede mostrar un esquema distinto del de la tabla actual:

```sql
-- Create a table
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_2 (
  order_number bigint, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2')

-- Insert initial data into the table
  INSERT INTO spark_catalog.db.time_travel_example_2 VALUES (2, 'Venus');

-- Alter table to add a new column
  ALTER TABLE spark_catalog.db.time_travel_example_2 ADD COLUMN (price double);

  ts = now();

-- Query the table at a current moment but using timestamp syntax

  SELECT * FROM spark_catalog.db.time_travel_example_2 TIMESTAMP AS OF ts;

    +------------+------------+
    |order_number|product_code|
    +------------+------------+
    |           2|       Venus|
    +------------+------------+

-- Query the table at a current moment
  SELECT * FROM spark_catalog.db.time_travel_example_2;
    +------------+------------+-----+
    |order_number|product_code|price|
    +------------+------------+-----+
    |           2|       Venus| NULL|
    +------------+------------+-----+
```

Esto ocurre porque `ALTER TABLE` no crea una instantánea nueva; para la tabla actual, Spark toma el valor de `schema_id` del archivo de metadatos más reciente, no de una instantánea.

<div id="scenario-3">
  #### Escenario 3:  Diferencias entre el esquema histórico y el actual
</div>

La segunda es que, al hacer viaje en el tiempo, no puedes obtener el estado de la tabla anterior a que se escribiera en ella ningún dato:

```sql
-- Create a table
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_3 (
  order_number bigint, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2');

  ts = now();

-- Query the table at a specific timestamp
  SELECT * FROM spark_catalog.db.time_travel_example_3 TIMESTAMP AS OF ts; -- Finises with error: Cannot find a snapshot older than ts.
```

En ClickHouse, el comportamiento es el mismo que en Spark. Puedes imaginar que sustituyes las consultas Select de Spark por consultas Select de ClickHouse y funcionará igual.

<div id="metadata-file-resolution">
  ## Resolución del archivo de metadatos
</div>

Al usar la función de tabla `iceberg` en ClickHouse, el sistema necesita localizar el archivo metadata.json correcto que describe la estructura de la tabla Iceberg. A continuación se explica cómo funciona este proceso de resolución:

<div id="candidate-search">
  ### Búsqueda de candidatos (en orden de prioridad)
</div>

1. **Especificación directa de la ruta**:
   *Si establece `iceberg_metadata_file_path`, el sistema usará esa ruta exacta combinándola con la ruta del directorio de la tabla Iceberg.

* Cuando se proporciona este ajuste, se ignoran todos los demás ajustes de resolución.

2. **Coincidencia del UUID de la tabla**:
   *Si se especifica `iceberg_metadata_table_uuid`, el sistema hará lo siguiente:
   *Buscará únicamente archivos `.metadata.json` en el directorio `metadata`
   *Filtrará los archivos que contengan un campo `table-uuid` que coincida con el UUID especificado (sin distinguir entre mayúsculas y minúsculas)

3. **Búsqueda por defecto**:
   *Si no se proporciona ninguno de los ajustes anteriores, todos los archivos `.metadata.json` del directorio `metadata` pasan a ser candidatos

<div id="most-recent-file">
  ### Selección del archivo más reciente
</div>

Después de identificar los archivos candidatos según las reglas anteriores, el sistema determina cuál es el más reciente:

* Si `iceberg_recent_metadata_file_by_last_updated_ms_field` está habilitado:

* Se selecciona el archivo con el valor `last-updated-ms` más alto

* En caso contrario:

* Se selecciona el archivo con el número de versión más alto

* (La versión aparece como `V` en los nombres de archivo con formato `V.metadata.json` o `V-uuid.metadata.json`)

**Nota**: Todos los ajustes mencionados son ajustes de la función de tabla (no ajustes globales ni ajustes a nivel de consulta) y deben especificarse como se muestra a continuación:

```sql
SELECT * FROM iceberg('s3://bucket/path/to/iceberg_table', 
    SETTINGS iceberg_metadata_table_uuid = 'a90eed4c-f74b-4e5b-b630-096fb9d09021');
```

**Nota**: Aunque los catálogos Iceberg normalmente se encargan de la resolución de metadatos, la función de tabla `iceberg` de ClickHouse interpreta directamente los archivos almacenados en S3 como tablas Iceberg, por lo que es importante comprender estas reglas de resolución.

<div id="metadata-cache">
  ## Caché de metadatos
</div>

El motor de tabla `Iceberg` y la función de tabla admiten una caché de metadatos que almacena información de los archivos manifest, la lista de manifiestos y el JSON de metadatos. La caché se almacena en memoria. Esta funcionalidad se controla mediante la configuración `use_iceberg_metadata_files_cache`, que está habilitada de forma predeterminada.

<div id="aliases">
  ## Alias
</div>

La función de tabla `iceberg` es ahora un alias de `icebergS3`.

<div id="virtual-columns">
  ## Columnas virtuales
</div>

* `_path` — Ruta del archivo. Tipo: `LowCardinality(String)`.
* `_file` — Nombre del archivo. Tipo: `LowCardinality(String)`.
* `_size` — Tamaño del archivo en bytes. Tipo: `Nullable(UInt64)`. Si se desconoce el tamaño del archivo, el valor es `NULL`.
* `_time` — Fecha y hora de la última modificación del archivo. Tipo: `Nullable(DateTime)`. Si se desconoce la hora, el valor es `NULL`.
* `_etag` — El etag del archivo. Tipo: `LowCardinality(String)`. Si se desconoce el etag, el valor es `NULL`.

<div id="writes-into-iceberg-table">
  ## Escritura en una tabla Iceberg
</div>

A partir de la versión 25.7, ClickHouse admite modificaciones en las tablas Iceberg de los usuarios.

Actualmente, esta es una función experimental, por lo que primero debe habilitarla:

```sql
SET allow_insert_into_iceberg = 1;
```

<div id="create-iceberg-table">
  ### Crear una tabla
</div>

Para crear su propia tabla Iceberg vacía, use los mismos comandos que para la lectura, pero especifique el esquema de forma explícita.
Las operaciones de escritura admiten todos los formatos de datos de la especificación Iceberg, como Parquet, Avro y ORC.

<div id="example">
  ### Ejemplo
</div>

```sql
CREATE TABLE iceberg_writes_example
(
    x Nullable(String),
    y Nullable(Int32)
)
ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/')
```

Nota: Para crear un archivo version hint, habilite el ajuste `iceberg_use_version_hint`.
Si desea comprimir el archivo metadata.json, especifique el nombre del códec en el ajuste `iceberg_metadata_compression_method`.

<div id="writes-inserts">
  ### INSERT
</div>

Después de crear una nueva tabla, puedes insertar datos con la sintaxis habitual de ClickHouse.

<div id="example">
  ### Ejemplo
</div>

```sql
INSERT INTO iceberg_writes_example VALUES ('Pavel', 777), ('Ivanov', 993);

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Pavel
y: 777

Row 2:
──────
x: Ivanov
y: 993
```

<div id="iceberg-writes-delete">
  ### DELETE
</div>

ClickHouse también permite eliminar filas adicionales en el formato merge-on-read.
Esta consulta creará una nueva instantánea con archivos de eliminación por posición.

<div id="example">
  ### Ejemplo
</div>

```sql
ALTER TABLE iceberg_writes_example DELETE WHERE x != 'Ivanov';

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993
```

<div id="iceberg-writes-schema-evolution">
  ### Evolución del esquema
</div>

ClickHouse permite agregar, eliminar, modificar o renombrar columnas con tipos simples (que no sean Tuple, Array ni Map).

<div id="example">
  ### Ejemplo
</div>

```sql
ALTER TABLE iceberg_writes_example MODIFY COLUMN y Nullable(Int64);
SHOW CREATE TABLE iceberg_writes_example;

   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `y` Nullable(Int64)                                  ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

ALTER TABLE iceberg_writes_example ADD COLUMN z Nullable(Int32);
SHOW CREATE TABLE iceberg_writes_example;

   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `y` Nullable(Int64),                                 ↴│
   │↳    `z` Nullable(Int32)                                  ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993
z: ᴺᵁᴸᴸ

ALTER TABLE iceberg_writes_example DROP COLUMN z;
SHOW CREATE TABLE iceberg_writes_example;
   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `y` Nullable(Int64)                                  ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993

ALTER TABLE iceberg_writes_example RENAME COLUMN y TO value;
SHOW CREATE TABLE iceberg_writes_example;

   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `value` Nullable(Int64)                              ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
value: 993
```

<div id="iceberg-writes-compaction">
  ### Compactación
</div>

ClickHouse admite la compactación de tablas Iceberg. Actualmente, puede fusionar archivos de eliminación por posición en archivos de datos mientras actualiza los metadatos. Los ID y las marcas de tiempo de las instantáneas anteriores permanecen sin cambios, por lo que la función de viaje en el tiempo puede seguir usándose con los mismos valores.

Cómo usarlo:

```sql
SET allow_experimental_iceberg_compaction = 1

OPTIMIZE TABLE iceberg_writes_example;

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993
```

<div id="iceberg-expire-snapshots">
  ### Eliminar instantáneas antiguas
</div>

Las tablas Iceberg acumulan instantáneas con cada operación INSERT, DELETE o UPDATE. Con el tiempo, esto puede dar lugar a una gran cantidad de instantáneas y de archivos de datos asociados. El comando `expire_snapshots` elimina las instantáneas antiguas y limpia los archivos de datos a los que ya no hace referencia ninguna instantánea conservada.

**Sintaxis:**

```sql
ALTER TABLE iceberg_table EXECUTE expire_snapshots(
    ['timestamp']
    [, expire_before = 'timestamp']
    [, retention_period = '3d']
    [, retain_last = 100]
    [, snapshot_ids = [1, 2, 3, 4]]
    [, dry_run = 1]
);
```

De forma predeterminada, las instantáneas que se conservan las determina la [política de retención](#iceberg-snapshot-retention-policy) (las propiedades de la tabla `min-snapshots-to-keep`, `max-snapshot-age-ms` y las sobrescrituras por referencia). Cuando se especifica `snapshot_ids`, la política de retención se omite y solo las instantáneas enumeradas se tienen en cuenta para su expiración.

**Argumentos:**

* `'timestamp'` (posicional) o `expire_before = 'timestamp'` — una cadena de fecha y hora (por ejemplo, `'2024-06-01 00:00:00'`) interpretada en la **zona horaria del servidor**. Actúa como mecanismo de seguridad: las instantáneas cuyo `timestamp-ms` sea igual o posterior a este valor quedan protegidas frente a la expiración, incluso si la política de retención las expiraría en otras circunstancias. Puede combinarse con `snapshot_ids`; en ese caso, las instantáneas enumeradas con fecha igual o posterior al timestamp no expiran.
* `retention_period = '<duration>'` — sobrescribe `history.expire.max-snapshot-age-ms` a nivel de tabla solo para esta invocación. Las instantáneas más antiguas que esta duración (medida desde ahora) pasan a ser candidatas para expiración. El valor es una cadena de duración compuesta por uno o más pares `{number}{unit}` concatenados. Unidades compatibles: `y` (365 días), `w` (7 días), `d` (24 horas), `h` (60 minutos), `m` (60 segundos), `s` (1 segundo), `ms` (1 milisegundo). Las unidades pueden combinarse, por ejemplo, `'3d'`, `'12h'`, `'1d12h30m'`, `'500ms'`.
* `retain_last = N` — sobrescribe `history.expire.min-snapshots-to-keep` a nivel de tabla solo para esta invocación. Siempre se conservan al menos `N` instantáneas, independientemente de su antigüedad.
* `snapshot_ids = [id1, id2, ...]` — expira exactamente los ID de instantánea enumerados (excepto las instantáneas referenciadas por la instantánea actual, las ramas o las etiquetas). Este modo omite por completo la política de retención y no puede combinarse con `retention_period` ni con `retain_last`.
* `dry_run = 1` — calcula qué expiraría y devuelve métricas sin escribir metadatos nuevos ni eliminar archivos.

:::note
`retention_period` y `retain_last` sobrescriben solo los valores predeterminados de retención **a nivel de tabla**. Las sobrescrituras de retención por referencia (rama/etiqueta) configuradas en las propiedades de la tabla Iceberg (por ejemplo, `refs.<branch>.min-snapshots-to-keep`) nunca se sobrescriben; siempre se aplican tal como se especifican en los metadatos de la tabla.
:::

**Ejemplo:**

```sql
SET allow_insert_into_iceberg = 1;

-- Create some snapshots by inserting data
INSERT INTO iceberg_table VALUES (1);
INSERT INTO iceberg_table VALUES (2);
INSERT INTO iceberg_table VALUES (3);

-- Expire using retention policy only
ALTER TABLE iceberg_table EXECUTE expire_snapshots();

-- Expire with a safety fuse: protect snapshots newer than the timestamp (positional syntax)
ALTER TABLE iceberg_table EXECUTE expire_snapshots('2025-01-01 00:00:00');

-- Same using the named argument form
ALTER TABLE iceberg_table EXECUTE expire_snapshots(expire_before = '2025-01-01 00:00:00');

-- Override retention parameters for one execution
ALTER TABLE iceberg_table EXECUTE expire_snapshots(retention_period = '3d', retain_last = 10);

-- Expire explicit snapshots
ALTER TABLE iceberg_table EXECUTE expire_snapshots(snapshot_ids = [101, 102, 103]);

-- Dry-run preview (no metadata updates, no file deletes)
ALTER TABLE iceberg_table EXECUTE expire_snapshots(retention_period = '1d', dry_run = 1);
```

**Salida:**

El comando devuelve una tabla con dos columnas (`metric_name String`, `metric_value Int64`) que contiene una fila por cada métrica. Los nombres de las métricas siguen la [especificación de Iceberg](https://iceberg.apache.org/docs/latest/spark-procedures/#output):

| metric&#95;name                       | Descripción                                                              |
| ------------------------------------- | ------------------------------------------------------------------------ |
| `deleted_data_files_count`            | Número de archivos de datos eliminados                                   |
| `deleted_position_delete_files_count` | Número de archivos de eliminación por posición eliminados                |
| `deleted_equality_delete_files_count` | Número de archivos de eliminación por igualdad eliminados                |
| `deleted_manifest_files_count`        | Número de archivos de manifiesto eliminados                              |
| `deleted_manifest_lists_count`        | Número de archivos de lista de manifiestos eliminados                    |
| `deleted_statistics_files_count`      | Número de archivos de estadísticas eliminados (actualmente siempre es 0) |
| `dry_run`                             | `1` para el modo de simulación, `0` para la ejecución normal             |

El comando realiza los siguientes pasos:

1. Evalúa la política de retención (consulte más abajo) para determinar qué instantáneas deben conservarse
2. Si se proporcionó un argumento `timestamp`, también protege todas las instantáneas con ese `timestamp` o posterior
3. Expira las instantáneas que no se conservan por la política ni están protegidas por el límite de `timestamp`
4. Calcula qué archivos están asociados exclusivamente a instantáneas expiradas
5. En modo normal: genera nuevos metadatos sin las instantáneas expiradas
6. En modo normal: elimina físicamente las listas de manifiestos, los archivos de manifiesto y los archivos de datos inalcanzables
7. En el modo `dry_run = 1`: omite los pasos 5 y 6 y solo devuelve las métricas calculadas

<div id="iceberg-snapshot-retention-policy">
  #### Política de retención de instantáneas
</div>

El comando `expire_snapshots` respeta la [política de retención de instantáneas de Iceberg](https://iceberg.apache.org/spec/#snapshot-retention-policy). La retención se configura mediante propiedades de la tabla Iceberg y sobrescrituras por referencia:

| Propiedad                              | Ámbito | Predeterminado                                                                    | Descripción                                                                                                               |
| -------------------------------------- | ------ | --------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `history.expire.min-snapshots-to-keep` | Tabla  | `iceberg_expire_default_min_snapshots_to_keep` (predeterminado `1`)               | Número mínimo de instantáneas que se deben conservar en la cadena de ancestros de cada rama                               |
| `history.expire.max-snapshot-age-ms`   | Tabla  | `iceberg_expire_default_max_snapshot_age_ms` (predeterminado `432000000`, 5 días) | Antigüedad máxima (en ms) de las instantáneas que se deben conservar en una rama                                          |
| `history.expire.max-ref-age-ms`        | Tabla  | `iceberg_expire_default_max_ref_age_ms` (predeterminado `∞`)                      | Antigüedad máxima (en ms) de una referencia de instantánea (rama o etiqueta) antes de que se elimine la propia referencia |

Cada referencia de instantánea (`refs` en los metadatos de Iceberg) puede sobrescribir estos valores con campos por referencia: `min-snapshots-to-keep`, `max-snapshot-age-ms` y `max-ref-age-ms`.

**Evaluación de la retención:**

* **Para cada rama** (incluida `main`): se recorre la cadena de ancestros a partir del head de la rama. Las instantáneas se conservan mientras se cumpla cualquiera de estas condiciones:
  * La instantánea es una de las primeras `min-snapshots-to-keep` de la cadena
  * La antigüedad de la instantánea está dentro de `max-snapshot-age-ms` (es decir, `now - timestamp-ms <= max-snapshot-age-ms`)
* **Para las etiquetas**: la instantánea etiquetada se conserva, a menos que la etiqueta haya superado su `max-ref-age-ms`, en cuyo caso se elimina la referencia de la etiqueta
* **Las referencias distintas de `main`** cuya antigüedad supera `max-ref-age-ms` se eliminan por completo (la rama `main` nunca se elimina)
* **Las referencias huérfanas** que apuntan a instantáneas inexistentes se eliminan con una advertencia
* **La instantánea actual siempre se conserva**, independientemente de la configuración de retención

**Privilegios requeridos:**

Se requiere el privilegio `ALTER TABLE EXECUTE`, que es un privilegio hijo de `ALTER TABLE` en la jerarquía de control de acceso de ClickHouse. Puede otorgarlo específicamente o mediante el privilegio padre:

```sql
-- Grant only EXECUTE permission
GRANT ALTER TABLE EXECUTE ON my_iceberg_table TO my_user;

-- Or grant all ALTER TABLE permissions (includes ALTER TABLE EXECUTE)
GRANT ALTER TABLE ON my_iceberg_table TO my_user;
```

:::note

* Solo se admiten tablas con Iceberg format version 2 (las instantáneas v1 no garantizan `manifest-list`, que es necesario para identificar de forma segura los archivos que deben limpiarse)
* La instantánea actual siempre se conserva, aunque sea anterior al timestamp especificado
* Requiere que la configuración `allow_insert_into_iceberg` esté habilitada
* Requiere que la configuración `allow_experimental_expire_snapshots` esté habilitada
* La autorización propia del catálogo (autenticación del REST catalog, AWS Glue IAM, etc.) se aplica por separado cuando ClickHouse actualiza los metadatos
  :::

<div id="iceberg-remove-orphan-files">
  ### Eliminar archivos huérfanos
</div>

Los archivos huérfanos son archivos almacenados que no están referenciados por ninguna instantánea en los metadatos de la tabla Iceberg. Se acumulan debido a escrituras fallidas, limpiezas parciales tras la compactación y operaciones interrumpidas, lo que provoca un crecimiento descontrolado del almacenamiento. El comando `remove_orphan_files` identifica y elimina estos archivos huérfanos.

**Sintaxis:**

```sql
-- Positional form: single unnamed older_than argument
ALTER TABLE iceberg_table EXECUTE remove_orphan_files('timestamp')

-- Named form
ALTER TABLE iceberg_table EXECUTE remove_orphan_files(
    older_than = 'timestamp',
    location = 'path',
    dry_run = 0|1
)

-- No arguments: use all defaults (older_than = 3 days ago)
ALTER TABLE iceberg_table EXECUTE remove_orphan_files()
```

**Parámetros:**

| Parámetro    | Tipo                      | Predeterminado                                                                | Descripción                                                                                                                                                                                              |
| ------------ | ------------------------- | ----------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `older_than` | `String` (marca temporal) | hace 3 días (configurable mediante `iceberg_orphan_files_older_than_seconds`) | Solo considera candidatos a huérfanos los archivos cuya hora de última modificación sea anterior a esta marca temporal. Es una medida de seguridad para evitar eliminar archivos de escrituras en curso. |
| `location`   | `String`                  | Ubicación de la tabla                                                         | Restringe el análisis a un subdirectorio específico dentro de la ubicación de la tabla (por ejemplo, `'data/'` o `'metadata/'`).                                                                         |
| `dry_run`    | `UInt64`                  | `0`                                                                           | Cuando es `1`, identifica los archivos huérfanos y devuelve el resumen de resultados sin eliminar nada realmente.                                                                                        |

**Ejemplos:**

```sql
-- Remove orphan files older than a specific timestamp
ALTER TABLE iceberg_table EXECUTE remove_orphan_files('2026-03-01 00:00:00');

-- Dry run: preview which files would be deleted
ALTER TABLE iceberg_table EXECUTE remove_orphan_files(dry_run = 1);

-- Scan only the data directory
ALTER TABLE iceberg_table EXECUTE remove_orphan_files(
    older_than = '2026-03-01 00:00:00',
    location = 'data/'
);

-- Combine positional older_than with named arguments
ALTER TABLE iceberg_table EXECUTE remove_orphan_files(
    '2026-03-01 00:00:00',
    dry_run = 1
);
```

**Salida:**

El comando devuelve una tabla con las columnas `metric_name` y `metric_value` que muestran el recuento de archivos eliminados (o que se eliminarían en modo dry&#95;run) por categoría. Las categorías de archivos se clasifican mediante heurísticas aproximadas basadas en convenciones de nomenclatura de archivos; los archivos que no coinciden con ningún patrón específico se asignan de forma predeterminada a `deleted_data_files_count`:

| metric&#95;name                                     | metric&#95;value |
| --------------------------------------------------- | ---------------- |
| deleted&#95;data&#95;files&#95;count                | 5                |
| deleted&#95;position&#95;delete&#95;files&#95;count | 2                |
| deleted&#95;equality&#95;delete&#95;files&#95;count | 0                |
| deleted&#95;manifest&#95;files&#95;count            | 3                |
| deleted&#95;manifest&#95;lists&#95;count            | 1                |
| deleted&#95;metadata&#95;files&#95;count            | 0                |
| deleted&#95;statistics&#95;files&#95;count          | 0                |
| skipped&#95;missing&#95;metadata&#95;count          | 0                |
| failed&#95;deletions&#95;count                      | 0                |

**Configuración:**

| Setting                                   | Type     | Default           | Description                                                                     |
| ----------------------------------------- | -------- | ----------------- | ------------------------------------------------------------------------------- |
| `allow_iceberg_remove_orphan_files`       | `Bool`   | `false`           | Ajuste de control para habilitar la funcionalidad (experimental).               |
| `iceberg_orphan_files_older_than_seconds` | `UInt64` | `259200` (3 días) | Umbral predeterminado de `older_than` en segundos cuando se omite el argumento. |

:::note

* **Requiere Iceberg format version 2 (o superior).** Las tablas de la versión 1 se rechazan porque carecen de punteros `manifest-list` en las instantáneas, que son necesarios para determinar de forma segura el conjunto de archivos accesibles. Ejecutar el comando en una tabla v1 devuelve un error `BAD_ARGUMENTS`.
* Requiere que estén habilitados tanto `allow_insert_into_iceberg` como `allow_iceberg_remove_orphan_files`
* Se recomienda ejecutar `expire_snapshots` antes de `remove_orphan_files` para que los archivos referenciados exclusivamente por instantáneas expiradas se limpien primero
* Use `dry_run = 1` para previsualizar los archivos huérfanos antes de eliminarlos
* El umbral `older_than` evita eliminar archivos de escrituras en curso: el umbral predeterminado de 3 días proporciona un margen de seguridad amplio
  :::

<div id="see-also">
  ## Véase también
</div>

* [motor de Iceberg](/es/engines/table-engines/integrations/iceberg.md)
* [función de tabla de clúster de Iceberg](/es/sql-reference/table-functions/icebergCluster.md)