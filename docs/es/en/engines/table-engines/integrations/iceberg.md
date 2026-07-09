---
description: 'Este motor proporciona una integración de solo lectura con tablas Apache Iceberg
  existentes en Amazon S3, Azure, HDFS y para tablas almacenadas localmente.'
sidebar_label: 'Iceberg'
sidebar_position: 90
slug: /engines/table-engines/integrations/iceberg
title: 'Motor de tabla Iceberg'
doc_type: 'reference'
---

:::warning
Recomendamos usar la [función de tabla Iceberg](/es/sql-reference/table-functions/iceberg.md) para trabajar con datos de Iceberg en ClickHouse. Actualmente, la función de tabla Iceberg ofrece funcionalidad suficiente y una interfaz parcial de solo lectura para las tablas Iceberg.

El motor de tabla Iceberg está disponible, pero puede tener limitaciones. ClickHouse no se diseñó originalmente para admitir tablas con esquemas que cambian externamente, lo que puede afectar la funcionalidad del motor de tabla Iceberg. Como resultado, algunas funciones que sí funcionan con tablas normales pueden no estar disponibles o no funcionar correctamente, especialmente al usar el analizador antiguo.

Para lograr una compatibilidad óptima, sugerimos usar la función de tabla Iceberg mientras seguimos mejorando el soporte del motor de tabla Iceberg.
:::

Este motor proporciona una integración de solo lectura con tablas [Iceberg](https://iceberg.apache.org/) de Apache existentes en Amazon S3, Azure, HDFS y para tablas almacenadas localmente.

<div id="create-table">
  ## Crear tabla
</div>

Tenga en cuenta que la tabla Iceberg ya debe existir en el almacenamiento; este comando no admite parámetros DDL para crear una tabla nueva.

```sql
CREATE TABLE iceberg_table_s3
    ENGINE = IcebergS3(url,  [, NOSIGN | access_key_id, secret_access_key, [session_token]], format, [,compression], [,extra_credentials])

CREATE TABLE iceberg_table_azure
    ENGINE = IcebergAzure(connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression])

CREATE TABLE iceberg_table_hdfs
    ENGINE = IcebergHDFS(path_to_table, [,format] [,compression_method])

CREATE TABLE iceberg_table_local
    ENGINE = IcebergLocal(path_to_table, [,format] [,compression_method])
```

<div id="engine-arguments">
  ## Argumentos del motor
</div>

La descripción de los argumentos coincide, respectivamente, con la descripción de los argumentos de los motores `S3`, `AzureBlobStorage`, `HDFS` y `File`.
`format` corresponde al formato de los archivos de datos de la tabla Iceberg.

En `IcebergS3`, se puede usar el parámetro opcional `extra_credentials` para pasar un `role_arn` y habilitar el acceso basado en roles en ClickHouse Cloud. Consulte [Secure S3](/es/cloud/data-sources/secure-s3) para ver los pasos de configuración.

Los parámetros del motor pueden especificarse mediante [colección nombrada](../../../operations/named-collections.md)

<div id="example">
  ### Ejemplo
</div>

```sql
CREATE TABLE iceberg_table ENGINE=IcebergS3('http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

Uso de colecciones nombradas:

```xml
<clickhouse>
    <named_collections>
        <iceberg_conf>
            <url>http://test.s3.amazonaws.com/clickhouse-bucket/</url>
            <access_key_id>test</access_key_id>
            <secret_access_key>test</secret_access_key>
        </iceberg_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE iceberg_table ENGINE=IcebergS3(iceberg_conf, filename = 'test_table')

```

<div id="aliases">
  ## Alias
</div>

El motor de tabla `Iceberg` detecta automáticamente el backend de almacenamiento a partir del ajuste `disk` y, en función de este, utiliza `IcebergS3`, `IcebergAzure` o `IcebergLocal`. Si no se especifica ningún `disk`, usa de forma predeterminada la implementación `IcebergS3`.

<div id="data-types">
  ## Tipos de datos
</div>

La siguiente tabla muestra cómo se asignan los tipos de datos de Iceberg a los tipos de datos de ClickHouse durante la inferencia del esquema (para fines de lectura).

<div id="primitive-types">
  ### Tipos primitivos
</div>

| Tipo de Iceberg    | Tipo de ClickHouse     | Notas                                                        |
| ------------------ | ---------------------- | ------------------------------------------------------------ |
| `boolean`          | `Bool`                 |                                                              |
| `int`              | `Int32`                |                                                              |
| `long`, `bigint`   | `Int64`                |                                                              |
| `float`            | `Float32`              |                                                              |
| `double`           | `Float64`              |                                                              |
| `date`             | `Date32`               |                                                              |
| `time`             | `Int64`                | Microsegundos desde la medianoche                            |
| `timestamp`        | `DateTime64(6)`        | Microsegundos, sin zona horaria                              |
| `timestamptz`      | `DateTime64(6, 'UTC')` | Microsegundos, zona horaria UTC                              |
| `timestamp_ns`     | `DateTime64(9)`        | Nanosegundos, sin zona horaria (solo a partir de Iceberg v3) |
| `timestamptz_ns`   | `DateTime64(9, 'UTC')` | Nanosegundos, zona horaria UTC (solo a partir de Iceberg v3) |
| `string`, `binary` | `String`               |                                                              |
| `uuid`             | `UUID`                 |                                                              |
| `fixed(N)`         | `FixedString(N)`       |                                                              |
| `decimal(P, S)`    | `Decimal(P, S)`        |                                                              |

<div id="complex-types">
  ### Tipos complejos
</div>

| Tipo de Iceberg | Tipo de ClickHouse |
| --------------- | ------------------ |
| `list`          | `Array`            |
| `map`           | `Map`              |
| `struct`        | `Tuple`            |

<div id="schema-evolution">
  ## Evolución del esquema
</div>

ClickHouse permite leer tablas Iceberg cuyo esquema ha evolucionado con el tiempo. Esto incluye tablas en las que se han añadido, eliminado o reordenado columnas, así como columnas que han pasado de ser obligatorias a ser Nullable. Además, se admiten las siguientes conversiones de tipos:

* int -&gt; long
* float -&gt; double
* decimal(P, S) -&gt; decimal(P&#39;, S) where P&#39; &gt; P.

Actualmente, no es posible cambiar estructuras anidadas ni los tipos de los elementos dentro de arrays y maps.

Para leer una tabla cuyo esquema haya cambiado después de su creación con inferencia dinámica de esquemas, configure allow&#95;dynamic&#95;metadata&#95;for&#95;data&#95;lakes = true al crear la tabla.

<div id="partition-pruning">
  ## Poda de particiones
</div>

ClickHouse admite la poda de particiones durante las consultas SELECT en tablas Iceberg, lo que ayuda a optimizar el rendimiento de las consultas al omitir archivos de datos irrelevantes. Para habilitar la poda de particiones, establezca `use_iceberg_partition_pruning = 1`. Para obtener más información sobre la poda de particiones en Iceberg, consulte https://iceberg.apache.org/spec/#partitioning

<div id="time-travel">
  ## Viaje en el tiempo
</div>

ClickHouse admite el viaje en el tiempo en las tablas Iceberg, lo que permite consultar datos históricos con una marca de tiempo específica o un ID de instantánea.

<div id="deleted-rows">
  ## Procesamiento de tablas con filas eliminadas
</div>

ClickHouse admite la lectura de tablas Iceberg que utilizan los siguientes métodos de eliminación:

* [Eliminación por posición](https://iceberg.apache.org/spec/#position-delete-files)
* [Eliminación por igualdad](https://iceberg.apache.org/spec/#equality-delete-files) (compatible a partir de la versión 25.8+)

El siguiente método de eliminación **no es compatible**:

* [Vectores de eliminación](https://iceberg.apache.org/spec/#deletion-vectors) (introducido en la v3)

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

Nota: No puede especificar los parámetros `iceberg_timestamp_ms` y `iceberg_snapshot_id` a la vez en la misma consulta.

<div id="important-considerations">
  ### Consideraciones importantes
</div>

* Las **instantáneas** suelen crearse cuando:
  * Se escriben datos nuevos en la tabla
  * Se realiza algún tipo de compactación de datos

* **Los cambios de esquema normalmente no crean instantáneas** - Esto da lugar a comportamientos importantes al usar viaje en el tiempo con tablas que han pasado por una evolución del esquema.

<div id="example-scenarios">
  ### Escenarios de ejemplo
</div>

Todos los escenarios están escritos en Spark porque CH todavía no admite escribir en tablas Iceberg.

<div id="scenario-1">
  #### Escenario 1: Cambios de esquema sin nuevas instantáneas
</div>

Considere esta secuencia de operaciones:

```sql
 -- Create a table with two columns
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example (
  order_number int, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2')

-- Insert data into the table
  INSERT INTO spark_catalog.db.time_travel_example VALUES 
    (1, 'Mars')

  ts1 = now() // A piece of pseudo code

-- Alter table to add a new column
  ALTER TABLE spark_catalog.db.time_travel_example ADD COLUMN (price double)
 
  ts2 = now()

-- Insert data into the table
  INSERT INTO spark_catalog.db.time_travel_example VALUES (2, 'Venus', 100)

   ts3 = now()

-- Query the table at each timestamp
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

Resultados de la consulta en distintas marcas de tiempo:

* En ts1 &amp; ts2: Solo aparecen las dos columnas originales
* En ts3: Aparecen las tres columnas, con NULL en el precio de la primera fila

<div id="scenario-2">
  #### Escenario 2: Diferencias entre el esquema histórico y el actual
</div>

Una consulta de viaje en el tiempo realizada en el momento actual puede mostrar un esquema distinto del de la tabla actual:

```sql
-- Create a table
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_2 (
  order_number int, 
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

Esto ocurre porque `ALTER TABLE` no crea una nueva instantánea, sino que, para la tabla actual, Spark toma el valor de `schema_id` del archivo de metadatos más reciente, no de una instantánea.

<div id="scenario-3">
  #### Escenario 3: Diferencias entre el esquema histórico y el actual
</div>

La segunda es que, al usar viaje en el tiempo, no se puede obtener el estado de la tabla anterior a que se escribiera ningún dato en ella:

```sql
-- Create a table
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_3 (
  order_number int, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2');

  ts = now();

-- Query the table at a specific timestamp
  SELECT * FROM spark_catalog.db.time_travel_example_3 TIMESTAMP AS OF ts; -- Finises with error: Cannot find a snapshot older than ts.
```

En ClickHouse, el comportamiento es el mismo que en Spark. Puedes sustituir mentalmente las consultas `Select` de Spark por las consultas `Select` de ClickHouse y funcionará igual.

<div id="metadata-file-resolution">
  ## Resolución del archivo de metadatos
</div>

Al usar el motor de tabla `Iceberg` en ClickHouse, el sistema necesita localizar el archivo metadata.json adecuado que describe la estructura de la tabla Iceberg. Así funciona este proceso de resolución:

<div id="candidate-search">
  ### Búsqueda de candidatos
</div>

1. **Especificación directa de la ruta**:

* Si configura `iceberg_metadata_file_path`, el sistema usará esta ruta exacta combinándola con la ruta del directorio de la tabla Iceberg.
* Cuando se proporciona esta configuración, se ignoran todas las demás configuraciones de resolución.

2. **Coincidencia del UUID de la tabla**:

* Si se especifica `iceberg_metadata_table_uuid`, el sistema:
  * Buscará solo archivos `.metadata.json` en el directorio `metadata`
  * Filtrará los archivos que contengan un campo `table-uuid` que coincida con el UUID especificado (sin distinción entre mayúsculas y minúsculas)

3. **Búsqueda predeterminada**:

* Si no se proporciona ninguna de las configuraciones anteriores, todos los archivos `.metadata.json` del directorio `metadata` pasan a ser candidatos

<div id="most-recent-file">
  ### Seleccionar el archivo más reciente
</div>

Después de identificar los archivos candidatos mediante las reglas anteriores, el sistema determina cuál es el más reciente:

* Si `iceberg_recent_metadata_file_by_last_updated_ms_field` está habilitada:
  * Se selecciona el archivo con el valor más alto de `last-updated-ms`

* En caso contrario:
  * Se selecciona el archivo con el número de versión más alto
  * (La versión aparece como `V` en los nombres de archivo con formato `V.metadata.json` o `V-uuid.metadata.json`)

**Nota**: Todos los ajustes mencionados (salvo que se especifique explícitamente lo contrario) son ajustes a nivel de motor y deben especificarse durante la creación de la tabla, como se muestra a continuación:

```sql
CREATE TABLE example_table ENGINE = Iceberg(
    's3://bucket/path/to/iceberg_table'
) SETTINGS iceberg_metadata_table_uuid = '6f6f6407-c6a5-465f-a808-ea8900e35a38';
```

**Nota**: Aunque los catálogos Iceberg suelen encargarse de resolver los metadatos, el motor de tabla `Iceberg` de ClickHouse interpreta directamente los archivos almacenados en S3 como tablas Iceberg, por lo que es importante comprender estas reglas de resolución.

<div id="data-cache">
  ## Caché de datos
</div>

El motor de tabla `Iceberg` y la función de tabla admiten la caché de datos, al igual que los almacenamientos `S3`, `AzureBlobStorage` y `HDFS`. Consulte [aquí](../../../engines/table-engines/integrations/s3.md#data-cache).

<div id="metadata-cache">
  ## Caché de metadatos
</div>

El motor de tabla `Iceberg` y la función de tabla admiten una caché de metadatos para almacenar la información de los archivos de manifiesto, la lista de manifiestos y el JSON de metadatos. La caché se almacena en memoria. Esta función se controla mediante la configuración `use_iceberg_metadata_files_cache`, que está habilitada de forma predeterminada.

<div id="async-metadata-prefetch">
  ## Precarga asíncrona de metadatos
</div>

La precarga asíncrona de metadatos puede habilitarse al crear una tabla `Iceberg` configurando `iceberg_metadata_async_prefetch_period_ms`. Si se establece en 0 (valor predeterminado) o si la caché de metadatos no está habilitada, la precarga asíncrona se desactiva.
Para habilitar esta función, se debe proporcionar un valor distinto de cero en milisegundos. Este valor representa el intervalo entre ciclos de precarga.

Si está habilitada, el servidor ejecutará una operación recurrente en segundo plano para listar el catalog remoto y detectar una nueva versión de los metadatos. A continuación, los analizará y recorrerá recursivamente la instantánea, recuperando los archivos activos de la lista de manifiestos y los archivos de manifiesto.
Los archivos que ya estén disponibles en la caché de metadatos no se volverán a descargar. Al final de cada ciclo de precarga, la instantánea de metadatos más reciente estará disponible en la caché de metadatos.

```sql
CREATE TABLE example_table ENGINE = Iceberg(
    's3://bucket/path/to/iceberg_table'
) SETTINGS
    iceberg_metadata_async_prefetch_period_ms = 60000;
```

Para aprovechar al máximo la precarga asíncrona de metadatos en las operaciones de lectura, el parámetro `iceberg_metadata_staleness_ms` debe especificarse como parámetro de consulta o de sesión. De forma predeterminada (0 - no especificado), en el contexto de cada consulta, el servidor obtendrá los metadatos más recientes del catálogo remoto.
Al especificar una tolerancia a la antigüedad de los metadatos, se permite al servidor usar la versión en caché de la instantánea de metadatos sin consultar el catálogo remoto. Si hay una versión de metadatos en la caché y se ha descargado dentro de la ventana de antigüedad indicada, se utilizará para procesar la consulta.
De lo contrario, la versión más reciente se obtendrá del catálogo remoto.

```sql
SELECT count() FROM icebench_table WHERE ...
SETTINGS iceberg_metadata_staleness_ms=120000
```

**Nota**: La precarga asíncrona de metadatos se ejecuta en `ICEBERG_SCEDULE_POOL`, que es el threadpool del lado del servidor para operaciones en segundo plano en tablas `Iceberg` activas. El tamaño de este threadpool se controla mediante el parámetro de configuración del servidor `iceberg_background_schedule_pool_size` (el valor predeterminado es 10).

**Nota**: Actualmente, se espera que el tamaño de la caché de metadatos sea suficiente para almacenar por completo la instantánea de metadatos más reciente de todas las tablas activas, si la precarga asíncrona está habilitada.

<div id="see-also">
  ## Ver también
</div>

* [función de tabla Iceberg](/es/sql-reference/table-functions/iceberg.md)