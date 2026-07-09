---
description: 'Proporciona una interfaz de tipo tabla para seleccionar/insertar archivos en Azure Blob
  Storage. Similar a la función s3.'
keywords: ['azure blob storage']
sidebar_label: 'azureBlobStorage'
sidebar_position: 10
slug: /sql-reference/table-functions/azureBlobStorage
title: 'azureBlobStorage'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="azureblobstorage-table-function">
  # Función de tabla azureBlobStorage
</div>

Proporciona una interfaz similar a una tabla para seleccionar/insertar archivos en [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs). Esta función de tabla es similar a la [función s3](../../sql-reference/table-functions/s3.md).

<div id="syntax">
  ## Sintaxis
</div>

<Tabs>
  <TabItem value="connection_string" label="Cadena de conexión" default>
    Las credenciales están integradas en la cadena de conexión, por lo que no se necesitan `account_name`/`account_key` por separado:

    ```sql
    azureBlobStorage(connection_string, container_name, blobpath [, format, compression, structure])
    ```
  </TabItem>

  <TabItem value="storage_account_url" label="URL de la cuenta de almacenamiento">
    Requiere `account_name` y `account_key` como argumentos independientes:

    ```sql
    azureBlobStorage(storage_account_url, container_name, blobpath, account_name, account_key [, format, compression, structure])
    ```
  </TabItem>

  <TabItem value="named_collection" label="Colección con nombre">
    Consulte [Colecciones con nombre](#named-collections) más abajo para ver la lista completa de claves compatibles:

    ```sql
    azureBlobStorage(named_collection[, option=value [,..]])
    ```
  </TabItem>
</Tabs>

<div id="arguments">
  ## Argumentos
</div>

| Argument                         | Descripción                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| -------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `connection_string`              | Una cadena de conexión que incluye credenciales integradas (nombre de la cuenta + clave de la cuenta o SAS token). Al usar esta forma, `account_name` y `account_key` **no** deben pasarse por separado. Consulta [Configurar una cadena de conexión](https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json\&bc=%2Fazure%2Fstorage%2Fblobs%2Fbreadcrumb%2Ftoc.json#configure-a-connection-string-for-an-azure-storage-account).                            |
| `storage_account_url`            | La endpoint URL de la cuenta de almacenamiento, por ejemplo `https://myaccount.blob.core.windows.net/`. Al usar esta forma, **debes** pasar también `account_name` y `account_key`.                                                                                                                                                                                                                                                                                                                                                   |
| `container_name`                 | Nombre del contenedor.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `blobpath`                       | Ruta del archivo. Admite los siguientes comodines en modo de solo lectura: `*`, `**`, `?`, `{abc,def}` y `{N..M}`, donde `N` y `M` son números, y `'abc'` y `'def'` son cadenas.                                                                                                                                                                                                                                                                                                                                                          |
| `account_name`                   | Nombre de la cuenta de almacenamiento. **Obligatorio** al usar `storage_account_url` sin SAS; **no** debe pasarse al usar `connection_string`.                                                                                                                                                                                                                                                                                                                                                                                        |
| `account_key`                    | Clave de la cuenta de almacenamiento. **Obligatoria** al usar `storage_account_url` sin SAS; **no** debe pasarse al usar `connection_string`.                                                                                                                                                                                                                                                                                                                                                                                         |
| `format`                         | El [formato](/es/sql-reference/formats) del archivo.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `compression`                    | Valores admitidos: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. De forma predeterminada, la compresión se detecta automáticamente según la extensión del archivo (igual que al establecer `auto`).                                                                                                                                                                                                                                                                                                                              |
| `structure`                      | Estructura de la tabla. Formato: `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `partition_strategy`             | Opcional. Valores admitidos: `WILDCARD` o `HIVE`. `WILDCARD` requiere `{_partition_id}` en la ruta, que se reemplaza por la clave de partición. `HIVE` no permite comodines, asume que la ruta es la raíz de la tabla y genera directorios particionados al estilo Hive con Snowflake IDs como nombres de archivo y el formato del archivo como extensión. El valor predeterminado es la setting `file_like_engine_default_partition_strategy` (`WILDCARD` en settings de `compatibility` anteriores a `26.6`; `HIVE` en caso contrario). |
| `partition_columns_in_data_file` | Opcional. Solo se usa con la estrategia de partición `HIVE`. Indica a ClickHouse si debe esperar que las columnas de partición se escriban en el archivo de datos. El valor predeterminado es `false`.                                                                                                                                                                                                                                                                                                                                    |
| `extra_credentials`              | Usa `client_id` y `tenant_id` para la autenticación. Si se proporcionan `extra_credentials`, tienen prioridad sobre `account_name` y `account_key`.                                                                                                                                                                                                                                                                                                                                                                                       |

<div id="named-collections">
  ## Colecciones con nombre
</div>

Los argumentos también se pueden pasar mediante [colecciones con nombre](/es/operations/named-collections). En este caso, se admiten las siguientes claves:

| Key                   | Required | Description                                                                                                        |
| --------------------- | -------- | ------------------------------------------------------------------------------------------------------------------ |
| `container`           | Sí       | Nombre del contenedor. Corresponde al argumento posicional `container_name`.                                       |
| `blob_path`           | Sí       | Ruta de archivo (con comodines opcionales). Corresponde al argumento posicional `blobpath`.                        |
| `connection_string`   | No*      | Cadena de conexión con credenciales integradas. *Debe proporcionarse `connection_string` o `storage_account_url`.  |
| `storage_account_url` | No*      | URL del endpoint de la cuenta de almacenamiento. *Debe proporcionarse `connection_string` o `storage_account_url`. |
| `account_name`        | No       | Obligatorio al usar `storage_account_url`                                                                          |
| `account_key`         | No       | Obligatorio al usar `storage_account_url`                                                                          |
| `format`              | No       | Formato de archivo.                                                                                                |
| `compression`         | No       | Tipo de compresión.                                                                                                |
| `structure`           | No       | Estructura de la tabla.                                                                                            |
| `client_id`           | No       | ID del client para la autenticación.                                                                               |
| `tenant_id`           | No       | ID del tenant para la autenticación.                                                                               |

:::note
Los nombres de las claves de la colección con nombre difieren de los nombres de los argumentos posicionales de la función: `container` (no `container_name`) y `blob_path` (no `blobpath`).
:::

**Ejemplo:**

```sql
CREATE NAMED COLLECTION azure_my_data AS
    storage_account_url = 'https://myaccount.blob.core.windows.net/',
    container = 'mycontainer',
    blob_path = 'data/*.parquet',
    account_name = 'myaccount',
    account_key = 'mykey...==',
    format = 'Parquet';

SELECT *
FROM azureBlobStorage(azure_my_data)
LIMIT 5;
```

También puede sobrescribir, en tiempo de consulta, los valores de una colección con nombre:

```sql
SELECT *
FROM azureBlobStorage(azure_my_data, blob_path = 'other_data/*.csv', format = 'CSVWithNames')
LIMIT 5;
```

<div id="returned_value">
  ## Valor devuelto
</div>

Una tabla con la estructura especificada para la lectura o escritura de datos en el archivo especificado.

<div id="examples">
  ## Ejemplos
</div>

<div id="reading-with-storage-account-url">
  ### Lectura con la forma `storage_account_url`
</div>

```sql
SELECT *
FROM azureBlobStorage(
    'https://myaccount.blob.core.windows.net/',
    'mycontainer',
    'data/*.parquet',
    'myaccount',
    'mykey...==',
    'Parquet'
)
LIMIT 5;
```

<div id="reading-with-connection-string">
  ### Lectura con la forma `connection_string`
</div>

```sql
SELECT *
FROM azureBlobStorage(
    'DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey...==;EndPointSuffix=core.windows.net',
    'mycontainer',
    'data/*.csv',
    'CSVWithNames'
)
LIMIT 5;
```

<div id="writing-with-partitions">
  ### Escritura con particiones
</div>

```sql
INSERT INTO TABLE FUNCTION azureBlobStorage(
    'DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey...==;EndPointSuffix=core.windows.net',
    'mycontainer',
    'test_{_partition_id}.csv',
    'CSV',
    'auto',
    'column1 UInt32, column2 UInt32, column3 UInt32'
) PARTITION BY column3
VALUES (1, 2, 3), (3, 2, 1), (78, 43, 3);
```

Luego, lea de nuevo una partición específica:

```sql
SELECT *
FROM azureBlobStorage(
    'DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey...==;EndPointSuffix=core.windows.net',
    'mycontainer',
    'test_1.csv',
    'CSV',
    'auto',
    'column1 UInt32, column2 UInt32, column3 UInt32'
);
```

```response
┌─column1─┬─column2─┬─column3─┐
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

<div id="virtual-columns">
  ## Columnas virtuales
</div>

* `_path` — Ruta del archivo. Tipo: `LowCardinality(String)`.
* `_file` — Nombre del archivo. Tipo: `LowCardinality(String)`.
* `_size` — Tamaño del archivo en bytes. Tipo: `Nullable(UInt64)`. Si se desconoce el tamaño del archivo, el valor es `NULL`.
* `_time` — Hora de la última modificación del archivo. Tipo: `Nullable(DateTime)`. Si se desconoce la hora, el valor es `NULL`.

<div id="partitioned-write">
  ## Escritura por particiones
</div>

<div id="partition-strategy">
  ### Estrategia de partición
</div>

Solo es compatible con consultas `INSERT`.

`WILDCARD`: Reemplaza el comodín `{_partition_id}` en la ruta del archivo con la clave de partición real. Se selecciona de forma predeterminada solo con la configuración `compatibility` anterior a `26.6`; de lo contrario, el valor predeterminado es `HIVE` (consulta la configuración `file_like_engine_default_partition_strategy`).

`HIVE` implementa el particionamiento con estilo Hive para lecturas y escrituras. Genera archivos con el siguiente formato: `<prefix>/<key1=val1/key2=val2...>/<snowflakeid>.<toLower(file_format)>`.

**Ejemplo de estrategia de partición `HIVE`**

```sql
INSERT INTO TABLE FUNCTION azureBlobStorage(
    azure_conf2,
    storage_account_url = 'https://myaccount.blob.core.windows.net/',
    container = 'cont',
    blob_path = 'azure_table_root',
    format = 'CSVWithNames',
    compression = 'auto',
    structure = 'year UInt16, country String, id Int32',
    partition_strategy = 'hive'
) PARTITION BY (year, country)
VALUES (2020, 'Russia', 1), (2021, 'Brazil', 2);
```

```result
SELECT _path, * FROM azureBlobStorage(
    azure_conf2,
    storage_account_url = 'https://myaccount.blob.core.windows.net/',
    container = 'cont',
    blob_path = 'azure_table_root/**.csvwithnames'
)

   ┌─_path───────────────────────────────────────────────────────────────────────────┬─id─┬─year─┬─country─┐
1. │ cont/azure_table_root/year=2021/country=Brazil/7351307847391293440.csvwithnames │  2 │ 2021 │ Brazil  │
2. │ cont/azure_table_root/year=2020/country=Russia/7351307847378710528.csvwithnames │  1 │ 2020 │ Russia  │
   └─────────────────────────────────────────────────────────────────────────────────┴────┴──────┴─────────┘
```

<div id="hive-style-partitioning">
  ## ajuste use_hive_partitioning
</div>

Esta es una indicación para que ClickHouse analice archivos particionados con estilo Hive al leerlos. No tiene efecto al escribir. Para que las lecturas y escrituras sean simétricas, use el argumento `partition_strategy`.

Si `use_hive_partitioning` se establece en 1, ClickHouse detectará el particionamiento de estilo Hive en la ruta (`/name=value/`) y permitirá usar las columnas de partición como columnas virtuales en la consulta. Estas columnas virtuales tendrán los mismos nombres que en la ruta particionada.

**Ejemplo**

Uso de una columna virtual creada con particionamiento de estilo Hive

```sql
SELECT * FROM azureBlobStorage(config, storage_account_url='...', container='...', blob_path='http://data/path/date=*/country=*/code=*/*.parquet') WHERE date > '2020-01-01' AND country = 'Netherlands' AND code = 42;
```

<div id="using-shared-access-signatures-sas-sas-tokens">
  ## Uso de firmas de acceso compartido (SAS)
</div>

Una firma de acceso compartido (SAS) es un URI que concede acceso restringido a un contenedor o archivo de Azure Storage. Úsela para proporcionar acceso con tiempo limitado a los recursos de una cuenta de almacenamiento sin compartir la clave de la cuenta de almacenamiento. Más detalles [aquí](https://learn.microsoft.com/en-us/rest/api/storageservices/delegate-access-with-shared-access-signature).

La función `azureBlobStorage` admite firmas de acceso compartido (SAS).

Un [Blob SAS token](https://learn.microsoft.com/en-us/azure/ai-services/translator/document-translation/how-to-guides/create-sas-tokens?tabs=Containers) contiene toda la información necesaria para autenticar la solicitud, incluido el blob de destino, los permisos y el período de validez. Para construir una URL del blob, añada el SAS token al endpoint del servicio de blobs. Por ejemplo, si el endpoint es `https://clickhousedocstest.blob.core.windows.net/`, la solicitud pasa a ser:

```sql
SELECT count()
FROM azureBlobStorage('BlobEndpoint=https://clickhousedocstest.blob.core.windows.net/;SharedAccessSignature=sp=r&st=2025-01-29T14:58:11Z&se=2025-01-29T22:58:11Z&spr=https&sv=2022-11-02&sr=c&sig=Ac2U0xl4tm%2Fp7m55IilWl1yHwk%2FJG0Uk6rMVuOiD0eE%3D', 'exampledatasets', 'example.csv')

┌─count()─┐
│      10 │
└─────────┘

1 row in set. Elapsed: 0.425 sec.
```

Como alternativa, los usuarios pueden usar la [URL SAS de Blob generada](https://learn.microsoft.com/en-us/azure/ai-services/translator/document-translation/how-to-guides/create-sas-tokens?tabs=Containers):

```sql
SELECT count()
FROM azureBlobStorage('https://clickhousedocstest.blob.core.windows.net/?sp=r&st=2025-01-29T14:58:11Z&se=2025-01-29T22:58:11Z&spr=https&sv=2022-11-02&sr=c&sig=Ac2U0xl4tm%2Fp7m55IilWl1yHwk%2FJG0Uk6rMVuOiD0eE%3D', 'exampledatasets', 'example.csv')

┌─count()─┐
│      10 │
└─────────┘

1 row in set. Elapsed: 0.153 sec.
```

<div id="related">
  ## Relacionado
</div>

* [Motor de tabla AzureBlobStorage](/es/engines/table-engines/integrations/azureBlobStorage.md)