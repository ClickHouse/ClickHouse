---
description: 'Este motor ofrece integración con el ecosistema de Azure Blob Storage.'
sidebar_label: 'Azure Blob Storage'
sidebar_position: 10
slug: /engines/table-engines/integrations/azureBlobStorage
title: 'Motor de tabla AzureBlobStorage'
doc_type: 'referencia'
---

Este motor ofrece integración con el ecosistema de [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs).

<div id="create-table">
  ## Crear una tabla
</div>

```sql
CREATE TABLE azure_blob_storage_table (name String, value UInt32)
    ENGINE = AzureBlobStorage(connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression, partition_strategy, partition_columns_in_data_file, extra_credentials(client_id=, tenant_id=)])
    [PARTITION BY expr]
    [SETTINGS ...]
```

<div id="engine-parameters">
  ### Parámetros del motor
</div>

* `endpoint` — URL del endpoint de AzureBlobStorage con contenedor y prefijo. Opcionalmente, puede incluir account&#95;name si el método de autenticación utilizado lo requiere. (`http://azurite1:{port}/[account_name]{container_name}/{data_prefix}`) o estos parámetros pueden proporcionarse por separado mediante storage&#95;account&#95;url, account&#95;name &amp; container. Para especificar el prefijo, debe usarse endpoint.
* `endpoint_contains_account_name` - Esta marca se usa para indicar si endpoint contiene account&#95;name, ya que solo es necesario para determinados métodos de autenticación. (Valor predeterminado: true)
* `connection_string|storage_account_url` — connection&#95;string incluye el nombre y la clave de la cuenta ([Crear cadena de conexión](https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json\&bc=%2Fazure%2Fstorage%2Fblobs%2Fbreadcrumb%2Ftoc.json#configure-a-connection-string-for-an-azure-storage-account)) o también puede proporcionar aquí la URL de la cuenta de almacenamiento, así como el nombre y la clave de la cuenta como parámetros independientes (consulte los parámetros account&#95;name &amp; account&#95;key)
* `container_name` - Nombre del contenedor
* `blobpath` - ruta de archivo. Admite los siguientes wildcards en modo readonly: `*`, `**`, `?`, `{abc,def}` y `{N..M}`, donde `N`, `M` — números, `'abc'`, `'def'` — cadenas.
* `account_name` - si se usa storage&#95;account&#95;url, aquí puede especificarse el nombre de la cuenta
* `account_key` - si se usa storage&#95;account&#95;url, aquí puede especificarse la clave de la cuenta
* `format` — El [formato](/es/interfaces/formats.md) del archivo.
* `compression` — Valores admitidos: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. De forma predeterminada, detecta automáticamente la compresión por la extensión del archivo. (equivale a establecerlo en `auto`).
* `partition_strategy` – Opciones: `wildcard` o `hive`. `wildcard` requiere un `{_partition_id}` en la ruta, que se sustituye por la clave de partición. `hive` no permite wildcards, asume que la ruta es la raíz de la tabla y genera directorios particionados al estilo Hive con Snowflake IDs como nombres de archivo y el formato de archivo como extensión. El valor predeterminado es la configuración `file_like_engine_default_partition_strategy` (`wildcard` con configuraciones de `compatibility` anteriores a `26.6`; `hive` en caso contrario).
* `partition_columns_in_data_file` - Solo se usa con la estrategia de partición `hive`. Indica a ClickHouse si debe esperar que las columnas de partición se escriban en el archivo de datos. El valor predeterminado es `false`.
* `extra_credentials` - Use `client_id` y `tenant_id` para la autenticación. Si se proporcionan extra&#95;credentials, tienen prioridad sobre `account_name` y `account_key`.

**Ejemplo**

Los usuarios pueden usar el emulador Azurite para el desarrollo local de Azure Storage. Más detalles [aquí](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=docker-hub%2Cblob-storage). Si usa una instancia local de Azurite, puede que sea necesario sustituir `http://localhost:10000` por `http://azurite1:10000` en los comandos siguientes, donde asumimos que Azurite está disponible en el host `azurite1`.

```sql
CREATE TABLE test_table (key UInt64, data String)
    ENGINE = AzureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1/;', 'testcontainer', 'test_table', 'CSV');

INSERT INTO test_table VALUES (1, 'a'), (2, 'b'), (3, 'c');

SELECT * FROM test_table;
```

```text
┌─key──┬─data──┐
│  1   │   a   │
│  2   │   b   │
│  3   │   c   │
└──────┴───────┘
```

<div id="virtual-columns">
  ## Columnas virtuales
</div>

* `_path` — Ruta del archivo. Tipo: `LowCardinality(String)`.
* `_file` — Nombre del archivo. Tipo: `LowCardinality(String)`.
* `_size` — Tamaño del archivo en bytes. Tipo: `Nullable(UInt64)`. Si se desconoce el tamaño, el valor es `NULL`.
* `_time` — Fecha y hora de la última modificación del archivo. Tipo: `Nullable(DateTime)`. Si se desconoce la hora, el valor es `NULL`.

<div id="authentication">
  ## Autenticación
</div>

Actualmente, hay 3 formas de autenticarse:

* `Managed Identity`: se puede usar proporcionando un `endpoint`, `connection_string` o `storage_account_url`.
* `SAS Token`: se puede usar proporcionando un `endpoint`, `connection_string` o `storage_account_url`. Se identifica por la presencia de `?` en la URL. Consulte [azureBlobStorage](/es/sql-reference/table-functions/azureBlobStorage#using-shared-access-signatures-sas-sas-tokens) para ver ejemplos.
* `Workload Identity`: se puede usar proporcionando un `endpoint` o `storage_account_url`. Si el parámetro `use_workload_identity` está establecido en la configuración, se usa [Workload Identity](https://github.com/Azure/azure-sdk-for-cpp/tree/main/sdk/identity/azure-identity#authenticate-azure-hosted-applications) para la autenticación.

<div id="data-cache">
  ### Caché de datos
</div>

El motor de tabla `Azure` admite el almacenamiento en caché de datos en el disco local.
Consulta las opciones de configuración y uso de la caché del sistema de archivos en esta [sección](/es/operations/storing-data.md/#using-local-cache).
El almacenamiento en caché se realiza en función de la ruta y el ETag del objeto de almacenamiento, por lo que ClickHouse no leerá una versión obsoleta de la caché.

Para habilitar la caché, usa la opción `filesystem_cache_name = '<name>'` y `enable_filesystem_cache = 1`.

```sql
SELECT *
FROM azureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1/;', 'testcontainer', 'test_table', 'CSV')
SETTINGS filesystem_cache_name = 'cache_for_azure', enable_filesystem_cache = 1;
```

1. añade la siguiente sección al archivo de configuración de ClickHouse:

```xml
<clickhouse>
    <filesystem_caches>
        <cache_for_azure>
            <path>path to cache directory</path>
            <max_size>10Gi</max_size>
        </cache_for_azure>
    </filesystem_caches>
</clickhouse>
```

2. reutilizar la configuración de caché (y, por tanto, el almacenamiento en caché) de la sección `storage_configuration` de ClickHouse, [descrita aquí](/es/operations/storing-data.md/#using-local-cache)

<div id="partition-by">
  ### PARTITION BY
</div>

`PARTITION BY` — Opcional. En la mayoría de los casos no necesitas una clave de partición y, si la necesitas, por lo general no hace falta que sea más granular que por mes. El particionado no acelera las consultas (a diferencia de la expresión ORDER BY). Nunca debes usar un particionado demasiado granular. No particiones tus datos por identificadores o nombres de cliente (en su lugar, haz que el identificador o el nombre del cliente sea la primera columna de la expresión ORDER BY).

Para particionar por mes, usa la expresión `toYYYYMM(date_column)`, donde `date_column` es una columna con una fecha del tipo [Date](/es/sql-reference/data-types/date.md). Los nombres de las particiones aquí tienen el formato `"YYYYMM"`.

<div id="partition-strategy">
  #### Estrategia de partición
</div>

`wildcard`: Reemplaza el comodín `{_partition_id}` en la ruta del archivo por la clave de partición real. La lectura no se admite. Se selecciona de forma predeterminada solo con configuraciones de `compatibility` anteriores a `26.6`; de lo contrario, el valor predeterminado es `hive` (consulta la configuración `file_like_engine_default_partition_strategy`).

`hive` implementa el particionado de estilo Hive para lecturas y escrituras. La lectura se implementa mediante un patrón glob recursivo. La escritura genera archivos con el siguiente formato: `<prefix>/<key1=val1/key2=val2...>/<snowflakeid>.<toLower(file_format)>`.

Nota: Al usar la estrategia de partición `hive`, la configuración `use_hive_partitioning` no tiene ningún efecto.

Ejemplo de la estrategia de partición `hive`:

```sql
arthur :) create table azure_table (year UInt16, country String, counter UInt8) ENGINE=AzureBlobStorage(account_name='devstoreaccount1', account_key='Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', storage_account_url = 'http://localhost:30000/devstoreaccount1', container='cont', blob_path='hive_partitioned', format='Parquet', compression='auto', partition_strategy='hive') PARTITION BY (year, country);

arthur :) insert into azure_table values (2020, 'Russia', 1), (2021, 'Brazil', 2);

arthur :) select _path, * from azure_table;

   ┌─_path──────────────────────────────────────────────────────────────────────┬─year─┬─country─┬─counter─┐
1. │ cont/hive_partitioned/year=2020/country=Russia/7351305360873664512.parquet │ 2020 │ Russia  │       1 │
2. │ cont/hive_partitioned/year=2021/country=Brazil/7351305360894636032.parquet │ 2021 │ Brazil  │       2 │
   └────────────────────────────────────────────────────────────────────────────┴──────┴─────────┴─────────┘
```

<div id="see-also">
  ## Véase también
</div>

[Función de tabla de Azure Blob Storage](/es/sql-reference/table-functions/azureBlobStorage)