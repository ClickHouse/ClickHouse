---
description: 'Permite procesar archivos de Azure Blob Storage en paralelo con muchos
  nodos en un clúster especificado.'
sidebar_label: 'azureBlobStorageCluster'
sidebar_position: 15
slug: /sql-reference/table-functions/azureBlobStorageCluster
title: 'azureBlobStorageCluster'
doc_type: 'reference'
---

Permite procesar archivos de [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs) en paralelo con muchos nodos en un clúster especificado. En el nodo iniciador, crea una conexión con todos los nodos del clúster, expande los asteriscos en la ruta de archivo de S3 y distribuye cada archivo de forma dinámica. En el nodo worker, consulta al iniciador cuál es la siguiente tarea que debe procesar y la procesa. Esto se repite hasta que todas las tareas finalizan.
Esta función de tabla es similar a la [función s3Cluster](../../sql-reference/table-functions/s3Cluster.md).

<div id="syntax">
  ## Sintaxis
</div>

```sql
azureBlobStorageCluster(cluster_name, connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression, structure])
```

<div id="arguments">
  ## Argumentos
</div>

| Argument            | Descripción                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| ------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster_name`      | Nombre de un clúster que se utiliza para crear un conjunto de direcciones y parámetros de conexión para servidores remotos y locales.                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `connection_string` | storage&#95;account&#95;url&#96; — connection&#95;string incluye el nombre y la clave de la cuenta ([Crear cadena de conexión](https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json\&bc=%2Fazure%2Fstorage%2Fblobs%2Fbreadcrumb%2Ftoc.json#configure-a-connection-string-for-an-azure-storage-account)); también puede proporcionar aquí la URL de la cuenta de almacenamiento y el nombre y la clave de la cuenta como parámetros independientes (consulte los parámetros account&#95;name y account&#95;key) |
| `container_name`    | Nombre del contenedor                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `blobpath`          | Ruta del archivo. Admite los siguientes comodines en modo readonly: `*`, `**`, `?`, `{abc,def}` y `{N..M}`, donde `N`, `M` — números, `'abc'`, `'def'` — cadenas.                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `account_name`      | Si se usa storage&#95;account&#95;url, aquí se puede especificar el nombre de la cuenta                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `account_key`       | Si se usa storage&#95;account&#95;url, aquí se puede especificar la clave de la cuenta                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `format`            | El [formato](/es/sql-reference/formats) del archivo.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `compression`       | Valores admitidos: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. De forma predeterminada, la compresión se detecta automáticamente según la extensión del archivo. (equivale a establecerlo en `auto`).                                                                                                                                                                                                                                                                                                                                                                               |
| `structure`         | Estructura de la tabla. Formato: `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |

<div id="returned_value">
  ## Valor devuelto
</div>

Una tabla con la estructura indicada para leer o escribir datos en el archivo especificado.

<div id="examples">
  ## Ejemplos
</div>

Al igual que con el motor de tabla [AzureBlobStorage](/es/engines/table-engines/integrations/azureBlobStorage), los usuarios pueden usar el emulador Azurite para el desarrollo local de Azure Storage. Encontrará más detalles [aquí](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=docker-hub%2Cblob-storage). A continuación, asumimos que Azurite está disponible en el hostname `azurite1`.

Seleccione el recuento del archivo `test_cluster_*.csv` usando todos los nodos del clúster `cluster_simple`:

```sql
SELECT count(*) FROM azureBlobStorageCluster(
        'cluster_simple', 'http://azurite1:10000/devstoreaccount1', 'testcontainer', 'test_cluster_count.csv', 'devstoreaccount1',
        'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'CSV',
        'auto', 'key UInt64')
```

<div id="using-shared-access-signatures-sas-sas-tokens">
  ## Uso de firmas de acceso compartido (SAS)
</div>

Consulta [azureBlobStorage](/es/sql-reference/table-functions/azureBlobStorage#using-shared-access-signatures-sas-sas-tokens) para ver ejemplos.

<div id="related">
  ## Relacionado
</div>

* [motor AzureBlobStorage](../../engines/table-engines/integrations/azureBlobStorage.md)
* [función de tabla azureBlobStorage](../../sql-reference/table-functions/azureBlobStorage.md)