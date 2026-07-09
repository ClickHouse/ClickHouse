---
description: 'Este motor ofrece integración de solo lectura con tablas de Delta Lake existentes en Amazon S3.'
sidebar_label: 'DeltaLake'
sidebar_position: 40
slug: /engines/table-engines/integrations/deltalake
title: 'motor de tabla DeltaLake'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="deltalake-table-engine">
  # motor de tabla DeltaLake
</div>

Este motor ofrece integración con tablas [Delta Lake](https://github.com/delta-io/delta) existentes en el almacenamiento de S3, GCP y Azure, y admite tanto lectura como escritura (a partir de la v25.10).

<div id="create-table">
  ## Crear una tabla DeltaLake
</div>

Para crear una tabla DeltaLake, esta debe existir previamente en el almacenamiento de S3, GCP o Azure. Los siguientes comandos no aceptan parámetros DDL para crear una tabla nueva.

<Tabs>
  <TabItem value="S3" label="S3" default>
    **Sintaxis**

    ```sql
    CREATE TABLE table_name
    ENGINE = DeltaLake(url, [aws_access_key_id, aws_secret_access_key,] [extra_credentials])
    ```

    **Parámetros del motor**

    * `url` — URL del bucket con la ruta a la tabla Delta Lake existente.
    * `aws_access_key_id`, `aws_secret_access_key` - Credenciales de larga duración para el usuario de la cuenta de [AWS](https://aws.amazon.com/). Puede usarlas para autenticar sus solicitudes. El parámetro es opcional. Si no se especifican credenciales, se usarán las del archivo de configuración.
    * `extra_credentials` - Opcional. Se usa para pasar un `role_arn` para el acceso basado en roles en ClickHouse Cloud. Consulte [Secure S3](/es/cloud/data-sources/secure-s3) para ver los pasos de configuración.

    Los parámetros del motor se pueden especificar mediante [colecciones con nombre](/es/operations/named-collections.md).

    **Ejemplo**

    ```sql
    CREATE TABLE deltalake
    ENGINE = DeltaLake('http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/test_table/', 'ABC123', 'Abc+123')
    ```

    Uso de colecciones con nombre:

    ```xml
    <clickhouse>
        <named_collections>
            <deltalake_conf>
                <url>http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/</url>
                <access_key_id>ABC123</access_key_id>
                <secret_access_key>Abc+123</secret_access_key>
            </deltalake_conf>
        </named_collections>
    </clickhouse>
    ```

    ```sql
    CREATE TABLE deltalake
    ENGINE = DeltaLake(deltalake_conf, filename = 'test_table')
    ```
  </TabItem>

  <TabItem value="GCP" label="GCP" default>
    **Sintaxis**

    ```sql
    -- Uso de URL HTTPS (recomendado)
    CREATE TABLE table_name
    ENGINE = DeltaLake('https://storage.googleapis.com/<bucket>/<path>/', '<access_key_id>', '<secret_access_key>')
    ```

    :::note[URI de gsutil no compatible]
    No se admite un URI de gsutil como `gs://clickhouse-docs-example-bucket`; use una URL que comience por `https://storage.googleapis.com`
    :::

    **Argumentos**

    * `url` — URL del bucket de GCS que apunta a la tabla Delta Lake. Debe usar el formato `https://storage.googleapis.com/<bucket>/<path>/`
      (el endpoint de la API XML de GCS), o `gs://<bucket>/<path>/`, que se convierte automáticamente.
    * `access_key_id` — Clave de acceso de GCS. Créela en Google Cloud Console → Cloud Storage → Settings → Interoperability.
    * `secret_access_key` — Secreto de GCS.

    **Colecciones con nombre**

    También puede usar colecciones con nombre.
    Por ejemplo:

    ```sql
    CREATE NAMED COLLECTION gcs_creds AS
    access_key_id = '<access_key>',
    secret_access_key = '<secret>';

    CREATE TABLE gcpDeltaLake
    ENGINE = DeltaLake(gcs_creds, url = 'https://storage.googleapis.com/<bucket>/<path>')
    ```
  </TabItem>

  <TabItem value="Azure" label="Azure" default>
    **Sintaxis**

    ```sql
    CREATE TABLE table_name
    ENGINE = DeltaLake(connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression])
    ```

    **Argumentos**

    * `connection_string` — cadena de conexión de Azure
    * `storage_account_url` — URL de la cuenta de almacenamiento de Azure (por ejemplo, https://account.blob.core.windows.net)
    * `container_name` — nombre del contenedor de Azure
    * `blobpath` — ruta a la tabla Delta Lake dentro del contenedor
    * `account_name` — nombre de la cuenta de almacenamiento de Azure
    * `account_key` — clave de la cuenta de almacenamiento de Azure
  </TabItem>
</Tabs>

<div id="insert-data">
  ## Escribir datos con una tabla DeltaLake
</div>

Una vez que hayas creado una tabla con el motor de tabla DeltaLake, puedes insertar datos en ella con:

```sql
SET allow_delta_lake_writes = 1;

INSERT INTO deltalake(id, firstname, lastname, gender, age)
VALUES (1, 'John', 'Smith', 'M', 32);
```

:::note
La escritura con el motor de tabla solo es compatible a través de delta kernel.
La escritura en Azure todavía no es compatible, pero sí en S3 y GCS.

Las escrituras en Delta Lake son una función Beta y deben habilitarse con `SET allow_delta_lake_writes = 1` (disponible a partir de la versión 26.7; en versiones anteriores, use `SET allow_experimental_delta_lake_writes = 1`).
:::

<div id="data-cache">
  ### Caché de datos
</div>

El motor de tabla `DeltaLake` y la función de tabla admiten el caché de datos, al igual que los almacenamientos `S3`, `AzureBlobStorage` y `HDFS`. Consulta [&quot;motor de tabla S3&quot;](../../../engines/table-engines/integrations/s3.md#data-cache) para obtener más información.

<div id="see-also">
  ## Véase también
</div>

* [función de tabla DeltaLake](../../../sql-reference/table-functions/deltalake.md)