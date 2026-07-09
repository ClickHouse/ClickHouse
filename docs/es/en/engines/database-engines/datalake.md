---
description: 'El motor de base de datos DataLakeCatalog le permite conectar ClickHouse a catálogos de datos externos y consultar datos en formato de tabla abierto'
sidebar_label: 'DataLakeCatalog'
slug: /engines/database-engines/datalakecatalog
title: 'DataLakeCatalog'
doc_type: 'reference'
---

El motor de base de datos `DataLakeCatalog` le permite conectar ClickHouse a
catálogos de datos externos y consultar datos en formato de tabla abierto sin necesidad
de duplicarlos.
Esto convierte a ClickHouse en un potente motor de consulta que se integra perfectamente con
su infraestructura de lago de datos existente.

<div id="supported-catalogs">
  ## Catálogos compatibles
</div>

El motor `DataLakeCatalog` admite los siguientes catálogos de datos:

* **AWS Glue Catalog** - Para tablas Iceberg en entornos de AWS
* **Databricks Unity Catalog** - Para tablas Delta Lake e Iceberg
* **Hive Metastore** - Catálogo tradicional del ecosistema de Hadoop
* **REST Catalogs** - Cualquier catálogo compatible con la especificación REST de Iceberg

<div id="creating-a-database">
  ## Crear una base de datos
</div>

Para usar el motor `DataLakeCatalog`, deberá habilitar los ajustes correspondientes que se indican a continuación:

```sql
SET allow_experimental_database_iceberg = 1;
SET allow_experimental_database_unity_catalog = 1;
SET allow_experimental_database_glue_catalog = 1;
SET allow_experimental_database_hms_catalog = 1;
SET allow_experimental_database_paimon_rest_catalog = 1;
```

Las bases de datos con el motor `DataLakeCatalog` pueden crearse con la siguiente sintaxis:

```sql
CREATE DATABASE database_name
ENGINE = DataLakeCatalog(catalog_endpoint[, user, password])
SETTINGS
catalog_type,
[...]
```

Se admiten las siguientes configuraciones:

| Setting                 | Description                                                                                                                                                                                                                                                                                                                                                                                                     |
| ----------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `catalog_type`          | Tipo de catálogo: `glue`, `unity` (Delta), `rest` (Iceberg), `hive`, `onelake` (Iceberg)                                                                                                                                                                                                                                                                                                                        |
| `warehouse`             | Nombre del warehouse o de la base de datos que se usará en el catálogo.                                                                                                                                                                                                                                                                                                                                         |
| `catalog_credential`    | Credencial de autenticación para el catálogo (por ejemplo, una API key o un token)                                                                                                                                                                                                                                                                                                                              |
| `auth_header`           | HTTP header personalizado para la autenticación con el servicio de catálogo                                                                                                                                                                                                                                                                                                                                     |
| `auth_scope`            | Alcance de OAuth2 para la autenticación (si se usa OAuth)                                                                                                                                                                                                                                                                                                                                                       |
| `storage_endpoint`      | URL del endpoint para el almacenamiento subyacente                                                                                                                                                                                                                                                                                                                                                              |
| `oauth_server_uri`      | URI del servidor de autorización de OAuth2 para la autenticación                                                                                                                                                                                                                                                                                                                                                |
| `vended_credentials`    | Booleano que indica si se usarán las credenciales proporcionadas por el catálogo (compatible con AWS S3 y Azure ADLS Gen2)                                                                                                                                                                                                                                                                                      |
| `aws_access_key_id`     | ID de la clave de acceso de AWS para acceder a S3/Glue (si no se usan credenciales proporcionadas)                                                                                                                                                                                                                                                                                                              |
| `aws_secret_access_key` | Clave secreta de acceso de AWS para acceder a S3/Glue (si no se usan credenciales proporcionadas)                                                                                                                                                                                                                                                                                                               |
| `region`                | Región de AWS del servicio (por ejemplo, `us-east-1`)                                                                                                                                                                                                                                                                                                                                                           |
| `dlf_access_key_id`     | ID de la clave de acceso para acceder a DLF                                                                                                                                                                                                                                                                                                                                                                     |
| `dlf_access_key_secret` | Clave secreta de acceso para acceder a DLF                                                                                                                                                                                                                                                                                                                                                                      |
| `force_add_bucket`      | Al construir URL de almacenamiento de objetos a partir de la ubicación de la tabla proporcionada por el catálogo y `storage_endpoint`, antepone el nombre del bucket o contenedor aunque el endpoint ya lo incluya. Valor predeterminado: `false`. Establézcalo en `true` para catálogos que devuelven rutas sin el bucket y requieren añadirlo en el paso de construcción de la URL (rutas de estilo Polaris). |

<div id="examples">
  ## Ejemplos
</div>

Consulta las secciones siguientes para ver ejemplos de uso del motor `DataLakeCatalog`:

* [Unity Catalog](/es/use-cases/data-lake/unity-catalog)
* [Glue Catalog](/es/use-cases/data-lake/glue-catalog)
* OneLake Catalog
  Puede usarse habilitando `allow_experimental_database_iceberg` o `allow_database_iceberg`.

```sql
CREATE DATABASE database_name
ENGINE = DataLakeCatalog(catalog_endpoint)
SETTINGS
    catalog_type = 'onelake',
    warehouse = warehouse,
    onelake_tenant_id = tenant_id,
    oauth_server_uri = server_uri,
    auth_scope = auth_scope,
    onelake_client_id = client_id,
    onelake_client_secret = client_secret;
SHOW TABLES IN database_name;
SELECT count() from database_name.table_name;
```