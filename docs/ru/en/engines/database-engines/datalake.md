---
description: 'Движок базы данных DataLakeCatalog позволяет подключать ClickHouse к внешним каталогам данных и выполнять запросы к данным в открытых табличных форматах'
sidebar_label: 'DataLakeCatalog'
slug: /engines/database-engines/datalakecatalog
title: 'DataLakeCatalog'
doc_type: 'reference'
---

Движок базы данных `DataLakeCatalog` позволяет подключать ClickHouse к внешним
каталогам данных и выполнять запросы к данным в открытых табличных форматах без
необходимости дублировать данные.
Это превращает ClickHouse в мощный движок запросов, который органично работает с
вашей существующей инфраструктурой озера данных.

<div id="supported-catalogs">
  ## Поддерживаемые каталоги
</div>

Движок `DataLakeCatalog` поддерживает следующие каталоги данных:

* **Каталог AWS Glue** - Для таблиц Iceberg в средах AWS
* **Databricks Unity Catalog** - Для таблиц Delta Lake и Iceberg
* **Hive Metastore** - Традиционный каталог экосистемы Hadoop
* **REST Catalogs** - Любые каталоги, поддерживающие спецификацию Iceberg REST

<div id="creating-a-database">
  ## Создание базы данных
</div>

Чтобы использовать движок `DataLakeCatalog`, необходимо включить указанные ниже настройки:

```sql
SET allow_experimental_database_iceberg = 1;
SET allow_experimental_database_unity_catalog = 1;
SET allow_experimental_database_glue_catalog = 1;
SET allow_experimental_database_hms_catalog = 1;
SET allow_experimental_database_paimon_rest_catalog = 1;
```

Базы данных с движком `DataLakeCatalog` можно создавать с помощью следующего синтаксиса:

```sql
CREATE DATABASE database_name
ENGINE = DataLakeCatalog(catalog_endpoint[, user, password])
SETTINGS
catalog_type,
[...]
```

Поддерживаются следующие настройки:

| Setting                 | Description                                                                                                                                                                                                                                                                                                                                                                                        |
| ----------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `catalog_type`          | Тип каталога: `glue`, `unity` (Delta), `rest` (Iceberg), `hive`, `onelake` (Iceberg)                                                                                                                                                                                                                                                                                                               |
| `warehouse`             | Имя хранилища/базы данных, используемое в каталоге.                                                                                                                                                                                                                                                                                                                                                |
| `catalog_credential`    | Учетные данные для аутентификации в каталоге (например, API key или token)                                                                                                                                                                                                                                                                                                                         |
| `auth_header`           | Пользовательский HTTP-заголовок для аутентификации в сервисе каталога                                                                                                                                                                                                                                                                                                                              |
| `auth_scope`            | Область OAuth2 для аутентификации (если используется OAuth)                                                                                                                                                                                                                                                                                                                                        |
| `storage_endpoint`      | URL конечной точки для нижележащего хранилища                                                                                                                                                                                                                                                                                                                                                      |
| `oauth_server_uri`      | URI сервера авторизации OAuth2 для аутентификации                                                                                                                                                                                                                                                                                                                                                  |
| `vended_credentials`    | Булевый признак того, следует ли использовать учетные данные, предоставляемые каталогом (поддерживаются AWS S3 и Azure ADLS Gen2)                                                                                                                                                                                                                                                                  |
| `aws_access_key_id`     | Идентификатор ключа доступа AWS для доступа к S3/Glue (если не используются учетные данные, предоставляемые каталогом)                                                                                                                                                                                                                                                                             |
| `aws_secret_access_key` | Секретный ключ доступа AWS для доступа к S3/Glue (если не используются учетные данные, предоставляемые каталогом)                                                                                                                                                                                                                                                                                  |
| `region`                | Region AWS для сервиса (например, `us-east-1`)                                                                                                                                                                                                                                                                                                                                                     |
| `dlf_access_key_id`     | Идентификатор ключа доступа для доступа к DLF                                                                                                                                                                                                                                                                                                                                                      |
| `dlf_access_key_secret` | Секрет ключа доступа для доступа к DLF                                                                                                                                                                                                                                                                                                                                                             |
| `force_add_bucket`      | При формировании URL объектного хранилища на основе расположения таблицы, предоставленного каталогом, и `storage_endpoint` добавляет в начало имя бакета/контейнера, даже если оно уже содержится в конечной точке. Значение по умолчанию: `false`. Установите `true` для каталогов, которые возвращают пути без бакета и требуют его добавления на этапе формирования URL (пути в стиле Polaris). |

<div id="examples">
  ## Примеры
</div>

Ниже приведены разделы с примерами использования движка `DataLakeCatalog`:

* [Unity Catalog](/ru/use-cases/data-lake/unity-catalog)
* [Glue Catalog](/ru/use-cases/data-lake/glue-catalog)
* OneLake Catalog
  Можно использовать при включении `allow_experimental_database_iceberg` или `allow_database_iceberg`.

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