---
description: 'Этот движок обеспечивает доступ только для чтения к существующим
  таблицам Delta Lake в Amazon S3.'
sidebar_label: 'DeltaLake'
sidebar_position: 40
slug: /engines/table-engines/integrations/deltalake
title: 'Движок таблицы DeltaLake'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="deltalake-table-engine">
  # Движок таблицы DeltaLake
</div>

Этот движок обеспечивает интеграцию с существующими таблицами [Delta Lake](https://github.com/delta-io/delta) в хранилищах S3, GCP и Azure и поддерживает как чтение, так и запись (начиная с v25.10).

<div id="create-table">
  ## Создание таблицы DeltaLake
</div>

Чтобы создать таблицу DeltaLake, она должна уже существовать в хранилище S3, GCP или Azure. Приведенные ниже команды не принимают DDL-параметры для создания новой таблицы.

<Tabs>
  <TabItem value="S3" label="S3" default>
    **Синтаксис**

    ```sql
    CREATE TABLE table_name
    ENGINE = DeltaLake(url, [aws_access_key_id, aws_secret_access_key,] [extra_credentials])
    ```

    **Параметры движка**

    * `url` — URL бакета с путем к существующей таблице Delta Lake.
    * `aws_access_key_id`, `aws_secret_access_key` - Долгосрочные учетные данные пользователя аккаунта [AWS](https://aws.amazon.com/). Их можно использовать для аутентификации запросов. Параметр необязателен. Если учетные данные не указаны, они берутся из файла конфигурации.
    * `extra_credentials` - Необязательный параметр. Используется для передачи `role_arn` при доступе на основе ролей в ClickHouse Cloud. Шаги настройки см. в разделе [Защищенный S3](/ru/cloud/data-sources/secure-s3).

    Параметры движка можно указать с помощью [именованных коллекций](/ru/operations/named-collections.md).

    **Пример**

    ```sql
    CREATE TABLE deltalake
    ENGINE = DeltaLake('http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/test_table/', 'ABC123', 'Abc+123')
    ```

    Использование именованных коллекций:

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
    **Синтаксис**

    ```sql
    -- Использование HTTPS URL (рекомендуется)
    CREATE TABLE table_name
    ENGINE = DeltaLake('https://storage.googleapis.com/<bucket>/<path>/', '<access_key_id>', '<secret_access_key>')
    ```

    :::note[URI gsutil не поддерживается]
    URI gsutil, например `gs://clickhouse-docs-example-bucket`, не поддерживается; используйте URL, начинающийся с `https://storage.googleapis.com`
    :::

    **Аргументы**

    * `url` — URL GCS-бакета для таблицы Delta Lake. Должен использовать формат `https://storage.googleapis.com/<bucket>/<path>/`
      (конечная точка GCS XML API) или `gs://<bucket>/<path>/`, который автоматически преобразуется.
    * `access_key_id` — ключ доступа GCS. Создается через Google Cloud Console → Cloud Storage → Settings → Interoperability.
    * `secret_access_key` — секретный ключ GCS.

    **Именованные коллекции**

    Вы также можете использовать именованные коллекции.
    Например:

    ```sql
    CREATE NAMED COLLECTION gcs_creds AS
    access_key_id = '<access_key>',
    secret_access_key = '<secret>';

    CREATE TABLE gcpDeltaLake
    ENGINE = DeltaLake(gcs_creds, url = 'https://storage.googleapis.com/<bucket>/<path>')
    ```
  </TabItem>

  <TabItem value="Azure" label="Azure" default>
    **Синтаксис**

    ```sql
    CREATE TABLE table_name
    ENGINE = DeltaLake(connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression])
    ```

    **Аргументы**

    * `connection_string` — строка подключения Azure
    * `storage_account_url` — URL аккаунта хранилища Azure (например, https://account.blob.core.windows.net)
    * `container_name` — имя контейнера Azure
    * `blobpath` — путь к таблице Delta Lake внутри контейнера
    * `account_name` — имя аккаунта хранилища Azure
    * `account_key` — ключ аккаунта хранилища Azure
  </TabItem>
</Tabs>

<div id="insert-data">
  ## Запись данных через таблицу DeltaLake
</div>

После создания таблицы на движке DeltaLake вы можете вставлять в неё данные с помощью:

```sql
SET allow_delta_lake_writes = 1;

INSERT INTO deltalake(id, firstname, lastname, gender, age)
VALUES (1, 'John', 'Smith', 'M', 32);
```

:::note
Запись с использованием движка таблицы поддерживается только через delta kernel.
Запись в Azure пока не поддерживается, но для S3 и GCS она работает.

Запись в Delta Lake — это функция в статусе бета, и её нужно включить с помощью `SET allow_delta_lake_writes = 1` (доступно начиная с версии 26.7; в более ранних версиях используйте `SET allow_experimental_delta_lake_writes = 1`).
:::

<div id="data-cache">
  ### Кэширование данных
</div>

Движок таблицы `DeltaLake` и табличная функция поддерживают кэширование данных так же, как и хранилища `S3`, `AzureBlobStorage` и `HDFS`. Подробнее см. в разделе [&quot;Движок таблицы S3&quot;](../../../engines/table-engines/integrations/s3.md#data-cache).

<div id="see-also">
  ## См. также
</div>

* [табличная функция DeltaLake](../../../sql-reference/table-functions/deltalake.md)