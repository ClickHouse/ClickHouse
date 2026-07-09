---
description: 'Этот движок обеспечивает доступ только для чтения к существующим
  таблицам Apache Hudi в Amazon S3.'
sidebar_label: 'Hudi'
sidebar_position: 86
slug: /engines/table-engines/integrations/hudi
title: 'Движок таблицы Hudi'
doc_type: 'reference'
---

Этот движок обеспечивает доступ только для чтения к существующим таблицам Apache [Hudi](https://hudi.apache.org/) в Amazon S3.

<div id="create-table">
  ## Создание таблицы
</div>

Обратите внимание: таблица Hudi уже должна существовать в S3; эта команда не принимает DDL-параметры для создания новой таблицы.

```sql
CREATE TABLE hudi_table
    ENGINE = Hudi(url, [aws_access_key_id, aws_secret_access_key,] [extra_credentials])
```

**Параметры движка**

* `url` — URL бакета с путём к существующей таблице Hudi.
* `aws_access_key_id`, `aws_secret_access_key` - Долговременные учетные данные пользователя аккаунта [AWS](https://aws.amazon.com/). Их можно использовать для аутентификации запросов. Параметр необязателен. Если учетные данные не указаны, используются данные из файла конфигурации.
* `extra_credentials` - Необязательный параметр. Используется для передачи `role_arn` для доступа на основе ролей в ClickHouse Cloud. Инструкции по настройке см. в разделе [Secure S3](/ru/cloud/data-sources/secure-s3).

Параметры движка можно указать с помощью [именованных коллекций](/ru/operations/named-collections.md).

**Пример**

```sql
CREATE TABLE hudi_table ENGINE=Hudi('http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/test_table/', 'ABC123', 'Abc+123')
```

Использование именованных коллекций:

```xml
<clickhouse>
    <named_collections>
        <hudi_conf>
            <url>http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/</url>
            <access_key_id>ABC123</access_key_id>
            <secret_access_key>Abc+123</secret_access_key>
        </hudi_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE hudi_table ENGINE=Hudi(hudi_conf, filename = 'test_table')
```

<div id="see-also">
  ## См. также
</div>

* [табличная функция Hudi](/ru/sql-reference/table-functions/hudi.md)