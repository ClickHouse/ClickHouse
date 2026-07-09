---
description: 'Предоставляет табличный интерфейс только для чтения для таблиц Apache Hudi в Amazon
  S3.'
sidebar_label: 'hudi'
sidebar_position: 85
slug: /sql-reference/table-functions/hudi
title: 'hudi'
doc_type: 'reference'
---

Предоставляет табличный интерфейс только для чтения для таблиц Apache [Hudi](https://hudi.apache.org/) в Amazon S3.

<div id="syntax">
  ## Синтаксис
</div>

```sql
hudi(url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])
```

<div id="arguments">
  ## Аргументы
</div>

| Аргумент                                     | Описание                                                                                                                                                                                                                                                                                                                                                                                                               |
| -------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `url`                                        | URL бакета с путем к существующей таблице Hudi в S3.                                                                                                                                                                                                                                                                                                                                                                   |
| `aws_access_key_id`, `aws_secret_access_key` | Долгосрочные учетные данные для пользователя аккаунта [AWS](https://aws.amazon.com/). Их можно использовать для аутентификации запросов. Эти параметры необязательны. Если учетные данные не указаны, используются значения из конфигурации ClickHouse. Дополнительные сведения см. в разделе [Использование S3 для хранения данных](/ru/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-s3). |
| `format`                                     | [Формат](/ru/interfaces/formats) файла.                                                                                                                                                                                                                                                                                                                                                                                   |
| `structure`                                  | Структура таблицы. Формат: `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                                                                                                                                              |
| `compression`                                | Параметр необязателен. Поддерживаемые значения: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. По умолчанию сжатие определяется автоматически по расширению файла.                                                                                                                                                                                                                                             |
| `extra_credentials`                          | Параметр необязателен. Используется для передачи `role_arn` для ролевого доступа в ClickHouse Cloud. Инструкции по настройке см. в разделе [Защищенный S3](/ru/cloud/data-sources/secure-s3).                                                                                                                                                                                                                             |

<div id="returned_value">
  ## Возвращаемое значение
</div>

Таблица с указанной структурой для чтения данных из указанной таблицы Hudi в S3.

<div id="virtual-columns">
  ## Виртуальные столбцы
</div>

* `_path` — Путь к файлу. Тип: `LowCardinality(String)`.
* `_file` — Имя файла. Тип: `LowCardinality(String)`.
* `_size` — Размер файла в байтах. Тип: `Nullable(UInt64)`. Если размер файла неизвестен, значение — `NULL`.
* `_time` — Время последнего изменения файла. Тип: `Nullable(DateTime)`. Если время неизвестно, значение — `NULL`.
* `_etag` — ETag файла. Тип: `LowCardinality(String)`. Если ETag неизвестен, значение — `NULL`.

<div id="related">
  ## См. также
</div>

* [движок Hudi](/ru/engines/table-engines/integrations/hudi.md)
* [кластерная табличная функция Hudi](/ru/sql-reference/table-functions/hudiCluster.md)