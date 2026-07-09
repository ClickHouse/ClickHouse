---
description: 'Документация по движку таблицы StripeLog'
slug: /engines/table-engines/log-family/stripelog
toc_priority: 32
toc_title: 'StripeLog'
title: 'Движок таблицы StripeLog'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="stripelog-table-engine">
  # Движок таблицы StripeLog
</div>

<CloudNotSupportedBadge />

Этот движок относится к семейству движков Log. Общие свойства движков семейства Log и различия между ними см. в статье [Семейство движков Log](../../../engines/table-engines/log-family/index.md).

Используйте этот движок, когда нужно хранить много таблиц с небольшим объёмом данных (менее 1 миллиона строк). Например, эту таблицу можно использовать для хранения входящих батчей данных для преобразования, если требуется их атомарная обработка. На сервере ClickHouse допустимо использовать 100 тыс. экземпляров таблиц этого типа. Этот движок таблицы предпочтительнее [Log](./log.md), когда требуется большое количество таблиц. Однако это достигается ценой снижения эффективности чтения.

<div id="table_engines-stripelog-creating-a-table">
  ## Создание таблицы
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    column1_name [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    column2_name [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = StripeLog
```

См. подробное описание запроса [CREATE TABLE](/ru/sql-reference/statements/create/table).

<div id="table_engines-stripelog-writing-the-data">
  ## Запись данных
</div>

Движок `StripeLog` хранит все столбцы в одном файле. Для каждого запроса `INSERT` ClickHouse дописывает блок данных в конец файла таблицы, записывая столбцы по одному.

Для каждой таблицы ClickHouse записывает следующие файлы:

* `data.bin` — файл данных.
* `index.mrk` — файл с метками. Метки содержат смещения для каждого столбца в каждом вставленном блоке данных.

Движок `StripeLog` не поддерживает операции `ALTER UPDATE` и `ALTER DELETE`.

<div id="table_engines-stripelog-reading-the-data">
  ## Чтение данных
</div>

Файл меток позволяет ClickHouse распараллелить чтение данных. Это означает, что запрос `SELECT` возвращает строки в непредсказуемом порядке. Используйте `ORDER BY`, чтобы отсортировать строки.

<div id="table_engines-stripelog-example-of-use">
  ## Пример использования
</div>

Создание таблицы:

```sql
CREATE TABLE stripe_log_table
(
    timestamp DateTime,
    message_type String,
    message String
)
ENGINE = StripeLog
```

Вставка данных:

```sql
INSERT INTO stripe_log_table VALUES (now(),'REGULAR','The first regular message')
INSERT INTO stripe_log_table VALUES (now(),'REGULAR','The second regular message'),(now(),'WARNING','The first warning message')
```

Мы использовали два запроса `INSERT`, чтобы создать два блока данных в файле `data.bin`.

ClickHouse использует несколько потоков при выборке данных. Каждый поток читает отдельный блок данных и возвращает результирующие строки независимо от других по мере завершения обработки. В результате порядок блоков строк в выводе в большинстве случаев не совпадает с порядком этих же блоков во входных данных. Например:

```sql
SELECT * FROM stripe_log_table
```

```text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2019-01-18 14:27:32 │ REGULAR      │ The second regular message │
│ 2019-01-18 14:34:53 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
┌───────────timestamp─┬─message_type─┬─message───────────────────┐
│ 2019-01-18 14:23:43 │ REGULAR      │ The first regular message │
└─────────────────────┴──────────────┴───────────────────────────┘
```

Сортировка результатов (по умолчанию — в порядке возрастания):

```sql
SELECT * FROM stripe_log_table ORDER BY timestamp
```

```text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2019-01-18 14:23:43 │ REGULAR      │ The first regular message  │
│ 2019-01-18 14:27:32 │ REGULAR      │ The second regular message │
│ 2019-01-18 14:34:53 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
```