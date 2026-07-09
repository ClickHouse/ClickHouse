---
description: 'Документация по движку Log'
slug: /engines/table-engines/log-family/log
toc_priority: 33
toc_title: 'Log'
title: 'Движок таблицы Log'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="log-table-engine">
  # Движок таблицы Log
</div>

<CloudNotSupportedBadge />

Этот движок таблицы относится к семейству движков `Log`. Общие свойства движков `Log` и различия между ними описаны в статье [Log Engine Family](../../../engines/table-engines/log-family/index.md).

`Log` отличается от [TinyLog](../../../engines/table-engines/log-family/tinylog.md) тем, что рядом с файлами столбцов хранится небольшой файл с «метками». Эти метки записываются для каждого блока данных и содержат смещения, указывающие, с какого места начинать чтение файла, чтобы пропустить указанное количество строк. Это позволяет читать данные таблицы в несколько потоков.
При одновременном доступе к данным операции чтения могут выполняться параллельно, тогда как операции записи блокируют и чтение, и друг друга.
Движок `Log` не поддерживает индексы. Кроме того, если запись в таблицу завершается сбоем, таблица становится повреждённой, и чтение из неё возвращает ошибку. Движок `Log` подходит для временных данных, таблиц с однократной записью, а также для тестирования или демонстрации.

<div id="table_engines-log-creating-a-table">
  ## Создание таблицы
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    column1_name [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    column2_name [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = Log
```

См. подробное описание запроса [CREATE TABLE](/ru/sql-reference/statements/create/table).

<div id="table_engines-log-writing-the-data">
  ## Запись данных
</div>

Движок `Log` эффективно хранит данные, записывая каждый столбец в отдельный файл. Для каждой таблицы движок `Log` записывает следующие файлы по указанному пути хранения:

* `<column>.bin`: файл данных для каждого столбца, содержащий сериализованные и сжатые данные.
  `__marks.mrk`: файл меток, в котором хранятся смещения и количество строк для каждого вставленного блока данных. Метки используются для более эффективного выполнения запросов, позволяя движку пропускать нерелевантные блоки данных при чтении.

<div id="writing-process">
  ### Процесс записи
</div>

Когда данные записываются в таблицу `Log`:

1. Данные сериализуются и сжимаются в блоки.
2. Для каждого столбца сжатые данные дописываются в соответствующий файл `<column>.bin`.
3. В файл `__marks.mrk` добавляются соответствующие записи, фиксирующие смещение и количество строк для вновь записанных данных.

<div id="table_engines-log-reading-the-data">
  ## Чтение данных
</div>

Файл с метками позволяет ClickHouse выполнять чтение данных параллельно. Это означает, что запрос `SELECT` возвращает строки в непредсказуемом порядке. Используйте предложение `ORDER BY` для сортировки строк.

<div id="table_engines-log-example-of-use">
  ## Пример использования
</div>

Создание таблицы:

```sql
CREATE TABLE log_table
(
    timestamp DateTime,
    message_type String,
    message String
)
ENGINE = Log
```

Вставка данных:

```sql
INSERT INTO log_table VALUES (now(),'REGULAR','The first regular message')
INSERT INTO log_table VALUES (now(),'REGULAR','The second regular message'),(now(),'WARNING','The first warning message')
```

Мы использовали два запроса `INSERT`, чтобы создать два блока данных в файлах `<column>.bin`.

ClickHouse использует несколько потоков при выборке данных. Каждый поток читает отдельный блок данных и по завершении независимо возвращает результирующие строки. В результате порядок блоков строк в выводе может не совпадать с порядком тех же блоков во входных данных. Например:

```sql
SELECT * FROM log_table
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
SELECT * FROM log_table ORDER BY timestamp
```

```text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2019-01-18 14:23:43 │ REGULAR      │ The first regular message  │
│ 2019-01-18 14:27:32 │ REGULAR      │ The second regular message │
│ 2019-01-18 14:34:53 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
```