---
description: 'Документация по движку таблицы TinyLog'
slug: /engines/table-engines/log-family/tinylog
toc_priority: 34
toc_title: 'TinyLog'
title: 'Движок таблицы TinyLog'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="tinylog-table-engine">
  # Движок таблицы TinyLog
</div>

<CloudNotSupportedBadge />

Этот движок относится к семейству движков Log. Об общих свойствах движков семейства Log и различиях между ними см. в разделе [Семейство движков Log](../../../engines/table-engines/log-family/index.md).

Этот движок таблицы обычно используется по принципу однократной записи: данные записываются один раз, а затем читаются столько раз, сколько необходимо. Например, таблицы типа `TinyLog` можно использовать для промежуточных данных, обрабатываемых небольшими батчами. Обратите внимание, что хранение данных в большом количестве маленьких таблиц неэффективно.

Запросы выполняются в одном потоке. Иными словами, этот движок предназначен для относительно небольших таблиц (примерно до 1 000 000 строк). Этот движок таблицы имеет смысл использовать, если у вас много маленьких таблиц, поскольку он проще, чем движок [Log](../../../engines/table-engines/log-family/log.md) (требуется открывать меньше файлов).

<div id="characteristics">
  ## Характеристики
</div>

* **Более простая структура**: В отличие от движка Log, TinyLog не использует файлы mark. Это уменьшает сложность, но также ограничивает возможности оптимизации производительности для больших наборов данных.
* **Однопоточные запросы**: Запросы к таблицам TinyLog выполняются в одном потоке, что делает этот движок подходящим для сравнительно небольших таблиц, обычно до 1 000 000 строк.
* **Эффективен для небольших таблиц**: Простота движка TinyLog делает его удобным для работы с большим количеством небольших таблиц, поскольку по сравнению с движком Log он требует меньше файловых операций.

В отличие от движка Log, TinyLog не использует файлы mark. Это уменьшает сложность, но также ограничивает возможности оптимизации производительности для более крупных наборов данных.

<div id="table_engines-tinylog-creating-a-table">
  ## Создание таблицы
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    column1_name [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    column2_name [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = TinyLog
```

См. подробное описание запроса [CREATE TABLE](/ru/sql-reference/statements/create/table).

<div id="table_engines-tinylog-writing-the-data">
  ## Запись данных
</div>

Движок `TinyLog` хранит все столбцы в одном файле. Для каждого запроса `INSERT` ClickHouse дописывает блок данных в конец файла таблицы, записывая столбцы по одному.

Для каждой таблицы ClickHouse записывает следующие файлы:

* `<column>.bin`: файл данных для каждого столбца, содержащий сериализованные и сжатые данные.

Движок `TinyLog` не поддерживает операции `ALTER UPDATE` и `ALTER DELETE`.

<div id="table_engines-tinylog-example-of-use">
  ## Пример использования
</div>

Создание таблицы:

```sql
CREATE TABLE tiny_log_table
(
    timestamp DateTime,
    message_type String,
    message String
)
ENGINE = TinyLog
```

Вставка данных:

```sql
INSERT INTO tiny_log_table VALUES (now(),'REGULAR','The first regular message')
INSERT INTO tiny_log_table VALUES (now(),'REGULAR','The second regular message'),(now(),'WARNING','The first warning message')
```

Мы использовали два запроса `INSERT`, чтобы создать два блока данных в файлах `<column>.bin`.

ClickHouse использует один поток для чтения данных. В результате порядок блоков строк в выходных данных соответствует порядку тех же блоков во входных данных. Например:

```sql
SELECT * FROM tiny_log_table
```

```text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2024-12-10 13:11:58 │ REGULAR      │ The first regular message  │
│ 2024-12-10 13:12:12 │ REGULAR      │ The second regular message │
│ 2024-12-10 13:12:12 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
```