---
description: 'Документация по CHECK TABLE'
sidebar_label: 'CHECK TABLE'
sidebar_position: 41
slug: /sql-reference/statements/check-table
title: 'Оператор CHECK TABLE'
doc_type: 'reference'
---

Запрос `CHECK TABLE` в ClickHouse используется для проверки указанной таблицы или её партиций. Он обеспечивает целостность данных, проверяя контрольные суммы и другие внутренние структуры данных.

В частности, он сравнивает фактические размеры файлов с ожидаемыми значениями, хранящимися на сервере. Если размеры файлов не совпадают с сохранёнными значениями, это означает, что данные повреждены. Это может быть вызвано, например, сбоем системы во время выполнения запроса.

:::warning
Запрос `CHECK TABLE` может читать все данные таблицы и занимать часть ресурсов, поэтому он может быть ресурсоёмким.
Перед выполнением этого запроса оцените его возможное влияние на производительность и использование ресурсов.
Этот запрос не повышает производительность системы, поэтому не следует выполнять его, если вы не уверены в своих действиях.
:::

<div id="syntax">
  ## Синтаксис
</div>

Синтаксис запроса выглядит следующим образом:

```sql
CHECK TABLE table_name [PARTITION partition_expression | PART part_name] [FORMAT format] [SETTINGS check_query_single_value_result = (0|1) [, other_settings]]
```

* `table_name`: Указывает имя таблицы, которую нужно проверить.
* `partition_expression`: (Необязательно) Если нужно проверить конкретную партицию таблицы, это выражение позволяет указать нужную партицию.
* `part_name`: (Необязательно) Если нужно проверить конкретную часть данных в таблице, можно добавить строковый литерал с именем части.
* `FORMAT format`: (Необязательно) Позволяет указать формат вывода результата.
* `SETTINGS`: (Необязательно) Позволяет задать дополнительные настройки.
  * (Необязательно): [check&#95;query&#95;single&#95;value&#95;result](../../operations/settings/settings#check_query_single_value_result): Эта настройка определяет, будет ли вывод подробным (`0`) или сводным (`1`).
  * Также можно применять другие настройки. Если детерминированный порядок результатов не требуется, можно установить max&#95;threads в значение больше единицы, чтобы ускорить запрос.

Ответ на запрос зависит от значения настройки `check_query_single_value_result`.
Если `check_query_single_value_result = 1`, возвращается только столбец `result` с одной строкой. Значение в этой строке равно `1`, если проверка целостности пройдена, и `0`, если данные повреждены.

Если `check_query_single_value_result = 0`, запрос возвращает следующие столбцы:

* `part_path`: Указывает путь к части данных или имя файла.
  * `is_passed`: Возвращает 1, если проверка этой части прошла успешно, иначе 0.
  * `message`: Любые дополнительные сообщения, связанные с проверкой, например сообщения об ошибках или об успешном завершении.

Запрос `CHECK TABLE` поддерживает следующие движки таблиц:

* [Log](../../engines/table-engines/log-family/log.md)
* [TinyLog](../../engines/table-engines/log-family/tinylog.md)
* [StripeLog](../../engines/table-engines/log-family/stripelog.md)
* [семейство MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)

Выполнение запроса для таблиц с другими движками таблиц приводит к исключению `NOT_IMPLEMENTED`.

Движки из семейства `*Log` не обеспечивают автоматическое восстановление данных при сбоях. Используйте запрос `CHECK TABLE`, чтобы своевременно выявлять потерю данных.

<div id="examples">
  ## Примеры
</div>

По умолчанию запрос `CHECK TABLE` отображает общий статус проверки таблицы:

```sql title="Query"
CHECK TABLE test_table;
```

```text title="Response"
┌─result─┐
│      1 │
└────────┘
```

Если вы хотите увидеть статус проверки для каждой отдельной части данных, можно использовать настройку `check_query_single_value_result`.

Чтобы проверить конкретную партицию таблицы, можно также использовать ключевое слово `PARTITION`.

```sql title="Query"
CHECK TABLE t0 PARTITION ID '201003'
FORMAT PrettyCompactMonoBlock
SETTINGS check_query_single_value_result = 0
```

```text title="Response"
┌─part_path────┬─is_passed─┬─message─┐
│ 201003_7_7_0 │         1 │         │
│ 201003_3_3_0 │         1 │         │
└──────────────┴───────────┴─────────┘
```

Аналогичным образом можно проверить конкретную часть таблицы с помощью ключевого слова `PART`.

```sql title="Query"
CHECK TABLE t0 PART '201003_7_7_0'
FORMAT PrettyCompactMonoBlock
SETTINGS check_query_single_value_result = 0
```

```text title="Response"
┌─part_path────┬─is_passed─┬─message─┐
│ 201003_7_7_0 │         1 │         │
└──────────────┴───────────┴─────────┘
```

Обратите внимание: если часть отсутствует, запрос возвращает ошибку:

```sql title="Query"
CHECK TABLE t0 PART '201003_111_222_0'
```

```text title="Response"
DB::Exception: No such data part '201003_111_222_0' to check in table 'default.t0'. (NO_SUCH_DATA_PART)
```

<div id="receiving-a-corrupted-result">
  ### Получение результата &#39;Corrupted&#39;
</div>

:::warning
Внимание: описанная здесь процедура, включая ручное изменение или удаление файлов напрямую из каталога данных, предназначена только для экспериментальных сред или сред разработки. **Не** пытайтесь выполнять эти действия на продакшн-сервере, так как это может привести к потере данных и другим непредвиденным последствиям.
:::

Удалите существующий файл контрольной суммы:

```bash
rm /var/lib/clickhouse-server/data/default/t0/201003_3_3_0/checksums.txt
```

```sql title="Query"
CHECK TABLE t0 PARTITION ID '201003'
FORMAT PrettyCompactMonoBlock
SETTINGS check_query_single_value_result = 0
```

```text title="Response"
┌─part_path────┬─is_passed─┬─message──────────────────────────────────┐
│ 201003_7_7_0 │         1 │                                          │
│ 201003_3_3_0 │         1 │ Checksums recounted and written to disk. │
└──────────────┴───────────┴──────────────────────────────────────────┘
```

Если файл checksums.txt отсутствует, его можно восстановить. Он будет заново вычислен и перезаписан при выполнении команды CHECK TABLE для конкретной партиции, при этом статус по-прежнему будет отображаться как &#39;is&#95;passed = 1&#39;.

Вы можете сразу проверить все существующие таблицы `(Replicated)MergeTree`, используя запрос `CHECK ALL TABLES`.

```sql
CHECK ALL TABLES
FORMAT PrettyCompactMonoBlock
SETTINGS check_query_single_value_result = 0
```

```text
┌─database─┬─table────┬─part_path───┬─is_passed─┬─message─┐
│ default  │ t2       │ all_1_95_3  │         1 │         │
│ db1      │ table_01 │ all_39_39_0 │         1 │         │
│ default  │ t1       │ all_39_39_0 │         1 │         │
│ db1      │ t1       │ all_39_39_0 │         1 │         │
│ db1      │ table_01 │ all_1_6_1   │         1 │         │
│ default  │ t1       │ all_1_6_1   │         1 │         │
│ db1      │ t1       │ all_1_6_1   │         1 │         │
│ db1      │ table_01 │ all_7_38_2  │         1 │         │
│ db1      │ t1       │ all_7_38_2  │         1 │         │
│ default  │ t1       │ all_7_38_2  │         1 │         │
└──────────┴──────────┴─────────────┴───────────┴─────────┘
```

<div id="if-the-data-is-corrupted">
  ## Если данные повреждены
</div>

Если таблица повреждена, вы можете скопировать неповреждённые данные в другую таблицу. Для этого:

1. Создайте новую таблицу с той же структурой, что и повреждённая таблица. Для этого выполните запрос `CREATE TABLE <new_table_name> AS <damaged_table_name>`.
2. Установите значение `max_threads` равным 1, чтобы следующий запрос выполнялся в одном потоке. Для этого выполните запрос `SET max_threads = 1`.
3. Выполните запрос `INSERT INTO <new_table_name> SELECT * FROM <damaged_table_name>`. Этот запрос скопирует неповреждённые данные из повреждённой таблицы в другую. Будут скопированы только данные, расположенные до повреждённой части.
4. Перезапустите `клиент ClickHouse`, чтобы сбросить значение `max_threads`.