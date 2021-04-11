---
toc_priority: 41
toc_title: CHECK
---

# CHECK TABLE Statement {#check-table}

Проверяет таблицу на повреждение данных.

``` sql
CHECK TABLE [db.]name
```

Запрос `CHECK TABLE` сравнивает текущие размеры файлов (в которых хранятся данные из колонок) с ожидаемыми значениями. Если значения не совпадают, данные в таблице считаются поврежденными. Искажение возможно, например, из-за сбоя при записи данных.

Ответ содержит колонку `result`, содержащую одну строку с типом [Boolean](../../sql-reference/data-types/boolean.md). Допустимые значения:

-   0 - данные в таблице повреждены;
-   1 - данные не повреждены.

Запрос `CHECK TABLE` поддерживает следующие движки таблиц:

-   [Log](../../engines/table-engines/log-family/log.md)
-   [TinyLog](../../engines/table-engines/log-family/tinylog.md)
-   [StripeLog](../../engines/table-engines/log-family/stripelog.md)
-   [Семейство MergeTree](../../engines/table-engines/mergetree-family/index.md)

При попытке выполнить запрос с таблицами с другими табличными движками, ClickHouse генерирует исключение.

В движках `*Log` не предусмотрено автоматическое восстановление данных после сбоя. Используйте запрос `CHECK TABLE`, чтобы своевременно выявлять повреждение данных.

## Проверка таблиц семейства MergeTree {#checking-mergetree-tables}

Для таблиц семейства `MergeTree` если [check_query_single_value_result](../../operations/settings/settings.md#check_query_single_value_result) = 0, запрос `CHECK TABLE` возвращает статус каждого куска данных таблицы на локальном сервере. 

```sql
SET check_query_single_value_result = 0;
CHECK TABLE test_table;
```

```text
┌─part_path─┬─is_passed─┬─message─┐
│ all_1_4_1 │         1 │         │
│ all_1_4_2 │         1 │         │
└───────────┴───────────┴─────────┘
```

Если `check_query_single_value_result` = 0, запрос `CHECK TABLE` возвращает статус таблицы в целом.

```sql
SET check_query_single_value_result = 1;
CHECK TABLE test_table;
```

```text
┌─result─┐
│      1 │
└────────┘
```

## Что делать, если данные повреждены {#if-data-is-corrupted}

В этом случае можно скопировать оставшиеся неповрежденные данные в другую таблицу. Для этого:

1.  Создайте новую таблицу с такой же структурой, как у поврежденной таблицы. Для этого выполните запрос `CREATE TABLE <new_table_name> AS <damaged_table_name>`.
2.  Установите значение параметра [max_threads](../../operations/settings/settings.md#settings-max_threads) в 1. Это нужно для того, чтобы выполнить следующий запрос в одном потоке. Установить значение параметра можно через запрос: `SET max_threads = 1`.
3.  Выполните запрос `INSERT INTO <new_table_name> SELECT * FROM <damaged_table_name>`. В результате неповрежденные данные будут скопированы в другую таблицу. Обратите внимание, будут скопированы только те данные, которые следуют до поврежденного участка.
4.  Перезапустите `clickhouse-client`, чтобы вернуть предыдущее значение параметра `max_threads`.


