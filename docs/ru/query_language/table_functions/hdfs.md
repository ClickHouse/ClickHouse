
# hdfs

Создаёт таблицу из файла.

```
hdfs(URI, format, structure)
```

**Входные параметры**

- `URI` — URI до файла в HDFS.
- `format` — [формат](../../interfaces/formats.md#formats) файла.
- `structure` — структура таблицы. Формат `'column1_name column1_type, column2_name column2_type, ...'`.

**Возвращаемое значение**

Таблица с указанной структурой, предназначенная для чтения или записи данных в указанном файле.

**Пример**

Таблица из `hdfs://hdfs1:9000/test` и выборка первых двух строк из неё:

``` sql
SELECT *
FROM hdfs('hdfs://hdfs1:9000/test', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
LIMIT 2
```
```
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

[Оригинальная статья](https://clickhouse.yandex/docs/ru/query_language/table_functions/hdfs/) <!--hide-->
