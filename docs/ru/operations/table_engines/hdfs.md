# HDFS {#table_engines-hdfs}

Управляет данными в HDFS. Данный движок похож на движок [File](file.md) и на движок [URL](url.md).

## Использование движка

```
ENGINE = HDFS(URI, format)
```

В параметр `URI` нужно передавать полный URI файла в HDFS.
Параметр `format` должен быть таким, который ClickHouse может использовать и в запросах `INSERT`, и в запросах `SELECT`. Полный список поддерживаемых форматов смотрите в разделе [Форматы](../../interfaces/formats.md#formats).

**Пример:**

**1.** Создадим на сервере таблицу `hdfs_engine_table`:

``` sql
CREATE TABLE hdfs_engine_table (name String, value UInt32) ENGINE=HDFS('hdfs://hdfs1:9000/other_storage', 'TSV')
```

**2.** Заполним файл:
``` sql
INSERT INTO hdfs_engine_table VALUES ('one', 1), ('two', 2), ('three', 3)
```

**3.** Запросим данные:

``` sql
SELECT * FROM hdfs_engine_table LIMIT 2
```

```
┌─name─┬─value─┐
│ one  │     1 │
│ two  │     2 │
└──────┴───────┘
```

## Детали реализации

- Поддерживается многопоточное чтение и запись.
- Не поддерживается:
    - использование операций `ALTER` и `SELECT...SAMPLE`;
    - индексы;
    - репликация.

[Оригинальная статья](https://clickhouse.yandex/docs/ru/operations/table_engines/hdfs/) <!--hide-->
