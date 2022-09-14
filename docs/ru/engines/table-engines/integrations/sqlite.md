---
toc_priority: 7
toc_title: SQLite
---

# SQLite {#sqlite}

Движок позволяет импортировать и экспортировать данные из SQLite, а также поддерживает отправку запросов к таблицам SQLite напрямую из ClickHouse.

## Создание таблицы {#creating-a-table}

``` sql
    CREATE TABLE [IF NOT EXISTS] [db.]table_name 
    (
        name1 [type1], 
        name2 [type2], ...
    ) ENGINE = SQLite('db_path', 'table')
```

**Параметры движка**

-   `db_path` — путь к файлу с базой данных SQLite.
-   `table` — имя таблицы в базе данных SQLite.

## Примеры использования {#usage-example}

Отобразим запрос, с помощью которого была создана таблица SQLite:

```sql
SHOW CREATE TABLE sqlite_db.table2;
```

``` text
CREATE TABLE SQLite.table2
( 
    `col1` Nullable(Int32), 
    `col2` Nullable(String)
) 
ENGINE = SQLite('sqlite.db','table2');
```

Получим данные из таблицы:

``` sql
SELECT * FROM sqlite_db.table2 ORDER BY col1;
```

```text
┌─col1─┬─col2──┐
│    1 │ text1 │
│    2 │ text2 │
│    3 │ text3 │
└──────┴───────┘
```

**См. также**

-   [SQLite](../../../engines/database-engines/sqlite.md) движок баз данных
-   [sqlite](../../../sql-reference/table-functions/sqlite.md) табличная функция
