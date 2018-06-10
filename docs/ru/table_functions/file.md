<a name="table_functions-file"></a>

# file

`file(path, format, structure)` - возвращает таблицу со столбцами, указанными в structure, созданную из файла path типа format.

path - относительный путь до файла от [user_files_path](../operations/server_settings/settings.md#user_files_path).

format - [формат](../formats/index.md) файла.

structure - структура таблицы в форме 'UserID UInt64, URL String'. Определяет имена и типы столбцов.

**Пример**

```sql
-- получение первых 10 строк таблицы, состоящей из трёх колонок типа UInt32 из CSV файла
SELECT * FROM file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32') LIMIT 10
```
