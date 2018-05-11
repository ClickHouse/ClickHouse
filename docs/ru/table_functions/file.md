<a name="table_functions-file"></a>

# file

`file(path, format, structure)` - создаёт таблицу из файла типа format, вида structure.

path - относительный путь до файла от [user_files_path](../operations/server_settings/settings.md#table_functions-file).

format - [формат](../formats#formats) файла.

structure - структура таблицы в форме UserID UInt64, URL String. Определяет имена и типы столбцов.

**Пример**

```sql
-- получение строк таблицы из файла, состоящей из трёх колонок типа UInt32
SELECT * FROM file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32') LIMIT 10
```
