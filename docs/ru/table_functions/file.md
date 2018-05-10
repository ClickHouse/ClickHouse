# file

`file(path, format, structure)` - загружает на сервер временную таблицу, созданную из файла типа format, вида structure.

path - путь из настройки пользовательской папки, которая устанавливается в [config.xml](../../../dbms/src/Server/config.xml).

format - файлы, указанные в file, будут разобраны форматом, указанным в [format](../formats).

structure - структура таблицы, в форме UserID UInt64, URL String. Определяет имена и типы столбцов.

Пример:
```sql
-- получение строк таблицы из файла, состоящей из трёх колонок типа UInt32
SELECT * FROM file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32') LIMIT 10
```
