# file

`file(path, format, structure)` - возвращает таблицу, созданную из файла типа format, вида structure.

--structure - структура таблицы, в форме UserID UInt64, URL String. Определяет имена и типы столбцов.
Файлы, указанные в file, будут разобраны форматом, указанным в format, с использованием типов данных, указанных в types или structure. Таблица будет загружена на сервер, и доступна там в качестве временной таблицы с именем name.

Пример:
```sql
-- получение таблицы из трёх колонок типа UInt32
SELECT * FROM file('test.csv', 'CSV', 'colomn1 UInt32, colomn2 UInt32, colomn3 UInt32') LIMIT 10
```
