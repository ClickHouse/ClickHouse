
# hdfs

Создаёт таблицу из файла в HDFS. Данная табличная функция похожа на табличную функцию [file](file.md).

```
hdfs(URI, format, structure)
```

**Входные параметры**

- `URI` — URI файла в HDFS. Путь к файлу поддерживает следующие шаблоны в режиме доступа только для чтения `*`, `?`, `{abc,def}` и `{N..M}`, где `N`, `M` — числа, ``'abc', 'def'` — строки.
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

**Шаблоны в пути файла**

Шаблоны могут содержаться в нескольких компонентах пути. Обрабатываются только существующие файлы, название которых целиком удовлетворяет шаблону (не только суффиксом или префиксом).

- `*` — Заменяет любое количество любых символов, включая отсутствие символов.
- `?` — Заменяет ровно один любой символ.
- `{some_string,another_string,yet_another_one}` — Заменяет любую из строк `'some_string', 'another_string', 'yet_another_one'`.
- `{N..M}` — Заменяет любое число в интервале от `N` до `M` включительно.

**Пример**

1. Предположим у нас есть несколько файлов со следующими URI в HDFS:

- 'hdfs://hdfs1:9000/some_dir/some_file_1'
- 'hdfs://hdfs1:9000/some_dir/some_file_2'
- 'hdfs://hdfs1:9000/some_dir/some_file_3'
- 'hdfs://hdfs1:9000/another_dir/some_file_1'
- 'hdfs://hdfs1:9000/another_dir/some_file_2'
- 'hdfs://hdfs1:9000/another_dir/some_file_3'

2. Запросим количество строк в этих файлах:

```sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32')
```

3. Запросим количество строк во всех файлах этих двух директорий:

```sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV', 'name String, value UInt32')
```

!!! warning
    Если ваш список файлов содержит интервал с ведущими нулями, используйте конструкцию с фигурными скобками для каждой цифры по отдельности или используйте `?`.

**Пример**

Запрос данных из файлов с именами `file000`, `file001`, ... , `file999`:

```sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/big_dir/file{0..9}{0..9}{0..9}', 'CSV', 'name String, value UInt32')
```

[Оригинальная статья](https://clickhouse.yandex/docs/ru/query_language/table_functions/hdfs/) <!--hide-->
