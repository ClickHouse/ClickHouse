---
toc_priority: 37
toc_title: file
---

# file {#file}

Создаёт таблицу из файла. Данная табличная функция похожа на табличные функции [url](../../sql-reference/table-functions/url.md) и [hdfs](../../sql-reference/table-functions/hdfs.md).

Функция `file` может использоваться в запросах `SELECT` и `INSERT` при работе с движком таблиц [File](../../engines/table-engines/special/file.md).

**Синтаксис**

``` sql
file(path, format, structure)
```

**Параметры**

-   `path` — относительный путь до файла от [user_files_path](../../sql-reference/table-functions/file.md#server_configuration_parameters-user_files_path). Путь к файлу поддерживает следующие шаблоны в режиме доступа только для чтения `*`, `?`, `{abc,def}` и `{N..M}`, где `N`, `M` — числа, `'abc', 'def'` — строки.
-   `format` — [формат](../../interfaces/formats.md#formats) файла.
-   `structure` — структура таблицы. Формат: `'colunmn1_name column1_ype, column2_name column2_type, ...'`.

**Возвращаемое значение**

Таблица с указанной структурой, предназначенная для чтения или записи данных в указанном файле.

**Примеры**

Настройка `user_files_path` и содержимое файла `test.csv`:

``` bash
$ grep user_files_path /etc/clickhouse-server/config.xml
    <user_files_path>/var/lib/clickhouse/user_files/</user_files_path>

$ cat /var/lib/clickhouse/user_files/test.csv
    1,2,3
    3,2,1
    78,43,45
```

Получение данных из таблицы в файле `test.csv` и выборка первых двух строк из неё:

``` sql
SELECT * FROM file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32') LIMIT 2;
```

``` text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

Получение первых 10 строк таблицы, содержащей 3 столбца типа [UInt32](../../sql-reference/data-types/int-uint.md), из CSV-файла:

``` sql
SELECT * FROM file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32') LIMIT 10;
```

Вставка данных из файла в таблицу:

``` sql
INSERT INTO FUNCTION file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32') VALUES (1, 2, 3), (3, 2, 1);
SELECT * FROM file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32');
```

``` text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

## Шаблоны поиска в компонентах пути {#globs-in-path}

При описании пути к файлу могут использоваться шаблоны поиска. Обрабатываются только те файлы, у которых путь и название соответствуют шаблону полностью (а не только префикс или суффикс).

-   `*` — заменяет любое количество любых символов кроме `/`, включая отсутствие символов.
-   `?` — заменяет ровно один любой символ.
-   `{some_string,another_string,yet_another_one}` — заменяет любую из строк `'some_string', 'another_string', 'yet_another_one'`.
-   `{N..M}` — заменяет любое число в интервале от `N` до `M` включительно (может содержать ведущие нули).

Конструкция с `{}` аналогична табличной функции [remote](remote.md).

**Пример**

Предположим, у нас есть несколько файлов со следующими относительными путями:

-   'some_dir/some_file_1'
-   'some_dir/some_file_2'
-   'some_dir/some_file_3'
-   'another_dir/some_file_1'
-   'another_dir/some_file_2'
-   'another_dir/some_file_3'

Запросим количество строк в этих файлах:

``` sql
SELECT count(*) FROM file('{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32');
```

Запросим количество строк во всех файлах этих двух директорий:

``` sql
SELECT count(*) FROM file('{some,another}_dir/*', 'TSV', 'name String, value UInt32');
```

!!! warning "Предупреждение"
    Если ваш список файлов содержит интервал с ведущими нулями, используйте конструкцию с фигурными скобками для каждой цифры по отдельности или используйте `?`.

**Пример**

Запрос данных из файлов с именами `file000`, `file001`, … , `file999`:

``` sql
SELECT count(*) FROM file('big_dir/file{0..9}{0..9}{0..9}', 'CSV', 'name String, value UInt32');
```

## Виртуальные столбцы {#virtualnye-stolbtsy}

-   `_path` — путь к файлу.
-   `_file` — имя файла.

**Смотрите также**

-   [Виртуальные столбцы](index.md#table_engines-virtual_columns)

