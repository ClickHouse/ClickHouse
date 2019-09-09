
# file

Создаёт таблицу из файла.

```
file(path, format, structure)
```

**Входные параметры**

- `path` — относительный путь до файла от [user_files_path](../../operations/server_settings/settings.md#server_settings-user_files_path). Путь к файлу поддерживает следующие шаблоны в режиме доступа только для чтения `*`, `?`, `{abc,def}` и `{N..M}`, где `N`, `M` — числа, ``'abc', 'def'` — строки.
- `format` — [формат](../../interfaces/formats.md#formats) файла.
- `structure` — структура таблицы. Формат `'colunmn1_name column1_ype, column2_name column2_type, ...'`.

**Возвращаемое значение**

Таблица с указанной структурой, предназначенная для чтения или записи данных в указанном файле.

**Пример**

Настройка `user_files_path` и содержимое файла `test.csv`:

```bash
$ grep user_files_path /etc/clickhouse-server/config.xml
    <user_files_path>/var/lib/clickhouse/user_files/</user_files_path>

$ cat /var/lib/clickhouse/user_files/test.csv
    1,2,3
    3,2,1
    78,43,45
```

Таблица из `test.csv` и выборка первых двух строк из неё:

``` sql
SELECT *
FROM file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
LIMIT 2
```
```
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

**Шаблоны в пути файла**

- `*` — Матчит любое количество любых символов, включая отсутствие символов.
- `?` — Матчит ровно один любой символ.
- `{some_string,another_string,yet_another_one}` — Матчит любую из строк `'some_string', 'another_string', 'yet_another_one'`.
- `{N..M}` — Матчит любое число в интервале от `N` до `M` включительно.

!!! warning
    Если ваш список файлов содержит интервал с ведущими нулями, используйте конструкцию с фигурными скобками для каждой цифры по отдельности или используйте `?`.

Шаблоны могут содержаться в разных частях пути. Обрабатываться будут ровно те файлы, которые и удовлетворяют всему шаблону пути, и существуют в файловой системе.

[Оригинальная статья](https://clickhouse.yandex/docs/ru/query_language/table_functions/file/) <!--hide-->
