<a name="table_functions-file"></a>

# file

Создаёт таблицу из файла.

```
file(path, format, structure)
```

**Входные параметры**

- `path` — относительный путь до файла от [user_files_path](../../operations/server_settings/settings.md#user_files_path).
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

```sql
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
