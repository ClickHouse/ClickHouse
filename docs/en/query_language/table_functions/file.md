
# file

Creates a table from a file.

```
file(path, format, structure)
```

**Input parameters**

- `path` — The relative path to the file from [user_files_path](../../operations/server_settings/settings.md#user_files_path).
- `format` —  The [format](../../interfaces/formats.md#formats) of the file.
- `structure` — Structure of the table. Format `'colunmn1_name column1_ype, column2_name column2_type, ...'`.

**Returned value**

A table with the specified structure for reading or writing data in the specified file.

**Example**

Setting `user_files_path` and the contents of the file `test.csv`:

```bash
$ grep user_files_path /etc/clickhouse-server/config.xml
    <user_files_path>/var/lib/clickhouse/user_files/</user_files_path>

$ cat /var/lib/clickhouse/user_files/test.csv
    1,2,3
    3,2,1
    78,43,45
```

Table from`test.csv` and selection of the first two rows from it:

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

``` sql
-- getting the first 10 lines of a table that contains 3 columns of UInt32 type from a CSV file
SELECT * FROM file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32') LIMIT 10
```

[Original article](https://clickhouse.yandex/docs/en/query_language/table_functions/file/) <!--hide-->
