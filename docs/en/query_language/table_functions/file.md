
# file

Creates a table from a file.

```
file(path, format, structure)
```

**Input parameters**

- `path` — The relative path to the file from [user_files_path](../../operations/server_settings/settings.md#server_settings-user_files_path). Path to file support following globs in readonly mode: `*`, `?`, `{abc,def}` and `{N..M}` where `N`, `M` — numbers, ``'abc', 'def'` — strings.
- `format` —  The [format](../../interfaces/formats.md#formats) of the file.
- `structure` — Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`.

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

**Globs in path**

- `*` — Matches any number of any characters including none.
- `?` — Matches any single character.
- `{some_string,another_string,yet_another_one}` — Matches any of strings `'some_string', 'another_string', 'yet_another_one'`.
- `{N..M}` — Matches any number in range from N to M including both borders.

!!! warning
    If your listing of files contains number ranges with leading zeros, use the construction with braces for each digit separately or use `?`.

Multiple path components can have globs. For being processed file should exists and matches to the whole path pattern.

[Original article](https://clickhouse.yandex/docs/en/query_language/table_functions/file/) <!--hide-->
