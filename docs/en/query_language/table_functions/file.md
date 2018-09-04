<a name="table_functions-file"></a>

# file

`file(path, format, structure` — Returns a table with the columns specified in the structure created from the file at path in the specified format.

path — The relative path to the file from [user_files_path](../../operations/server_settings/settings.md#user_files_path).

format — The file data [format](../../interfaces/formats.md#formats).

structure — The structure of the table in the format 'UserID UInt64, URL String'. Defines the column names and types.

**Example**

```sql
-- Get the first 10 rows of a table consisting of three UInt32 columns from a CSV file.
SELECT * FROM file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32') LIMIT 10
```

