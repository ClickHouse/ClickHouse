<a name="table_functions-file"></a>

# file

`file(path, format, structure)` - returns a table created from a path file with a format type, with columns specified in structure.

path - a relative path to a file from [user_files_path](../../operations/server_settings/settings.md#user_files_path).

format - file [format](../../interfaces/formats.md#formats).

structure -  table structure in 'UserID UInt64, URL String' format. Determines column names and types.

**Example**

```sql
-- getting the first 10 lines of a table that contains 3 columns of UInt32 type from a CSV file
SELECT * FROM file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32') LIMIT 10
```
