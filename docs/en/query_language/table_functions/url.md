<a name="table_functions-url"></a>

# url

`url(URL, format, structure)` — Returns a table with the columns specified in
`structure`, created from data located at `URL` in the specified `format`.

URL — URL where the server accepts `GET` and/or `POST` requests
over HTTP or HTTPS.

format — The data [format](../../interfaces/formats.md#formats).

structure — The structure of the table in the format `'UserID UInt64, Name String'`. Defines the column names and types.

**Example**

```sql
-- Get 3 rows of a table consisting of two columns of type String and UInt32 from the server, which returns the data in CSV format
SELECT * FROM url('http://127.0.0.1:12345/', CSV, 'column1 String, column2 UInt32') LIMIT 3
```

