---
toc_priority: 41
toc_title: url
---

# url {#url}

`url(URL, format, structure)` - returns a table created from the `URL` with given
`format` and `structure`.

URL - HTTP or HTTPS server address, which can accept `GET` and/or `POST` requests.

format - [format](../../interfaces/formats.md#formats) of the data.

structure - table structure in `'UserID UInt64, Name String'` format. Determines column names and types.

**Example**

``` sql
-- getting the first 3 lines of a table that contains columns of String and UInt32 type from HTTP-server which answers in CSV format.
SELECT * FROM url('http://127.0.0.1:12345/', CSV, 'column1 String, column2 UInt32') LIMIT 3
```

[Original article](https://clickhouse.tech/docs/en/query_language/table_functions/url/) <!--hide-->
