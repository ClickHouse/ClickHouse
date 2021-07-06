---
toc_priority: 41
toc_title: url
---

# url {#url}

`url` function creates a table from the `URL` with given `format` and `structure`.

`url` function may be used in `SELECT` and `INSERT` queries on data in [URL](../../engines/table-engines/special/url.md) tables.

**Syntax**

``` sql
url(URL, format, structure)
```

**Parameters**

- `URL` — HTTP or HTTPS server address, which can accept `GET` or `POST` requests (for `SELECT` or `INSERT` queries correspondingly). Type: [String](../../sql-reference/data-types/string.md).
- `format` — [Format](../../interfaces/formats.md#formats) of the data. Type: [String](../../sql-reference/data-types/string.md).
- `structure` — Table structure in `'UserID UInt64, Name String'` format. Determines column names and types. Type: [String](../../sql-reference/data-types/string.md).

**Returned value**

A table with the specified format and structure and with data from the defined `URL`.

**Globs in URL**

Curly brackets in a URL are used to generate a set of shards or specify a failover addresses. 

- {*a*,*b*} - any number of variants separated by a comma. This pattern generates address with *a* and address with *b*.
- {*n*..*m*} - a range of numbers. It generates addresses with an increasing indices from *n* to *m*.
- {*0n*..*m*} - a range of numbers with leading zeroes. It preserves leading zeroes in indices.
- {*a*|*b*} - any number of variants separated by a `|`. Specify a failover: variant *b* is used if address with *a* is not available.

Several curly brackets may be used inside a single URL

**Examples**

Getting the first 3 lines of a table that contains columns of `String` and [UInt32](../../sql-reference/data-types/int-uint.md) type from HTTP-server which answers in [CSV](../../interfaces/formats.md#csv) format.

``` sql
SELECT * FROM url('http://127.0.0.1:12345/', CSV, 'column1 String, column2 UInt32') LIMIT 3;
```

Inserting data from a `URL` into a table:

``` sql
CREATE TABLE test_table (column1 String, column2 UInt32) ENGINE=Memory;
INSERT INTO FUNCTION url('http://127.0.0.1:8123/?query=INSERT+INTO+test_table+FORMAT+CSV', 'CSV', 'column1 String, column2 UInt32') VALUES ('http interface', 42);
SELECT * FROM test_table;
```

This query reads `table-08.csv`, `table-09.csv`, `table-10.csv` from `replica0.local`. If it fails it tries `replica1.local`. `replica2.local` is the last failover option.

``` sql
SELECT * FROM url('https://replica{0|1|2}.local/table-{08..10}.csv', 'CSV', 'column1 String, column2 Uint32') 3;
```


[Original article](https://clickhouse.tech/docs/en/sql-reference/table-functions/url/) <!--hide-->
