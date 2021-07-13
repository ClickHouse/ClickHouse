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

**Patterns in URL**

Patterns in curly brackets `{ }` are used to generate a set of shards or specify failover addresses. Multiple patterns are allowed in a single URL. The following pattern types are supported.

- {*a*,*b*} - Any number of variants separated by a comma. The pattern is replaced with *a* in the first shard address and it is replaced with *b* in the second shard address and so on. For instance, `example0{1,2}-1` generates `example01-1` and `example02-1`.
- {*n*..*m*} - A range of numbers. This pattern generates shard addresses with incrementing indices from *n* to *m*. `example0{1..2}-1` generates `example01-1` and `example02-1`.
- {*0n*..*0m*} - A range of numbers with leading zeroes. This modification preserves leading zeroes in indices. The pattern `example{01..03}-1` generates `example01-1`, `example02-1` and `example03-1`.
- {*a*|*b*} - Any number of variants separated by a `|`. The pattern specifies a failover: the URL address with *b* is used if the address with *a* is not available. The pattern `example01-{1|2}` generates `example01-1`. If it is not available then `example01-2` is used.

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