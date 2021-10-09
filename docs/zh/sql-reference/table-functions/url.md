---
toc_priority: 41
toc_title: url
---

# url {#url}

`url` 函数从 `URL` 创建一个具有给定 `format` 和 `structure` 的表。

`url` 函数可用于对[URL](../../engines/table-engines/special/url.md)表中的数据进行 `SELECT` 和 `INSERT` 的查询中。

**语法**

``` sql
url(URL, format, structure)
```

**参数**

- `URL` — HTTP或HTTPS服务器地址，它可以接受 `GET` 或 `POST` 请求 (对应于 `SELECT` 或 `INSERT` 查询)。类型: [String](../../sql-reference/data-types/string.md)。
- `format` — 数据[格式](../../interfaces/formats.md#formats)。类型: [String](../../sql-reference/data-types/string.md)。
- `structure` — 以 `'UserID UInt64, Name String'` 格式的表结构。确定列名和类型。 类型: [String](../../sql-reference/data-types/string.md)。

**返回值**

A table with the specified format and structure and with data from the defined `URL`.

**示例**

获取一个表的前3行，该表是从HTTP服务器获取的包含 `String` 和 [UInt32](../../sql-reference/data-types/int-uint.md) 类型的列，以[CSV](../../interfaces/formats.md#csv)格式返回。

``` sql
SELECT * FROM url('http://127.0.0.1:12345/', CSV, 'column1 String, column2 UInt32') LIMIT 3;
```

将 `URL` 的数据插入到表中:

``` sql
CREATE TABLE test_table (column1 String, column2 UInt32) ENGINE=Memory;
INSERT INTO FUNCTION url('http://127.0.0.1:8123/?query=INSERT+INTO+test_table+FORMAT+CSV', 'CSV', 'column1 String, column2 UInt32') VALUES ('http interface', 42);
SELECT * FROM test_table;
```
[原始文章](https://clickhouse.com/docs/en/query_language/table_functions/url/) <!--hide-->
