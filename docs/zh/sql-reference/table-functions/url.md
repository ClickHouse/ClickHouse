---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 41
toc_title: url
---

# url {#url}

`url(URL, format, structure)` -返回从创建的表 `URL` 与给定
`format` 和 `structure`.

URL-HTTP或HTTPS服务器地址，它可以接受 `GET` 和/或 `POST` 请求。

格式 - [格式](../../interfaces/formats.md#formats) 的数据。

结构-表结构 `'UserID UInt64, Name String'` 格式。 确定列名称和类型。

**示例**

``` sql
-- getting the first 3 lines of a table that contains columns of String and UInt32 type from HTTP-server which answers in CSV format.
SELECT * FROM url('http://127.0.0.1:12345/', CSV, 'column1 String, column2 UInt32') LIMIT 3
```

[原始文章](https://clickhouse.tech/docs/en/query_language/table_functions/url/) <!--hide-->
