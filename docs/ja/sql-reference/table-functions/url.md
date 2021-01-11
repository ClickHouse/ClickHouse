---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 41
toc_title: url
---

# url {#url}

`url(URL, format, structure)` -から作成されたテーブルを返します `URL` 与えられたと
`format` と `structure`.

URL-HTTPまたはHTTPSサーバーアドレス。 `GET` および/または `POST` リクエスト

形式 - [形式](../../interfaces/formats.md#formats) データの。

structure-テーブルの構造 `'UserID UInt64, Name String'` 形式。 列名と型を決定します。

**例**

``` sql
-- getting the first 3 lines of a table that contains columns of String and UInt32 type from HTTP-server which answers in CSV format.
SELECT * FROM url('http://127.0.0.1:12345/', CSV, 'column1 String, column2 UInt32') LIMIT 3
```

[元の記事](https://clickhouse.tech/docs/en/query_language/table_functions/url/) <!--hide-->
