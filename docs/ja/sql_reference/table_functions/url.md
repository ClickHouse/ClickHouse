---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 41
toc_title: url
---

# url {#url}

`url(URL, format, structure)` -から作成されたテーブルを返します `URL` 与えられると
`format` と `structure`.

URL-受け入れることができるHTTPまたはHTTPSサーバアドレス `GET` および/または `POST` 要求。

書式 - [書式](../../interfaces/formats.md#formats) データの。

構造-テーブルの構造 `'UserID UInt64, Name String'` フォーマット。 列の名前と型を決定します。

**例えば**

``` sql
-- getting the first 3 lines of a table that contains columns of String and UInt32 type from HTTP-server which answers in CSV format.
SELECT * FROM url('http://127.0.0.1:12345/', CSV, 'column1 String, column2 UInt32') LIMIT 3
```

[元の記事](https://clickhouse.tech/docs/en/query_language/table_functions/url/) <!--hide-->
