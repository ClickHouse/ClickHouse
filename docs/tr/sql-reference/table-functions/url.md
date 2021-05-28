---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 41
toc_title: url
---

# url {#url}

`url(URL, format, structure)` - oluşturulan bir tablo döndürür `URL` verilen ile
`format` ve `structure`.

Kabul edebilen URL - HTTP veya HTTPS sunucu adresi `GET` ve / veya `POST` istemler.

biçimli - [biçimli](../../interfaces/formats.md#formats) verilerin.

yapı-tablo yapısı `'UserID UInt64, Name String'` biçimli. Sütun adlarını ve türlerini belirler.

**Örnek**

``` sql
-- getting the first 3 lines of a table that contains columns of String and UInt32 type from HTTP-server which answers in CSV format.
SELECT * FROM url('http://127.0.0.1:12345/', CSV, 'column1 String, column2 UInt32') LIMIT 3
```

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/table_functions/url/) <!--hide-->
