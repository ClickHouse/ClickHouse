---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 41
toc_title: URL
---

# URL {#url}

`url(URL, format, structure)` - retourne une table créée à partir du `URL` avec le
`format` et `structure`.

URL-adresse du serveur HTTP ou HTTPS, qui peut accepter `GET` et/ou `POST` demande.

format - [format](../../interfaces/formats.md#formats) des données.

structure - structure de table dans `'UserID UInt64, Name String'` format. Détermine les noms et les types de colonnes.

**Exemple**

``` sql
-- getting the first 3 lines of a table that contains columns of String and UInt32 type from HTTP-server which answers in CSV format.
SELECT * FROM url('http://127.0.0.1:12345/', CSV, 'column1 String, column2 UInt32') LIMIT 3
```

[Article Original](https://clickhouse.tech/docs/en/query_language/table_functions/url/) <!--hide-->
