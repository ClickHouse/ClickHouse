---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 41
toc_title: URL
---

# URL {#url}

`url(URL, format, structure)` - devuelve una tabla creada a partir del `URL` con dado
`format` y `structure`.

URL - Dirección de servidor HTTP o HTTPS, que puede aceptar `GET` y/o `POST` peticiones.

formato - [formato](../../interfaces/formats.md#formats) de los datos.

estructura - estructura de la tabla en `'UserID UInt64, Name String'` formato. Determina los nombres y tipos de columna.

**Ejemplo**

``` sql
-- getting the first 3 lines of a table that contains columns of String and UInt32 type from HTTP-server which answers in CSV format.
SELECT * FROM url('http://127.0.0.1:12345/', CSV, 'column1 String, column2 UInt32') LIMIT 3
```

[Artículo Original](https://clickhouse.tech/docs/en/query_language/table_functions/url/) <!--hide-->
