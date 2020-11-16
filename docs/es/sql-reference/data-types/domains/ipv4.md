---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 59
toc_title: IPv4
---

## IPv4 {#ipv4}

`IPv4` es un dominio basado en `UInt32` tipo y sirve como un reemplazo con tipo para almacenar valores IPv4. Proporciona un almacenamiento compacto con el formato de entrada-salida amigable para los humanos y la información sobre el tipo de columna en la inspección.

### Uso básico {#basic-usage}

``` sql
CREATE TABLE hits (url String, from IPv4) ENGINE = MergeTree() ORDER BY url;

DESCRIBE TABLE hits;
```

``` text
┌─name─┬─type───┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┐
│ url  │ String │              │                    │         │                  │
│ from │ IPv4   │              │                    │         │                  │
└──────┴────────┴──────────────┴────────────────────┴─────────┴──────────────────┘
```

O puede usar el dominio IPv4 como clave:

``` sql
CREATE TABLE hits (url String, from IPv4) ENGINE = MergeTree() ORDER BY from;
```

`IPv4` domain admite formato de entrada personalizado como cadenas IPv4:

``` sql
INSERT INTO hits (url, from) VALUES ('https://wikipedia.org', '116.253.40.133')('https://clickhouse.tech', '183.247.232.58')('https://clickhouse.tech/docs/en/', '116.106.34.242');

SELECT * FROM hits;
```

``` text
┌─url────────────────────────────────┬───────────from─┐
│ https://clickhouse.tech/docs/en/ │ 116.106.34.242 │
│ https://wikipedia.org              │ 116.253.40.133 │
│ https://clickhouse.tech          │ 183.247.232.58 │
└────────────────────────────────────┴────────────────┘
```

Los valores se almacenan en forma binaria compacta:

``` sql
SELECT toTypeName(from), hex(from) FROM hits LIMIT 1;
```

``` text
┌─toTypeName(from)─┬─hex(from)─┐
│ IPv4             │ B7F7E83A  │
└──────────────────┴───────────┘
```

Los valores de dominio no se pueden convertir implícitamente en tipos distintos de `UInt32`.
Si desea convertir `IPv4` valor a una cadena, tienes que hacer eso explícitamente con `IPv4NumToString()` función:

``` sql
SELECT toTypeName(s), IPv4NumToString(from) as s FROM hits LIMIT 1;
```

    ┌─toTypeName(IPv4NumToString(from))─┬─s──────────────┐
    │ String                            │ 183.247.232.58 │
    └───────────────────────────────────┴────────────────┘

O echar a un `UInt32` valor:

``` sql
SELECT toTypeName(i), CAST(from as UInt32) as i FROM hits LIMIT 1;
```

``` text
┌─toTypeName(CAST(from, 'UInt32'))─┬──────────i─┐
│ UInt32                           │ 3086477370 │
└──────────────────────────────────┴────────────┘
```

[Artículo Original](https://clickhouse.tech/docs/en/data_types/domains/ipv4) <!--hide-->
