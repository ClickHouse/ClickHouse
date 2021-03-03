---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 59
toc_title: Ipv44
---

## Ipv44 {#ipv4}

`IPv4` dayalı bir doma aindir `UInt32` tip ve IPv4 değerlerini depolamak için yazılan bir yedek olarak hizmet eder. İnsan dostu giriş-çıkış biçimi ve muayene ile ilgili sütun tipi bilgileri ile kompakt depolama sağlar.

### Temel Kullanım {#basic-usage}

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

Veya IPv4 etki alanını anahtar olarak kullanabilirsiniz:

``` sql
CREATE TABLE hits (url String, from IPv4) ENGINE = MergeTree() ORDER BY from;
```

`IPv4` etki alanı IPv4 dizeleri olarak özel giriş biçimini destekler:

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

Değerler kompakt ikili formda saklanır:

``` sql
SELECT toTypeName(from), hex(from) FROM hits LIMIT 1;
```

``` text
┌─toTypeName(from)─┬─hex(from)─┐
│ IPv4             │ B7F7E83A  │
└──────────────────┴───────────┘
```

Etki alanı değerleri örtülü olarak dışındaki türlere dönüştürülemez `UInt32`.
Dönüştürmek istiyorsanız `IPv4` bir dizeye değer, bunu açıkça yapmak zorundasınız `IPv4NumToString()` işlev:

``` sql
SELECT toTypeName(s), IPv4NumToString(from) as s FROM hits LIMIT 1;
```

    ┌─toTypeName(IPv4NumToString(from))─┬─s──────────────┐
    │ String                            │ 183.247.232.58 │
    └───────────────────────────────────┴────────────────┘

Ya da bir döküm `UInt32` değer:

``` sql
SELECT toTypeName(i), CAST(from as UInt32) as i FROM hits LIMIT 1;
```

``` text
┌─toTypeName(CAST(from, 'UInt32'))─┬──────────i─┐
│ UInt32                           │ 3086477370 │
└──────────────────────────────────┴────────────┘
```

[Orijinal makale](https://clickhouse.tech/docs/en/data_types/domains/ipv4) <!--hide-->
