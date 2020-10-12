---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 60
toc_title: IPv6
---

## IPv6 {#ipv6}

`IPv6` dayalı bir doma aindir `FixedString(16)` tip ve IPv6 değerlerini depolamak için yazılan bir yedek olarak hizmet eder. İnsan dostu giriş-çıkış biçimi ve muayene ile ilgili sütun tipi bilgileri ile kompakt depolama sağlar.

### Temel Kullanım {#basic-usage}

``` sql
CREATE TABLE hits (url String, from IPv6) ENGINE = MergeTree() ORDER BY url;

DESCRIBE TABLE hits;
```

``` text
┌─name─┬─type───┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┐
│ url  │ String │              │                    │         │                  │
│ from │ IPv6   │              │                    │         │                  │
└──────┴────────┴──────────────┴────────────────────┴─────────┴──────────────────┘
```

Veya kullanabilirsiniz `IPv6` anahtar olarak etki alanı:

``` sql
CREATE TABLE hits (url String, from IPv6) ENGINE = MergeTree() ORDER BY from;
```

`IPv6` etki alanı IPv6 dizeleri olarak özel girişi destekler:

``` sql
INSERT INTO hits (url, from) VALUES ('https://wikipedia.org', '2a02:aa08:e000:3100::2')('https://clickhouse.tech', '2001:44c8:129:2632:33:0:252:2')('https://clickhouse.tech/docs/en/', '2a02:e980:1e::1');

SELECT * FROM hits;
```

``` text
┌─url────────────────────────────────┬─from──────────────────────────┐
│ https://clickhouse.tech          │ 2001:44c8:129:2632:33:0:252:2 │
│ https://clickhouse.tech/docs/en/ │ 2a02:e980:1e::1               │
│ https://wikipedia.org              │ 2a02:aa08:e000:3100::2        │
└────────────────────────────────────┴───────────────────────────────┘
```

Değerler kompakt ikili formda saklanır:

``` sql
SELECT toTypeName(from), hex(from) FROM hits LIMIT 1;
```

``` text
┌─toTypeName(from)─┬─hex(from)────────────────────────┐
│ IPv6             │ 200144C8012926320033000002520002 │
└──────────────────┴──────────────────────────────────┘
```

Etki alanı değerleri örtülü olarak dışındaki türlere dönüştürülemez `FixedString(16)`.
Dönüştürmek istiyorsanız `IPv6` bir dizeye değer, bunu açıkça yapmak zorundasınız `IPv6NumToString()` işlev:

``` sql
SELECT toTypeName(s), IPv6NumToString(from) as s FROM hits LIMIT 1;
```

``` text
┌─toTypeName(IPv6NumToString(from))─┬─s─────────────────────────────┐
│ String                            │ 2001:44c8:129:2632:33:0:252:2 │
└───────────────────────────────────┴───────────────────────────────┘
```

Ya da bir döküm `FixedString(16)` değer:

``` sql
SELECT toTypeName(i), CAST(from as FixedString(16)) as i FROM hits LIMIT 1;
```

``` text
┌─toTypeName(CAST(from, 'FixedString(16)'))─┬─i───────┐
│ FixedString(16)                           │  ��� │
└───────────────────────────────────────────┴─────────┘
```

[Orijinal makale](https://clickhouse.tech/docs/en/data_types/domains/ipv6) <!--hide-->
