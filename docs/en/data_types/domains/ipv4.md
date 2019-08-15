## IPv4

`IPv4` is a domain based on `UInt32` type and serves as typed replacement for storing IPv4 values. It provides compact storage with human-friendly input-output format, and column type information on inspection.

### Basic Usage

``` sql
CREATE TABLE hits (url String, from IPv4) ENGINE = MergeTree() ORDER BY url;

DESCRIBE TABLE hits;
```

```
┌─name─┬─type───┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┐
│ url  │ String │              │                    │         │                  │
│ from │ IPv4   │              │                    │         │                  │
└──────┴────────┴──────────────┴────────────────────┴─────────┴──────────────────┘
```

OR you can use IPv4 domain as a key:

``` sql
CREATE TABLE hits (url String, from IPv4) ENGINE = MergeTree() ORDER BY from;
```

`IPv4` domain supports custom input format as IPv4-strings:

``` sql
INSERT INTO hits (url, from) VALUES ('https://wikipedia.org', '116.253.40.133')('https://clickhouse.yandex', '183.247.232.58')('https://clickhouse.yandex/docs/en/', '116.106.34.242');

SELECT * FROM hits;
```

```
┌─url────────────────────────────────┬───────────from─┐
│ https://clickhouse.yandex/docs/en/ │ 116.106.34.242 │
│ https://wikipedia.org              │ 116.253.40.133 │
│ https://clickhouse.yandex          │ 183.247.232.58 │
└────────────────────────────────────┴────────────────┘
```

Values are stored in compact binary form:

``` sql
SELECT toTypeName(from), hex(from) FROM hits LIMIT 1;
```

```
┌─toTypeName(from)─┬─hex(from)─┐
│ IPv4             │ B7F7E83A  │
└──────────────────┴───────────┘
```

Domain values are not implicitly convertible to types other than `UInt32`.
If you want to convert `IPv4` value to a string, you have to do that explicitly with `IPv4NumToString()` function:

``` sql
SELECT toTypeName(s), IPv4NumToString(from) as s FROM hits LIMIT 1;
```

```
┌─toTypeName(IPv4NumToString(from))─┬─s──────────────┐
│ String                            │ 183.247.232.58 │
└───────────────────────────────────┴────────────────┘
```

Or cast to a `UInt32` value:

``` sql
SELECT toTypeName(i), CAST(from as UInt32) as i FROM hits LIMIT 1;
```

```
┌─toTypeName(CAST(from, 'UInt32'))─┬──────────i─┐
│ UInt32                           │ 3086477370 │
└──────────────────────────────────┴────────────┘
```

[Original article](https://clickhouse.yandex/docs/en/data_types/domains/ipv4) <!--hide-->
