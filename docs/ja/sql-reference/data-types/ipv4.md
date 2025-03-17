---
slug: /ja/sql-reference/data-types/ipv4
sidebar_position: 28
sidebar_label: IPv4
---

## IPv4

IPv4アドレス。4バイトでUInt32として保存されます。

### 基本的な使用法

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

または、IPv4ドメインをキーとして使用することもできます:

``` sql
CREATE TABLE hits (url String, from IPv4) ENGINE = MergeTree() ORDER BY from;
```

`IPv4` ドメインはIPv4文字列としてのカスタム入力形式をサポートしています:

``` sql
INSERT INTO hits (url, from) VALUES ('https://wikipedia.org', '116.253.40.133')('https://clickhouse.com', '183.247.232.58')('https://clickhouse.com/docs/ja/', '116.106.34.242');

SELECT * FROM hits;
```

``` text
┌─url────────────────────────────────┬───────────from─┐
│ https://clickhouse.com/docs/ja/ │ 116.106.34.242 │
│ https://wikipedia.org              │ 116.253.40.133 │
│ https://clickhouse.com          │ 183.247.232.58 │
└────────────────────────────────────┴────────────────┘
```

値はコンパクトなバイナリ形式で保存されます:

``` sql
SELECT toTypeName(from), hex(from) FROM hits LIMIT 1;
```

``` text
┌─toTypeName(from)─┬─hex(from)─┐
│ IPv4             │ B7F7E83A  │
└──────────────────┴───────────┘
```

IPv4アドレスはIPv6アドレスと直接比較することができます:

```sql
SELECT toIPv4('127.0.0.1') = toIPv6('::ffff:127.0.0.1');
```

```text
┌─equals(toIPv4('127.0.0.1'), toIPv6('::ffff:127.0.0.1'))─┐
│                                                       1 │
└─────────────────────────────────────────────────────────┘
```

**関連項目**

- [IPv4およびIPv6アドレスを操作するための関数](../functions/ip-address-functions.md)
