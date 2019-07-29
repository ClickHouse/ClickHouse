## IPv6

`IPv6` 是基于`FixedString(16)` 类型的domain类型，用来存储IPv6地址值。它使用紧凑的存储方式，提供用户友好的输入输出格式， 自动检查列类型。

### 基本用法

``` sql
CREATE TABLE hits (url String, from IPv6) ENGINE = MergeTree() ORDER BY url;

DESCRIBE TABLE hits;
```

```
┌─name─┬─type───┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┐
│ url  │ String │              │                    │         │                  │
│ from │ IPv6   │              │                    │         │                  │
└──────┴────────┴──────────────┴────────────────────┴─────────┴──────────────────┘
```

您也可以使用 `IPv6`domain做主键:

``` sql
CREATE TABLE hits (url String, from IPv6) ENGINE = MergeTree() ORDER BY from;
```

`IPv6` domain支持定制化的IPv6地址字符串格式:

``` sql
INSERT INTO hits (url, from) VALUES ('https://wikipedia.org', '2a02:aa08:e000:3100::2')('https://clickhouse.yandex', '2001:44c8:129:2632:33:0:252:2')('https://clickhouse.yandex/docs/en/', '2a02:e980:1e::1');

SELECT * FROM hits;
```

```
┌─url────────────────────────────────┬─from──────────────────────────┐
│ https://clickhouse.yandex          │ 2001:44c8:129:2632:33:0:252:2 │
│ https://clickhouse.yandex/docs/en/ │ 2a02:e980:1e::1               │
│ https://wikipedia.org              │ 2a02:aa08:e000:3100::2        │
└────────────────────────────────────┴───────────────────────────────┘
```

它以紧凑的二进制格式存储数值:

``` sql
SELECT toTypeName(from), hex(from) FROM hits LIMIT 1;
```

```
┌─toTypeName(from)─┬─hex(from)────────────────────────┐
│ IPv6             │ 200144C8012926320033000002520002 │
└──────────────────┴──────────────────────────────────┘
```
Domain不可隐式转换为除`FixedString(16)`以外的类型。如果要将`IPv6`值转换为字符串，则必须使用`IPv6NumToString()`函数显示地进行此操作:

``` sql
SELECT toTypeName(s), IPv6NumToString(from) as s FROM hits LIMIT 1;
```

```
┌─toTypeName(IPv6NumToString(from))─┬─s─────────────────────────────┐
│ String                            │ 2001:44c8:129:2632:33:0:252:2 │
└───────────────────────────────────┴───────────────────────────────┘
```

或转换为 `FixedString(16)`类型:

``` sql
SELECT toTypeName(i), CAST(from as FixedString(16)) as i FROM hits LIMIT 1;
```

```
┌─toTypeName(CAST(from, 'FixedString(16)'))─┬─i───────┐
│ FixedString(16)                           │  ��� │
└───────────────────────────────────────────┴─────────┘
```

[Original article](https://clickhouse.yandex/docs/en/data_types/domains/ipv6) <!--hide-->
