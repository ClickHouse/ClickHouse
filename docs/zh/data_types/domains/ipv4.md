## IPv4

`IPv4` 是基于 `UInt32` 的domain类型，用来存储IPv4地址。它使用紧凑的存储方式，提供用户友好的输入输出格式，
自动检查列类型。

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

或者你可以使用IPv4 domain作主键：

``` sql
CREATE TABLE hits (url String, from IPv4) ENGINE = MergeTree() ORDER BY from;
```

`IPv4` domain支持定制化的IPv4地址字符串格式:

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

数据值以紧凑的二进制格式存储:

``` sql
SELECT toTypeName(from), hex(from) FROM hits LIMIT 1;
```

```
┌─toTypeName(from)─┬─hex(from)─┐
│ IPv4             │ B7F7E83A  │
└──────────────────┴───────────┘
```

Domain不可隐式转换为除`UInt32`以外的类型。如果要将IPv4值转换为字符串，则必须使用`IPv4NumToString()`函数显示的进行此操作。

``` sql
SELECT toTypeName(s), IPv4NumToString(from) as s FROM hits LIMIT 1;
```

```
┌─toTypeName(IPv4NumToString(from))─┬─s──────────────┐
│ String                            │ 183.247.232.58 │
└───────────────────────────────────┴────────────────┘
```

或转换为 `UInt32` 类型:

``` sql
SELECT toTypeName(i), CAST(from as UInt32) as i FROM hits LIMIT 1;
```

```
┌─toTypeName(CAST(from, 'UInt32'))─┬──────────i─┐
│ UInt32                           │ 3086477370 │
└──────────────────────────────────┴────────────┘
```

[Original article](https://clickhouse.yandex/docs/en/data_types/domains/ipv4) <!--hide-->
