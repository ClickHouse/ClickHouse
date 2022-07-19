## IPv6 {#ipv6}

`IPv6`是与`FixedString(16)`类型保持二进制兼容的Domain类型，其用于存储IPv6地址的值。它提供了更为紧凑的二进制存储的同时支持识别可读性更加友好的输入输出格式。

### 基本用法 {#ji-ben-yong-fa}

``` sql
CREATE TABLE hits (url String, from IPv6) ENGINE = MergeTree() ORDER BY url;

DESCRIBE TABLE hits;
```

    ┌─name─┬─type───┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┐
    │ url  │ String │              │                    │         │                  │
    │ from │ IPv6   │              │                    │         │                  │
    └──────┴────────┴──────────────┴────────────────────┴─────────┴──────────────────┘

同时您也可以使用`IPv6`类型的列作为主键：

``` sql
CREATE TABLE hits (url String, from IPv6) ENGINE = MergeTree() ORDER BY from;
```

在写入与查询时，`IPv6`类型能够识别可读性更加友好的输入输出格式：

``` sql
INSERT INTO hits (url, from) VALUES ('https://wikipedia.org', '2a02:aa08:e000:3100::2')('https://clickhouse.com', '2001:44c8:129:2632:33:0:252:2')('https://clickhouse.com/docs/en/', '2a02:e980:1e::1');

SELECT * FROM hits;
```

    ┌─url────────────────────────────────┬─from──────────────────────────┐
    │ https://clickhouse.com          │ 2001:44c8:129:2632:33:0:252:2 │
    │ https://clickhouse.com/docs/en/ │ 2a02:e980:1e::1               │
    │ https://wikipedia.org              │ 2a02:aa08:e000:3100::2        │
    └────────────────────────────────────┴───────────────────────────────┘

同时它提供更为紧凑的二进制存储格式：

``` sql
SELECT toTypeName(from), hex(from) FROM hits LIMIT 1;
```

    ┌─toTypeName(from)─┬─hex(from)────────────────────────┐
    │ IPv6             │ 200144C8012926320033000002520002 │
    └──────────────────┴──────────────────────────────────┘

不可隐式转换为除`FixedString(16)`以外的其他类型类型。如果要将`IPv6`类型的值转换成字符串，你可以使用`IPv6NumToString()`显示的进行转换：

``` sql
SELECT toTypeName(s), IPv6NumToString(from) as s FROM hits LIMIT 1;
```

    ┌─toTypeName(IPv6NumToString(from))─┬─s─────────────────────────────┐
    │ String                            │ 2001:44c8:129:2632:33:0:252:2 │
    └───────────────────────────────────┴───────────────────────────────┘

或使用`CAST`将其转换为`FixedString(16)`：

``` sql
SELECT toTypeName(i), CAST(from as FixedString(16)) as i FROM hits LIMIT 1;
```

    ┌─toTypeName(CAST(from, 'FixedString(16)'))─┬─i───────┐
    │ FixedString(16)                           │  ��� │
    └───────────────────────────────────────────┴─────────┘

[来源文章](https://clickhouse.com/docs/en/data_types/domains/ipv6) <!--hide-->
