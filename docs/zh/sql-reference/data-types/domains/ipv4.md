## IPv4 {#ipv4}

`IPv4`是与`UInt32`类型保持二进制兼容的Domain类型，其用于存储IPv4地址的值。它提供了更为紧凑的二进制存储的同时支持识别可读性更加友好的输入输出格式。

### 基本使用 {#ji-ben-shi-yong}

``` sql
CREATE TABLE hits (url String, from IPv4) ENGINE = MergeTree() ORDER BY url;

DESCRIBE TABLE hits;
```

    ┌─name─┬─type───┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┐
    │ url  │ String │              │                    │         │                  │
    │ from │ IPv4   │              │                    │         │                  │
    └──────┴────────┴──────────────┴────────────────────┴─────────┴──────────────────┘

同时您也可以使用`IPv4`类型的列作为主键：

``` sql
CREATE TABLE hits (url String, from IPv4) ENGINE = MergeTree() ORDER BY from;
```

在写入与查询时，`IPv4`类型能够识别可读性更加友好的输入输出格式：

``` sql
INSERT INTO hits (url, from) VALUES ('https://wikipedia.org', '116.253.40.133')('https://clickhouse.tech', '183.247.232.58')('https://clickhouse.tech/docs/en/', '116.106.34.242');

SELECT * FROM hits;
```

    ┌─url────────────────────────────────┬───────────from─┐
    │ https://clickhouse.tech/docs/en/ │ 116.106.34.242 │
    │ https://wikipedia.org              │ 116.253.40.133 │
    │ https://clickhouse.tech          │ 183.247.232.58 │
    └────────────────────────────────────┴────────────────┘

同时它提供更为紧凑的二进制存储格式：

``` sql
SELECT toTypeName(from), hex(from) FROM hits LIMIT 1;
```

    ┌─toTypeName(from)─┬─hex(from)─┐
    │ IPv4             │ B7F7E83A  │
    └──────────────────┴───────────┘

不可隐式转换为除`UInt32`以外的其他类型类型。如果要将`IPv4`类型的值转换成字符串，你可以使用`IPv4NumToString()`显示的进行转换：

``` sql
SELECT toTypeName(s), IPv4NumToString(from) as s FROM hits LIMIT 1;
```

    ┌─toTypeName(IPv4NumToString(from))─┬─s──────────────┐
    │ String                            │ 183.247.232.58 │
    └───────────────────────────────────┴────────────────┘

或可以使用`CAST`将它转换为`UInt32`类型:

``` sql
SELECT toTypeName(i), CAST(from as UInt32) as i FROM hits LIMIT 1;
```

    ┌─toTypeName(CAST(from, 'UInt32'))─┬──────────i─┐
    │ UInt32                           │ 3086477370 │
    └──────────────────────────────────┴────────────┘

[来源文章](https://clickhouse.tech/docs/en/data_types/domains/ipv4) <!--hide-->
