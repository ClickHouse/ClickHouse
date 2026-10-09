#include <DataTypes/DataTypeIPv4andIPv6.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationIPv4andIPv6.h>


namespace DB
{

void registerDataTypeIPv4andIPv6(DataTypeFactory & factory)
{
    factory.registerSimpleDataType("IPv4", [] { return DataTypePtr(std::make_shared<DataTypeIPv4>()); }, DataTypeFactory::Case::Sensitive,
        Documentation{
            .description = R"DOCS_MD(
## IPv4 {#ipv4}

IPv4 addresses. Stored in 4 bytes as UInt32.

### Basic Usage {#basic-usage}

```sql
CREATE TABLE hits (url String, from IPv4) ENGINE = MergeTree() ORDER BY url;

DESCRIBE TABLE hits;
```

```text
┌─name─┬─type───┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┐
│ url  │ String │              │                    │         │                  │
│ from │ IPv4   │              │                    │         │                  │
└──────┴────────┴──────────────┴────────────────────┴─────────┴──────────────────┘
```

OR you can use IPv4 domain as a key:

```sql
CREATE TABLE hits (url String, from IPv4) ENGINE = MergeTree() ORDER BY from;
```

`IPv4` domain supports custom input format as IPv4-strings:

```sql
INSERT INTO hits (url, from) VALUES ('https://wikipedia.org', '116.253.40.133')('https://clickhouse.com', '183.247.232.58')('https://clickhouse.com/docs/en/', '116.106.34.242');

SELECT * FROM hits;
```

```text
┌─url────────────────────────────────┬───────────from─┐
│ https://clickhouse.com/docs/en/ │ 116.106.34.242 │
│ https://wikipedia.org              │ 116.253.40.133 │
│ https://clickhouse.com          │ 183.247.232.58 │
└────────────────────────────────────┴────────────────┘
```

Values are stored in compact binary form:

```sql
SELECT toTypeName(from), hex(from) FROM hits LIMIT 1;
```

```text
┌─toTypeName(from)─┬─hex(from)─┐
│ IPv4             │ B7F7E83A  │
└──────────────────┴───────────┘
```

IPv4 addresses can be directly compared to IPv6 addresses:

```sql
SELECT toIPv4('127.0.0.1') = toIPv6('::ffff:127.0.0.1');
```

```text
┌─equals(toIPv4('127.0.0.1'), toIPv6('::ffff:127.0.0.1'))─┐
│                                                       1 │
└─────────────────────────────────────────────────────────┘
```

**See Also**

- [Functions for Working with IPv4 and IPv6 Addresses](../functions/ip-address-functions.md)
)DOCS_MD",
            .syntax = "IPv4",
            .related = {"IPv6"},
        });
    factory.registerAlias("INET4", "IPv4", DataTypeFactory::Case::Insensitive);
    factory.registerSimpleDataType("IPv6", [] { return DataTypePtr(std::make_shared<DataTypeIPv6>()); }, DataTypeFactory::Case::Sensitive,
        Documentation{
            .description = R"DOCS_MD(
## IPv6 {#ipv6}

IPv6 addresses. Stored in 16 bytes as UInt128 big-endian.

### Basic Usage {#basic-usage}

```sql
CREATE TABLE hits (url String, from IPv6) ENGINE = MergeTree() ORDER BY url;

DESCRIBE TABLE hits;
```

```text
┌─name─┬─type───┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┐
│ url  │ String │              │                    │         │                  │
│ from │ IPv6   │              │                    │         │                  │
└──────┴────────┴──────────────┴────────────────────┴─────────┴──────────────────┘
```

OR you can use `IPv6` domain as a key:

```sql
CREATE TABLE hits (url String, from IPv6) ENGINE = MergeTree() ORDER BY from;
```

`IPv6` domain supports custom input as IPv6-strings:

```sql
INSERT INTO hits (url, from) VALUES ('https://wikipedia.org', '2a02:aa08:e000:3100::2')('https://clickhouse.com', '2001:44c8:129:2632:33:0:252:2')('https://clickhouse.com/docs/en/', '2a02:e980:1e::1');

SELECT * FROM hits;
```

```text
┌─url────────────────────────────────┬─from──────────────────────────┐
│ https://clickhouse.com          │ 2001:44c8:129:2632:33:0:252:2 │
│ https://clickhouse.com/docs/en/ │ 2a02:e980:1e::1               │
│ https://wikipedia.org              │ 2a02:aa08:e000:3100::2        │
└────────────────────────────────────┴───────────────────────────────┘
```

Values are stored in compact binary form:

```sql
SELECT toTypeName(from), hex(from) FROM hits LIMIT 1;
```

```text
┌─toTypeName(from)─┬─hex(from)────────────────────────┐
│ IPv6             │ 200144C8012926320033000002520002 │
└──────────────────┴──────────────────────────────────┘
```

IPv6 addresses can be directly compared to IPv4 addresses:

```sql
SELECT toIPv4('127.0.0.1') = toIPv6('::ffff:127.0.0.1');
```

```text
┌─equals(toIPv4('127.0.0.1'), toIPv6('::ffff:127.0.0.1'))─┐
│                                                       1 │
└─────────────────────────────────────────────────────────┘
```

**See Also**

- [Functions for Working with IPv4 and IPv6 Addresses](../functions/ip-address-functions.md)
)DOCS_MD",
            .syntax = "IPv6",
            .related = {"IPv4"},
        });
    factory.registerAlias("INET6", "IPv6", DataTypeFactory::Case::Insensitive);
}

}
