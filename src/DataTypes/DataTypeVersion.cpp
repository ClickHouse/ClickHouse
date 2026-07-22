#include <DataTypes/DataTypeVersion.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationVersion.h>


namespace DB
{

void registerDataTypeVersion(DataTypeFactory & factory)
{
    factory.registerSimpleDataType("Version", [] { return DataTypePtr(std::make_shared<DataTypeVersion>()); }, DataTypeFactory::Case::Sensitive,
        Documentation{
            .description = R"DOCS_MD(
## Version {#version}

A semantic-version-like value `major.minor.patch.build`. Stored in 16 bytes as a packed UInt128:
`(major << 96) | (minor << 64) | (patch << 32) | build`.

### Basic Usage {#basic-usage}

```sql
CREATE TABLE releases (name String, ver Version) ENGINE = MergeTree() ORDER BY name;

INSERT INTO releases VALUES ('clickhouse', '24.3.1.2672');

SELECT * FROM releases;
```

```text
┌─name───────┬─ver─────────┐
│ clickhouse │ 24.3.1.2672 │
└────────────┴─────────────┘
```

Up to 4 dot-separated components may be given; missing trailing components are padded with zero,
so `'1.2'`, `'1.2.0'` and `'1.2.0.0'` all parse to the same value. The value is always printed back
in the full canonical 4-component form:

```sql
SELECT toVersion('1.2');
```

```text
┌─toVersion('1.2')─┐
│ 1.2.0.0           │
└───────────────────┘
```

Because the components are packed most-significant-first (major, then minor, then patch, then
build), comparing two `Version` values as plain unsigned integers is equivalent to comparing them
component-by-component, so `<`, `>`, `=`, `!=`, `<=`, `>=` all behave as expected:

```sql
SELECT toVersion('2.0.0.0') > toVersion('1.99.99.99');
```

```text
┌─greater(toVersion('2.0.0.0'), toVersion('1.99.99.99'))─┐
│                                                       1 │
└──────────────────────────────────────────────────────────┘
```
)DOCS_MD",
            .syntax = "Version",
        });
}

}
