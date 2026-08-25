#include <Storages/System/StorageSystemDiskTypes.h>
#include <Common/SystemTableDocumentation.h>
#include <Storages/System/SystemTableSourceRegistry.h>

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <Disks/DiskFactory.h>

namespace DB
{

ColumnsDescription StorageSystemDiskTypes::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "The name of the disk type, as specified in the `type` of a disk configuration."},
        {"description", std::make_shared<DataTypeString>(), "A high-level description of what the disk type does."},
        {"syntax", std::make_shared<DataTypeString>(), "How the disk type is specified in a disk configuration."},
        {"examples", std::make_shared<DataTypeString>(), "Usage examples."},
        {"introduced_in", std::make_shared<DataTypeString>(), "The ClickHouse version in which the disk type was first introduced, in the form major.minor."},
        {"related", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "The names of related disk types."},
    };
}

void StorageSystemDiskTypes::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto & factory = DiskFactory::instance();
    for (const auto & name : factory.getAllRegisteredNames())
    {
        const auto documentation = factory.getDocumentation(name);

        size_t i = 0;
        res_columns[i++]->insert(name);
        res_columns[i++]->insert(documentation.description);
        res_columns[i++]->insert(documentation.syntaxAsString());
        res_columns[i++]->insert(documentation.examplesAsString());
        res_columns[i++]->insert(documentation.introducedInAsString());

        Array related;
        for (const auto & related_name : documentation.related)
            related.push_back(related_name);
        res_columns[i++]->insert(related);
    }
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemDiskTypes) }

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "disk_types",
    .description = R"DOCS_MD(
Contains the list of disk types supported by the server, along with embedded documentation for each type. A disk type is specified in the `type` of a disk configuration and determines where and how a disk stores its data (local filesystem, object storage, a cache over another disk, and so on).

Note that this table lists the available disk *types*, whereas [`system.disks`](/reference/system-tables/disks) lists the disk instances configured on the server.
)DOCS_MD",
    .examples = R"DOCS_MD(
```sql title="Query"
SELECT name, description
FROM system.disk_types
WHERE name IN ('local', 'object_storage')
ORDER BY name
```
)DOCS_MD",
    .additional_sections = R"DOCS_MD(
## Configuration examples {#configuration-examples}

A disk can be configured in two ways: **statically**, in the server configuration files (XML or YAML), or **dynamically**, in the settings of a `CREATE`/`ATTACH` query using the `disk` function. The same disk type and parameters are accepted in both cases.

### Static configuration {#static-configuration}

Disks are defined under `storage_configuration` in the server configuration. The following example defines an `s3` disk and a storage policy that uses it.

```xml title="config.xml"
<clickhouse>
    <storage_configuration>
        <disks>
            <s3_disk>
                <type>s3</type>
                <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
                <use_environment_credentials>1</use_environment_credentials>
            </s3_disk>
        </disks>
        <policies>
            <s3_policy>
                <volumes>
                    <main>
                        <disk>s3_disk</disk>
                    </main>
                </volumes>
            </s3_policy>
        </policies>
    </storage_configuration>
</clickhouse>
```

The same configuration in YAML:

```yaml title="config.yaml"
storage_configuration:
  disks:
    s3_disk:
      type: s3
      endpoint: https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/
      use_environment_credentials: 1
  policies:
    s3_policy:
      volumes:
        main:
          disk: s3_disk
```

A table can then use the disk through its storage policy:

```sql title="Query"
CREATE TABLE test (a Int32, b String)
ENGINE = MergeTree() ORDER BY a
SETTINGS storage_policy = 's3_policy';
```

### Dynamic configuration {#dynamic-configuration}

A disk can also be defined directly in the settings of a `CREATE`/`ATTACH` query, without a predefined disk in the configuration files, using the `disk` function:

```sql title="Query"
CREATE TABLE test (a Int32, b String)
ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(
    type = s3,
    endpoint = 'https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/',
    use_environment_credentials = 1
);
```

See [Configuring external storage](/concepts/features/configuration/server-config/storing-data) for the full list of parameters of each disk type.
)DOCS_MD",
    .see_also = R"DOCS_MD(
- [`system.disks`](/reference/system-tables/disks) — The disk instances configured on the server.
- [`system.storage_policies`](/reference/system-tables/storage_policies) — Storage policies and volumes.
)DOCS_MD")

}
