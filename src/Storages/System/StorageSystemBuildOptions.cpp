#include <Storages/System/StorageSystemBuildOptions.h>
#include <Common/SystemTableDocumentation.h>
#include <Storages/System/SystemTableSourceRegistry.h>

#include <Columns/IColumn.h>
#include <DataTypes/DataTypeString.h>
#include <Core/Settings.h>

extern const char * auto_config_build[];

namespace DB
{

ColumnsDescription StorageSystemBuildOptions::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Name of the build option."},
        {"value", std::make_shared<DataTypeString>(), "Value of the build option."},
    };
}

void StorageSystemBuildOptions::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    for (auto * it = auto_config_build; *it; it += 2)
    {
        res_columns[0]->insert(it[0]);
        res_columns[1]->insert(it[1]);
    }
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemBuildOptions) }

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "build_options",
    .description = R"DOCS_MD(
Contains information about the ClickHouse server's build options.
)DOCS_MD",
    .examples = R"DOCS_MD(
```sql
SELECT * FROM system.build_options LIMIT 5
```

```text
┌─name─────────────┬─value─┐
│ USE_BROTLI       │ 1     │
│ USE_BZIP2        │ 1     │
│ USE_CAPNP        │ 1     │
│ USE_CASSANDRA    │ 1     │
│ USE_DATASKETCHES │ 1     │
└──────────────────┴───────┘
```
)DOCS_MD")

}
