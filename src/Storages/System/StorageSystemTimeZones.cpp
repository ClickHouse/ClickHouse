#include <Columns/IColumn.h>
#include <Common/SystemTableDocumentation.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/System/StorageSystemTimeZones.h>


extern const char * auto_time_zones[];

namespace DB
{
ColumnsDescription StorageSystemTimeZones::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"time_zone", std::make_shared<DataTypeString>(), "List of supported time zones."},
    };
}

void StorageSystemTimeZones::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    for (auto * it = auto_time_zones; *it; ++it)
        res_columns[0]->insert(String(*it));
}
}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemTimeZones) }

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "time_zones",
    .description = R"DOCS_MD(
Contains a list of time zones that are supported by the ClickHouse server. This list of timezones might vary depending on the version of ClickHouse.
)DOCS_MD",
    .examples = R"DOCS_MD(
```sql
SELECT * FROM system.time_zones LIMIT 10
```

```text
┌─time_zone──────────┐
│ Africa/Abidjan     │
│ Africa/Accra       │
│ Africa/Addis_Ababa │
│ Africa/Algiers     │
│ Africa/Asmara      │
│ Africa/Asmera      │
│ Africa/Bamako      │
│ Africa/Bangui      │
│ Africa/Banjul      │
│ Africa/Bissau      │
└────────────────────┘
```
)DOCS_MD")

}
