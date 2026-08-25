#include <Storages/System/StorageSystemFailPoints.h>
#include <Common/SystemTableDocumentation.h>
#include <Storages/System/SystemTableSourceRegistry.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Common/FailPoint.h>


namespace DB
{

ColumnsDescription StorageSystemFailPoints::getColumnsDescription()
{
    return ColumnsDescription{
        {"name", std::make_shared<DataTypeString>(), "Name of the failpoint."},
        {"type",
         std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
             {"once", 0},
             {"regular", 1},
             {"pauseable_once", 2},
             {"pauseable", 3},
         }),
         "Type of failpoint: 'once' fires a single time then auto-disables, "
         "'regular' fires every time, "
         "'pauseable_once' blocks execution once, "
         "'pauseable' blocks execution every time until resumed."},
        {"enabled", std::make_shared<DataTypeUInt8>(), "Whether the failpoint is currently enabled (1) or disabled (0)."},
    };
}

void StorageSystemFailPoints::fillData(
    MutableColumns & res_columns, ContextPtr /* context */, const ActionsDAG::Node * /* predicate */, std::vector<UInt8> /* columns_mask */) const
{
    /// Get all available failpoints from the FailPointInjection registry.
    /// getFailPoints() returns a vector of {name, type, enabled} tuples
    /// covering all four categories: once, regular, pauseable_once, pauseable.
    const auto & fail_points = FailPointInjection::getFailPoints();

    for (const auto & [name, type, enabled] : fail_points)
    {
        res_columns[0]->insert(name);
        res_columns[1]->insert(static_cast<Int8>(type)); /// 0=once, 1=regular, 2=pauseable_once, 3=pauseable
        res_columns[2]->insert(static_cast<UInt8>(enabled ? 1 : 0));
    }
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemFailPoints) }

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "fail_points",
    .description = R"DOCS_MD(
Contains a list of all available failpoints registered in the server, along with their type and whether they are currently enabled.

Failpoints can be enabled and disabled at runtime using the `SYSTEM ENABLE FAILPOINT` and `SYSTEM DISABLE FAILPOINT` statements.
)DOCS_MD",
    .examples = R"DOCS_MD(
```sql title="Query"
SYSTEM ENABLE FAILPOINT replicated_merge_tree_insert_retry_pause;
SELECT * FROM system.fail_points WHERE enabled = 1
```

```text title="Response"
┌─name──────────────────────────────────────┬─type────────────┬─enabled─┐
│ replicated_merge_tree_insert_retry_pause  │ pauseable_once  │       1 │
└───────────────────────────────────────────┴─────────────────┴─────────┘
```
)DOCS_MD")

}
