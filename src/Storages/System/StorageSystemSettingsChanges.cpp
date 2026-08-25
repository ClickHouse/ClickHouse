#include <Core/SettingsChangesHistory.h>
#include <Common/SystemTableDocumentation.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/System/StorageSystemSettingsChanges.h>

namespace DB
{

namespace
{
DataTypePtr getSettingsTypeEnum()
{
    return std::make_shared<DataTypeEnum8>(
    DataTypeEnum8::Values
        {
            {"Session", 0},
            {"MergeTree", 1},
        });
}
}


ColumnsDescription StorageSystemSettingsChanges::getColumnsDescription()
{
    /// TODO: Fill in all the comments
    return ColumnsDescription
    {
        {"type", getSettingsTypeEnum(), "The group of settings (Session, MergeTree...)"},
        {"version", std::make_shared<DataTypeString>(), "The ClickHouse server version."},
        {"changes",
         std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(
             DataTypes{
                 std::make_shared<DataTypeString>(),
                 std::make_shared<DataTypeString>(),
                 std::make_shared<DataTypeString>(),
                 std::make_shared<DataTypeString>()},
             Names{"name", "previous_value", "new_value", "reason"})), "The list of changes in settings which changed the behaviour of ClickHouse."},
    };
}

void StorageSystemSettingsChanges::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto & settings_changes_history = getSettingsChangesHistory();
    for (auto it = settings_changes_history.rbegin(); it != settings_changes_history.rend(); ++it)
    {
        res_columns[0]->insert(0);
        res_columns[1]->insert(it->first.toString());
        Array changes;
        for (const auto & change : it->second)
            changes.push_back(Tuple{change.name, fieldToString(change.previous_value), fieldToString(change.new_value), change.reason});
        res_columns[2]->insert(changes);
    }

    const auto & mergetree_settings_changes_history = getMergeTreeSettingsChangesHistory();
    for (auto it = mergetree_settings_changes_history.rbegin(); it != mergetree_settings_changes_history.rend(); ++it)
    {
        res_columns[0]->insert(1);
        res_columns[1]->insert(it->first.toString());
        Array changes;
        for (const auto & change : it->second)
            changes.push_back(Tuple{change.name, fieldToString(change.previous_value), fieldToString(change.new_value), change.reason});
        res_columns[2]->insert(changes);
    }
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemSettingsChanges) }

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "settings_changes",
    .description = R"DOCS_MD(
Contains information about setting changes in previous ClickHouse versions.
)DOCS_MD",
    .examples = R"DOCS_MD(
```sql
SELECT *
FROM system.settings_changes
WHERE version = '23.5'
FORMAT Vertical
```

```text
Row 1:
──────
type:    Core
version: 23.5
changes: [('input_format_parquet_preserve_order','1','0','Allow Parquet reader to reorder rows for better parallelism.'),('parallelize_output_from_storages','0','1','Allow parallelism when executing queries that read from file/url/s3/etc. This may reorder rows.'),('use_with_fill_by_sorting_prefix','0','1','Columns preceding WITH FILL columns in ORDER BY clause form sorting prefix. Rows with different values in sorting prefix are filled independently'),('output_format_parquet_compliant_nested_types','0','1','Change an internal field name in output Parquet file schema.')]
```
)DOCS_MD",
    .see_also = R"DOCS_MD(
- [Settings](/reference/system-tables/overview#system-tables-introduction)
- [system.settings](/reference/system-tables/settings)
)DOCS_MD")

}
