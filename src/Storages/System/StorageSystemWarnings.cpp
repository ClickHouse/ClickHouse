#include <Columns/IColumn.h>
#include <Common/SystemTableDocumentation.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/System/StorageSystemWarnings.h>
#include <Storages/ColumnsDescription.h>


namespace DB
{

ColumnsDescription StorageSystemWarnings::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"message", std::make_shared<DataTypeString>(), "A warning message issued by ClickHouse server."},
        {"message_format_string", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "A format string that was used to format the message."},
    };
}

void StorageSystemWarnings::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    for (const auto & [_, warning] : context->getWarnings())
    {
        res_columns[0]->insert(warning.text);
        res_columns[1]->insert(warning.format_string);
    }
}


void StorageSystemWarnings::truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &)
{
    Context::getGlobalContextInstance()->removeAllWarnings();
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemWarnings) }

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "warnings",
    .description = R"DOCS_MD(
This table shows warnings about the ClickHouse server.
Warnings of the same type are combined into a single warning.
For example, if the number N of attached databases exceeds a configurable threshold T, a single entry containing the current value N is shown instead of N separate entries.
If current value drops below the threshold, the entry is removed from the table.

The table can be configured with these settings:

- [max_table_num_to_warn](/reference/settings/server-settings/settings/max-table#max_table_num_to_warn)
- [max_database_num_to_warn](/reference/settings/server-settings/settings/max-database#max_database_num_to_warn)
- [max_dictionary_num_to_warn](/reference/settings/server-settings/settings/max-dictionary#max_dictionary_num_to_warn)
- [max_view_num_to_warn](/reference/settings/server-settings/settings/max-view#max_view_num_to_warn)
- [max_part_num_to_warn](/reference/settings/server-settings/settings/max#max_part_num_to_warn)
- [max_pending_mutations_to_warn](/reference/settings/server-settings/settings/max-pending#max_pending_mutations_to_warn)
- [max_pending_mutations_execution_time_to_warn](/reference/settings/server-settings/settings/max-pending#max_pending_mutations_execution_time_to_warn)
- [max_named_collection_num_to_warn](/reference/settings/server-settings/settings/max-named#max_named_collection_num_to_warn)
- [resource_overload_warnings](/concepts/features/configuration/settings/server-overload#resource-overload-warnings)
)DOCS_MD",
    .examples = R"DOCS_MD(
```sql title="Query"
 SELECT * FROM system.warnings LIMIT 2 \G;
```

```text title="Response"
Row 1:
──────
message:               The number of active parts is more than 10.
message_format_string: The number of active parts is more than {}.

Row 2:
──────
message:               The number of attached databases is more than 2.
message_format_string: The number of attached databases is more than {}.
```
)DOCS_MD")

}
