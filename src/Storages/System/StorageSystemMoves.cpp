#include <Interpreters/Context.h>
#include <Common/SystemTableDocumentation.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypeString.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/MergeTree/MovesList.h>
#include <Storages/System/StorageSystemMoves.h>
#include <Access/ContextAccess.h>


namespace DB
{

ColumnsDescription StorageSystemMoves::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"database", std::make_shared<DataTypeString>(), "Name of the database."},
        {"table", std::make_shared<DataTypeString>(), "Name of the table containing moving data part."},
        {"elapsed", std::make_shared<DataTypeFloat64>(), "Time elapsed (in seconds) since data part movement started."},
        {"target_disk_name", std::make_shared<DataTypeString>(), "Name of disk to which the data part is moving."},
        {"target_disk_path", std::make_shared<DataTypeString>(), "Path to the mount point of the disk in the file system."},
        {"part_name", std::make_shared<DataTypeString>(), "Name of the data part being moved."},
        {"part_size", std::make_shared<DataTypeUInt64>(), "Data part size."},
        {"thread_id", std::make_shared<DataTypeUInt64>(), "Identifier of a thread performing the movement."},
    };
}


void StorageSystemMoves::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto access = context->getAccess();
    const bool check_access_for_tables = !access->isGranted(AccessType::SHOW_TABLES);

    for (const auto & move : context->getMovesList().get())
    {
        if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, move.database, move.table))
            continue;

        size_t i = 0;
        res_columns[i++]->insert(move.database);
        res_columns[i++]->insert(move.table);
        res_columns[i++]->insert(move.elapsed);
        res_columns[i++]->insert(move.target_disk_name);
        res_columns[i++]->insert(move.target_disk_path);
        res_columns[i++]->insert(move.part_name);
        res_columns[i++]->insert(move.part_size);
        res_columns[i++]->insert(move.thread_id);
    }
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemMoves) }

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "moves",
    .description = R"DOCS_MD(
The table contains information about in-progress [data part moves](/reference/statements/alter/partition#move-partitionpart) of [MergeTree](/reference/engines/table-engines/mergetree-family/mergetree) tables. Each data part movement is represented by a single row.
)DOCS_MD",
    .examples = R"DOCS_MD(
```sql
SELECT * FROM system.moves
```

```response
┌─database─┬─table─┬─────elapsed─┬─target_disk_name─┬─target_disk_path─┬─part_name─┬─part_size─┬─thread_id─┐
│ default  │ test2 │ 1.668056039 │ s3               │ ./disks/s3/      │ all_3_3_0 │       136 │    296146 │
└──────────┴───────┴─────────────┴──────────────────┴──────────────────┴───────────┴───────────┴───────────┘
```
)DOCS_MD",
    .see_also = R"DOCS_MD(
- [MergeTree](/reference/engines/table-engines/mergetree-family/mergetree) table engine
- [Using Multiple Block Devices for Data Storage](/reference/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-multiple-volumes)
- [ALTER TABLE ... MOVE PART](/reference/statements/alter/partition#move-partitionpart) command
)DOCS_MD")

}
