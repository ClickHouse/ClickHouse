#include <Columns/ColumnArray.h>
#include <Common/SystemTableDocumentation.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/ProfileEventsExt.h>
#include <Storages/System/StorageSystemUserProcesses.h>


namespace DB
{

ColumnsDescription StorageSystemUserProcesses::getColumnsDescription()
{
    auto low_cardinality_string = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());

    auto description = ColumnsDescription
    {
        {"user", std::make_shared<DataTypeString>(), "User name."},
        {"memory_usage", std::make_shared<DataTypeInt64>(), "Sum of RAM used by all processes of the user. It might not include some types of dedicated memory. See the max_memory_usage setting."},
        {"peak_memory_usage", std::make_shared<DataTypeInt64>(), "The peak of memory usage of the user. It can be reset when no queries are run for the user."},
        {"ProfileEvents", std::make_shared<DataTypeMap>(low_cardinality_string, std::make_shared<DataTypeUInt64>()), "Summary of ProfileEvents that measure different metrics for the user. The description of them could be found in the table system.events"},
    };

    description.setAliases({
        {"ProfileEvents.Names", {std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())}, "mapKeys(ProfileEvents)"},
        {"ProfileEvents.Values", {std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>())}, "mapValues(ProfileEvents)"}
    });

    return description;
}

void StorageSystemUserProcesses::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto user_info = context->getProcessList().getUserInfo(true);

    for (const auto & [user, info] : user_info)
    {
        size_t i = 0;

        res_columns[i++]->insert(user);
        res_columns[i++]->insert(info.memory_usage);
        res_columns[i++]->insert(info.peak_memory_usage);
        {
            IColumn * column = res_columns[i++].get();

            if (info.profile_counters)
                ProfileEvents::dumpToMapColumn(*info.profile_counters, column, true);
            else
            {
                column->insertDefault();
            }
        }
    }
}
}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemUserProcesses) }

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "user_processes",
    .description = R"DOCS_MD(
This system table can be used to get overview of memory usage and ProfileEvents of users.
)DOCS_MD",
    .examples = R"DOCS_MD(
```sql
SELECT * FROM system.user_processes LIMIT 10 FORMAT Vertical;
```

```response
Row 1:
──────
user:              default
memory_usage:      9832
peak_memory_usage: 9832
ProfileEvents:     {'Query':5,'SelectQuery':5,'QueriesWithSubqueries':38,'SelectQueriesWithSubqueries':38,'QueryTimeMicroseconds':842048,'SelectQueryTimeMicroseconds':842048,'ReadBufferFromFileDescriptorRead':6,'ReadBufferFromFileDescriptorReadBytes':234,'IOBufferAllocs':3,'IOBufferAllocBytes':98493,'ArenaAllocChunks':283,'ArenaAllocBytes':1482752,'FunctionExecute':670,'TableFunctionExecute':16,'DiskReadElapsedMicroseconds':19,'NetworkSendElapsedMicroseconds':684,'NetworkSendBytes':139498,'SelectedRows':6076,'SelectedBytes':685802,'ContextLock':1140,'RWLockAcquiredReadLocks':193,'RWLockReadersWaitMilliseconds':4,'RealTimeMicroseconds':1585163,'UserTimeMicroseconds':889767,'SystemTimeMicroseconds':13630,'SoftPageFaults':1947,'OSCPUWaitMicroseconds':6,'OSCPUVirtualTimeMicroseconds':903251,'OSReadChars':28631,'OSWriteChars':28888,'QueryProfilerRuns':3,'LogTrace':79,'LogDebug':24}

1 row in set. Elapsed: 0.010 sec.
```
)DOCS_MD")

}
