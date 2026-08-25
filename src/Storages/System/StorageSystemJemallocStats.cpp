#include <Storages/System/StorageSystemJemallocStats.h>
#include <Common/SystemTableDocumentation.h>
#include <Storages/System/SystemTableSourceRegistry.h>

#include <Columns/IColumn.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>
#include <Access/Common/AccessFlags.h>

#if USE_JEMALLOC
#    include <jemalloc/jemalloc.h>
#endif

namespace DB
{

ColumnsDescription StorageSystemJemallocStats::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"stats", std::make_shared<DataTypeString>(), "JEMalloc statistics output from malloc_stats_print."},
    };
}

void StorageSystemJemallocStats::fillData(
    MutableColumns & res_columns, ContextPtr /*context*/, const ActionsDAG::Node *, std::vector<UInt8>) const
{
#if USE_JEMALLOC
    auto print_to_string = [](void * output, const char * data)
    {
        std::string * output_data = reinterpret_cast<std::string *>(output);
        *output_data += std::string(data);
    };

    std::string stats;
    je_malloc_stats_print(print_to_string, &stats, nullptr);

    res_columns[0]->insert(stats);
#else
    res_columns[0]->insert(std::string());
#endif
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemJemallocStats) }

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "jemalloc_stats",
    .description = R"DOCS_MD(
Returns jemalloc statistics in a single row with a single column. Equivalent to SYSTEM JEMALLOC STATS command.
)DOCS_MD")

}
