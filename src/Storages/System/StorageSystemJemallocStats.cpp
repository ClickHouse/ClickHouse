#include <Storages/System/StorageSystemJemallocStats.h>

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

void StorageSystemJemallocStats::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    context->checkAccess(AccessType::SYSTEM_JEMALLOC);

#if USE_JEMALLOC
    auto print_to_string = [](void * output, const char * data)
    {
        std::string * output_data = reinterpret_cast<std::string *>(output);
        *output_data += std::string(data);
    };

    std::string stats;
    malloc_stats_print(print_to_string, &stats, nullptr);

    res_columns[0]->insert(stats);
#else
    res_columns[0]->insert(std::string());
#endif
}

}
