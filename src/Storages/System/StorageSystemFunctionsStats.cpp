#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/System/StorageSystemFunctionsStats.h>
#include <Common/FunctionCallStats.h>


namespace DB
{

ColumnsDescription StorageSystemFunctionsStats::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"function_name", std::make_shared<DataTypeString>(), "Name of the function."},
        {"call_count", std::make_shared<DataTypeUInt64>(), "Number of times the function was called (per block, not per row)."},
        {"rows_processed", std::make_shared<DataTypeUInt64>(), "Total number of rows processed by the function."},
    };
}


void StorageSystemFunctionsStats::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    auto all_stats = FunctionCallStats::instance().getAll();

    for (const auto & [function_name, stats] : all_stats)
    {
        size_t col_num = 0;
        res_columns[col_num++]->insert(function_name);
        res_columns[col_num++]->insert(stats.call_count);
        res_columns[col_num++]->insert(stats.rows_processed);
    }
}

}
