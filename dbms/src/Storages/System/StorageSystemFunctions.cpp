#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Storages/System/StorageSystemFunctions.h>


namespace DB
{

NamesAndTypesList StorageSystemFunctions::getNamesAndTypes()
{
    return {
        {"name", std::make_shared<DataTypeString>()},
        {"is_aggregate", std::make_shared<DataTypeUInt8>()},
    };
}

void StorageSystemFunctions::fillData(MutableColumns & res_columns, const Context &, const SelectQueryInfo &) const
{
    const auto & functions = FunctionFactory::instance().functions;
    for (const auto & it : functions)
    {
        res_columns[0]->insert(it.first);
        res_columns[1]->insert(UInt64(0));
    }

    const auto & aggregate_functions = AggregateFunctionFactory::instance().aggregate_functions;
    for (const auto & it : aggregate_functions)
    {
        res_columns[0]->insert(it.first);
        res_columns[1]->insert(UInt64(1));
    }
}

}
