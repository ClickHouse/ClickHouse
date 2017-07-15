#include <Storages/System/StorageSystemFunctions.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Interpreters/Context.h>


namespace DB
{

StorageSystemFunctions::StorageSystemFunctions(const std::string & name_)
    : name(name_)
    , columns{
        { "name",           std::make_shared<DataTypeString>() },
        { "is_aggregate",   std::make_shared<DataTypeUInt8>()  }
    }
{
}


BlockInputStreams StorageSystemFunctions::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    const size_t max_block_size,
    const unsigned num_streams)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;

    ColumnWithTypeAndName column_name{ std::make_shared<ColumnString>(), std::make_shared<DataTypeString>(), "name" };
    ColumnWithTypeAndName column_is_aggregate{ std::make_shared<ColumnUInt8>(), std::make_shared<DataTypeUInt8>(), "is_aggregate" };

    const auto & functions = FunctionFactory::instance().functions;
    for (const auto & it : functions)
    {
        column_name.column->insert(it.first);
        column_is_aggregate.column->insert(UInt64(0));
    }

    const auto & aggregate_functions = AggregateFunctionFactory::instance().aggregate_functions;
    for (const auto & it : aggregate_functions)
    {
        column_name.column->insert(it.first);
        column_is_aggregate.column->insert(UInt64(1));
    }

    return BlockInputStreams{ std::make_shared<OneBlockInputStream>(Block{ column_name, column_is_aggregate }) };
}

}
