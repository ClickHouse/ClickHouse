#include <DB/Storages/System/StorageSystemFunctions.h>
#include <DB/Functions/FunctionFactory.h>
#include <DB/AggregateFunctions/AggregateFunctionFactory.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnsNumber.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataStreams/OneBlockInputStream.h>

namespace DB
{

StorageSystemFunctions::StorageSystemFunctions(const std::string & name_)
	: name(name_)
	, columns{
		{ "name",           new DataTypeString },
		{ "is_aggregate",   new DataTypeUInt8  }
	}
{
}

StoragePtr StorageSystemFunctions::create(const std::string & name_)
{
	return (new StorageSystemFunctions{name_})->thisPtr();
}

BlockInputStreams StorageSystemFunctions::read(
	const Names & column_names,
	ASTPtr query,
	const Context & context,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	const size_t max_block_size,
	const unsigned threads)
{
	check(column_names);
	processed_stage = QueryProcessingStage::FetchColumns;

	ColumnWithTypeAndName column_name{ new ColumnString, new DataTypeString, "name" };
	ColumnWithTypeAndName column_is_aggregate{ new ColumnUInt8, new DataTypeUInt8, "is_aggregate" };

	const auto & functions = FunctionFactory::instance().functions;
	for (const auto & it : functions)
	{
		column_name.column->insert(it.first);
		column_is_aggregate.column->insert(UInt64(0));
	}

	const auto & aggregate_function_factory = context.getAggregateFunctionFactory();
	for (const auto & details : aggregate_function_factory)
	{
		if (!details.is_alias)
		{
			column_name.column->insert(details.name);
			column_is_aggregate.column->insert(UInt64(1));
		}
	}

	return BlockInputStreams{ 1, new OneBlockInputStream{{ column_name, column_is_aggregate }} };
}

}
