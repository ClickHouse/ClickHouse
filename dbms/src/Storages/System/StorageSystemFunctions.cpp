#include <DB/Storages/System/StorageSystemFunctions.h>
#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/IFunction.h>
#include <DB/AggregateFunctions/AggregateFunctionFactory.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnsNumber.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/Interpreters/Context.h>


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

StoragePtr StorageSystemFunctions::create(const std::string & name_)
{
	return make_shared(name_);
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

	ColumnWithTypeAndName column_name{ std::make_shared<ColumnString>(), std::make_shared<DataTypeString>(), "name" };
	ColumnWithTypeAndName column_is_aggregate{ std::make_shared<ColumnUInt8>(), std::make_shared<DataTypeUInt8>(), "is_aggregate" };

	const auto & functions = FunctionFactory::instance().functions;
	for (const auto & it : functions)
	{
		column_name.column->insert(it.first);
		column_is_aggregate.column->insert(UInt64(0));
	}

	const auto & aggregate_functions = context.getAggregateFunctionFactory().aggregate_functions;
	for (const auto & it : aggregate_functions)
	{
		column_name.column->insert(it.first);
		column_is_aggregate.column->insert(UInt64(1));
	}

	return BlockInputStreams{ std::make_shared<OneBlockInputStream>(Block{ column_name, column_is_aggregate }) };
}

}
