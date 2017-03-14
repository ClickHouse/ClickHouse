#include <DB/Storages/System/StorageSystemAsynchronousMetrics.h>

#include <DB/Interpreters/AsynchronousMetrics.h>
#include <DB/Columns/ColumnsNumber.h>
#include <DB/Columns/ColumnString.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypesNumber.h>
#include <DB/DataStreams/OneBlockInputStream.h>


namespace DB
{


StorageSystemAsynchronousMetrics::StorageSystemAsynchronousMetrics(const std::string & name_, const AsynchronousMetrics & async_metrics_)
	: name(name_),
	columns
	{
		{"metric", 		std::make_shared<DataTypeString>()},
		{"value",		std::make_shared<DataTypeFloat64>()},
	},
	async_metrics(async_metrics_)
{
}

StoragePtr StorageSystemAsynchronousMetrics::create(const std::string & name_, const AsynchronousMetrics & async_metrics_)
{
	return make_shared(name_, async_metrics_);
}


BlockInputStreams StorageSystemAsynchronousMetrics::read(
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

	Block block;

	ColumnWithTypeAndName col_metric;
	col_metric.name = "metric";
	col_metric.type = std::make_shared<DataTypeString>();
	col_metric.column = std::make_shared<ColumnString>();
	block.insert(col_metric);

	ColumnWithTypeAndName col_value;
	col_value.name = "value";
	col_value.type = std::make_shared<DataTypeFloat64>();
	col_value.column = std::make_shared<ColumnFloat64>();
	block.insert(col_value);

	auto async_metrics_values = async_metrics.getValues();

	for (const auto & name_value : async_metrics_values)
	{
		col_metric.column->insert(name_value.first);
		col_value.column->insert(name_value.second);
	}

	return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(block));
}


}
