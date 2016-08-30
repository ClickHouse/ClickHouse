#include <DB/Storages/System/StorageSystemMerges.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/Columns/ColumnString.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/Interpreters/Context.h>
#include <DB/Storages/MergeTree/MergeList.h>


namespace DB
{

StorageSystemMerges::StorageSystemMerges(const std::string & name)
	: name{name}
	, columns{
		{ "database", std::make_shared<DataTypeString>() },
		{ "table", std::make_shared<DataTypeString>() },
		{ "elapsed", std::make_shared<DataTypeFloat64>() },
		{ "progress", std::make_shared<DataTypeFloat64>() },
		{ "num_parts", std::make_shared<DataTypeUInt64>() },
		{ "result_part_name", std::make_shared<DataTypeString>() },
		{ "total_size_bytes_compressed", std::make_shared<DataTypeUInt64>() },
		{ "total_size_marks", std::make_shared<DataTypeUInt64>() },
		{ "bytes_read_uncompressed", std::make_shared<DataTypeUInt64>() },
		{ "rows_read", std::make_shared<DataTypeUInt64>() },
		{ "bytes_written_uncompressed", std::make_shared<DataTypeUInt64>() },
		{ "rows_written", std::make_shared<DataTypeUInt64>() }
	}
{
}

StoragePtr StorageSystemMerges::create(const std::string & name)
{
	return make_shared(name);
}

BlockInputStreams StorageSystemMerges::read(
	const Names & column_names,
	ASTPtr query,
	const Context & context,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	const size_t max_block_size,
	const unsigned)
{
	check(column_names);
	processed_stage = QueryProcessingStage::FetchColumns;

	ColumnWithTypeAndName col_database{std::make_shared<ColumnString>(), std::make_shared<DataTypeString>(), "database"};
	ColumnWithTypeAndName col_table{std::make_shared<ColumnString>(), std::make_shared<DataTypeString>(), "table"};
	ColumnWithTypeAndName col_elapsed{std::make_shared<ColumnFloat64>(), std::make_shared<DataTypeFloat64>(), "elapsed"};
	ColumnWithTypeAndName col_progress{std::make_shared<ColumnFloat64>(), std::make_shared<DataTypeFloat64>(), "progress"};
	ColumnWithTypeAndName col_num_parts{std::make_shared<ColumnUInt64>(), std::make_shared<DataTypeUInt64>(), "num_parts"};
	ColumnWithTypeAndName col_result_part_name{std::make_shared<ColumnString>(), std::make_shared<DataTypeString>(), "result_part_name"};
	ColumnWithTypeAndName col_total_size_bytes_compressed{std::make_shared<ColumnUInt64>(), std::make_shared<DataTypeUInt64>(), "total_size_bytes_compressed"};
	ColumnWithTypeAndName col_total_size_marks{std::make_shared<ColumnUInt64>(), std::make_shared<DataTypeUInt64>(), "total_size_marks"};
	ColumnWithTypeAndName col_bytes_read_uncompressed{std::make_shared<ColumnUInt64>(), std::make_shared<DataTypeUInt64>(), "bytes_read_uncompressed"};
	ColumnWithTypeAndName col_rows_read{std::make_shared<ColumnUInt64>(), std::make_shared<DataTypeUInt64>(), "rows_read"};
	ColumnWithTypeAndName col_bytes_written_uncompressed{std::make_shared<ColumnUInt64>(), std::make_shared<DataTypeUInt64>(), "bytes_written_uncompressed"};
	ColumnWithTypeAndName col_rows_written{std::make_shared<ColumnUInt64>(), std::make_shared<DataTypeUInt64>(), "rows_written"};

	for (const auto & merge : context.getMergeList().get())
	{
		col_database.column->insert(merge.database);
		col_table.column->insert(merge.table);
		col_elapsed.column->insert(merge.watch.elapsedSeconds());
		col_progress.column->insert(merge.progress);
		col_num_parts.column->insert(merge.num_parts);
		col_result_part_name.column->insert(merge.result_part_name);
		col_total_size_bytes_compressed.column->insert(merge.total_size_bytes_compressed);
		col_total_size_marks.column->insert(merge.total_size_marks);
		col_bytes_read_uncompressed.column->insert(merge.bytes_read_uncompressed.load(std::memory_order_relaxed));
		col_rows_read.column->insert(merge.rows_read.load(std::memory_order_relaxed));
		col_bytes_written_uncompressed.column->insert(merge.bytes_written_uncompressed.load(std::memory_order_relaxed));
		col_rows_written.column->insert(merge.rows_written.load(std::memory_order_relaxed));
	}

	Block block{
		col_database,
		col_table,
		col_elapsed,
		col_progress,
		col_num_parts,
		col_result_part_name,
		col_total_size_bytes_compressed,
		col_total_size_marks,
		col_bytes_read_uncompressed,
		col_rows_read,
		col_bytes_written_uncompressed,
		col_rows_written
	};

	return BlockInputStreams{1, std::make_shared<OneBlockInputStream>(block)};
}

}
