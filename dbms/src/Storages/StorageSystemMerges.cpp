#include <DB/Storages/StorageSystemMerges.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/Columns/ColumnString.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/Interpreters/Context.h>

namespace DB
{

StorageSystemMerges::StorageSystemMerges(const std::string & name, const Context & context)
	: name{name}, context(context)
	, columns{
		{ "database", new DataTypeString },
		{ "table", new DataTypeString },
		{ "num_parts", new DataTypeUInt64 },
		{ "result_part_name", new DataTypeString },
		{ "total_size_bytes", new DataTypeUInt64 },
		{ "total_size_marks", new DataTypeUInt64 },
		{ "bytes_read", new DataTypeUInt64 },
		{ "rows_read", new DataTypeUInt64 },
		{ "bytes_written", new DataTypeUInt64 },
		{ "rows_written", new DataTypeUInt64 }
	}
{
}

StoragePtr StorageSystemMerges::create(const std::string & name, const Context & context)
{
	return (new StorageSystemMerges{name, context})->thisPtr();
}

BlockInputStreams StorageSystemMerges::read(
	const Names & column_names, ASTPtr query, const Settings & settings,
	QueryProcessingStage::Enum & processed_stage, const size_t max_block_size, const unsigned)
{
	check(column_names);
	processed_stage = QueryProcessingStage::FetchColumns;

	Block block;

	ColumnWithNameAndType col_database{new ColumnString, new DataTypeString, "database"};
	ColumnWithNameAndType col_table{new ColumnString, new DataTypeString, "table"};
	ColumnWithNameAndType col_num_parts{new ColumnUInt64, new DataTypeUInt64, "num_parts"};
	ColumnWithNameAndType col_result_part_name{new ColumnString, new DataTypeString, "result_part_name"};
	ColumnWithNameAndType col_total_size_bytes{new ColumnUInt64, new DataTypeUInt64, "total_size_bytes"};
	ColumnWithNameAndType col_total_size_marks{new ColumnUInt64, new DataTypeUInt64, "total_size_marks"};
	ColumnWithNameAndType col_bytes_read{new ColumnUInt64, new DataTypeUInt64, "bytes_read"};
	ColumnWithNameAndType col_rows_read{new ColumnUInt64, new DataTypeUInt64, "rows_read"};
	ColumnWithNameAndType col_bytes_written{new ColumnUInt64, new DataTypeUInt64, "bytes_written"};
	ColumnWithNameAndType col_rows_written{new ColumnUInt64, new DataTypeUInt64, "rows_written"};

	for (const auto & merge : context.getMergeList().get())
	{
		const auto bytes_read = merge.bytes_read;
		const auto rows_read = merge.rows_read;
		const auto bytes_written = merge.bytes_written;
		const auto rows_written = merge.rows_written;

		col_database.column->insert(merge.database);
		col_table.column->insert(merge.table);
		col_num_parts.column->insert(merge.num_parts);
		col_result_part_name.column->insert(merge.result_part_name);
		col_total_size_bytes.column->insert(merge.total_size_bytes);
		col_total_size_marks.column->insert(merge.total_size_marks);
		col_bytes_read.column->insert(bytes_read);
		col_rows_read.column->insert(rows_read);
		col_bytes_written.column->insert(bytes_written);
		col_rows_written.column->insert(rows_written);
	}

	block.insert(col_database);
	block.insert(col_num_parts);
	block.insert(col_table);
	block.insert(col_total_size_bytes);
	block.insert(col_result_part_name);
	block.insert(col_bytes_read);
	block.insert(col_total_size_marks);
	block.insert(col_bytes_written);
	block.insert(col_rows_read);
	block.insert(col_rows_written);

	return BlockInputStreams{1, new OneBlockInputStream{block}};
}


}
