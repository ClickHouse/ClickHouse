#include <DB/Storages/StorageSystemMerges.h>
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
		{ "database", new DataTypeString },
		{ "table", new DataTypeString },
		{ "elapsed", new DataTypeFloat64 },
		{ "progress", new DataTypeFloat64 },
		{ "num_parts", new DataTypeUInt64 },
		{ "result_part_name", new DataTypeString },
		{ "total_size_bytes_compressed", new DataTypeUInt64 },
		{ "total_size_marks", new DataTypeUInt64 },
		{ "bytes_read_uncompressed", new DataTypeUInt64 },
		{ "rows_read", new DataTypeUInt64 },
		{ "bytes_written_uncompressed", new DataTypeUInt64 },
		{ "rows_written", new DataTypeUInt64 }
	}
{
}

StoragePtr StorageSystemMerges::create(const std::string & name)
{
	return (new StorageSystemMerges{name})->thisPtr();
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

	ColumnWithNameAndType col_database{new ColumnString, new DataTypeString, "database"};
	ColumnWithNameAndType col_table{new ColumnString, new DataTypeString, "table"};
	ColumnWithNameAndType col_elapsed{new ColumnFloat64, new DataTypeFloat64, "elapsed"};
	ColumnWithNameAndType col_progress{new ColumnFloat64, new DataTypeFloat64, "progress"};
	ColumnWithNameAndType col_num_parts{new ColumnUInt64, new DataTypeUInt64, "num_parts"};
	ColumnWithNameAndType col_result_part_name{new ColumnString, new DataTypeString, "result_part_name"};
	ColumnWithNameAndType col_total_size_bytes_compressed{new ColumnUInt64, new DataTypeUInt64, "total_size_bytes_compressed"};
	ColumnWithNameAndType col_total_size_marks{new ColumnUInt64, new DataTypeUInt64, "total_size_marks"};
	ColumnWithNameAndType col_bytes_read_uncompressed{new ColumnUInt64, new DataTypeUInt64, "bytes_read_uncompressed"};
	ColumnWithNameAndType col_rows_read{new ColumnUInt64, new DataTypeUInt64, "rows_read"};
	ColumnWithNameAndType col_bytes_written_uncompressed{new ColumnUInt64, new DataTypeUInt64, "bytes_written_uncompressed"};
	ColumnWithNameAndType col_rows_written{new ColumnUInt64, new DataTypeUInt64, "rows_written"};

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
		col_bytes_read_uncompressed.column->insert(merge.bytes_read_uncompressed);
		col_rows_read.column->insert(merge.rows_read);
		col_bytes_written_uncompressed.column->insert(merge.bytes_written_uncompressed);
		col_rows_written.column->insert(merge.rows_written);
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

	return BlockInputStreams{1, new OneBlockInputStream{block}};
}

}
