#include <DB/Columns/ColumnString.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/Storages/StorageSystemProcesses.h>


namespace DB
{


StorageSystemProcesses::StorageSystemProcesses(const std::string & name_, const Context & context_)
	: name(name_), context(context_)
	, columns{
		{ "user", 			new DataTypeString	},
		{ "address",		new DataTypeString	},
		{ "elapsed", 		new DataTypeFloat64	},
		{ "rows_read", 		new DataTypeUInt64	},
		{ "bytes_read",		new DataTypeUInt64	},
		{ "memory_usage",	new DataTypeUInt64	},
		{ "query", 			new DataTypeString	},
		{ "query_id", 		new DataTypeString	}
	}
{
}

StoragePtr StorageSystemProcesses::create(const std::string & name_, const Context & context_)
{
	return (new StorageSystemProcesses(name_, context_))->thisPtr();
}


BlockInputStreams StorageSystemProcesses::read(
	const Names & column_names, ASTPtr query, const Settings & settings,
	QueryProcessingStage::Enum & processed_stage, size_t max_block_size, unsigned threads)
{
	check(column_names);
	processed_stage = QueryProcessingStage::FetchColumns;

	ColumnWithNameAndType col_user{new ColumnString, new DataTypeString, "user"};
	ColumnWithNameAndType col_address{new ColumnString, new DataTypeString, "address"};
	ColumnWithNameAndType col_elapsed{new ColumnFloat64, new DataTypeFloat64, "elapsed"};
	ColumnWithNameAndType col_rows_read{new ColumnUInt64, new DataTypeUInt64, "rows_read"};
	ColumnWithNameAndType col_bytes_read{new ColumnUInt64, new DataTypeUInt64, "bytes_read"};
	ColumnWithNameAndType col_memory_usage{new ColumnUInt64, new DataTypeUInt64, "memory_usage"};
	ColumnWithNameAndType col_query{new ColumnString, new DataTypeString, "query"};
	ColumnWithNameAndType col_query_id{new ColumnString, new DataTypeString, "query_id"};

	for (const auto & process : context.getProcessList().get())
	{
		const size_t rows_read = process.rows_processed;
		const size_t bytes_read = process.bytes_processed;

		col_user.column->insert(process.user);
		col_address.column->insert(process.ip_address.toString());
		col_elapsed.column->insert(process.watch.elapsedSeconds());
		col_rows_read.column->insert(rows_read);
		col_bytes_read.column->insert(bytes_read);
		col_memory_usage.column->insert(static_cast<UInt64>(process.memory_tracker.get()));
		col_query.column->insert(process.query);
		col_query_id.column->insert(process.query_id);
	}

	Block block{
		col_user,
		col_address,
		col_elapsed,
		col_rows_read,
		col_bytes_read,
		col_memory_usage,
		col_query,
		col_query_id
	};

	return BlockInputStreams(1, new OneBlockInputStream(block));
}


}
