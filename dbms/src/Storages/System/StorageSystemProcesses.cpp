#include <DB/Columns/ColumnString.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/Interpreters/ProcessList.h>
#include <DB/Storages/System/StorageSystemProcesses.h>


namespace DB
{


StorageSystemProcesses::StorageSystemProcesses(const std::string & name_)
	: name(name_)
	, columns{
		{ "user", 			new DataTypeString	},
		{ "address",		new DataTypeString	},
		{ "elapsed", 		new DataTypeFloat64	},
		{ "rows_read", 		new DataTypeUInt64	},
		{ "bytes_read",		new DataTypeUInt64	},
		{ "total_rows_approx", new DataTypeUInt64 },
		{ "memory_usage",	new DataTypeUInt64	},
		{ "query", 			new DataTypeString	},
		{ "query_id", 		new DataTypeString	}
	}
{
}

StoragePtr StorageSystemProcesses::create(const std::string & name_)
{
	return (new StorageSystemProcesses(name_))->thisPtr();
}


BlockInputStreams StorageSystemProcesses::read(
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

	ColumnWithTypeAndName col_user{new ColumnString, new DataTypeString, "user"};
	ColumnWithTypeAndName col_address{new ColumnString, new DataTypeString, "address"};
	ColumnWithTypeAndName col_elapsed{new ColumnFloat64, new DataTypeFloat64, "elapsed"};
	ColumnWithTypeAndName col_rows_read{new ColumnUInt64, new DataTypeUInt64, "rows_read"};
	ColumnWithTypeAndName col_bytes_read{new ColumnUInt64, new DataTypeUInt64, "bytes_read"};
	ColumnWithTypeAndName col_total_rows_approx{new ColumnUInt64, new DataTypeUInt64, "total_rows_approx"};
	ColumnWithTypeAndName col_memory_usage{new ColumnUInt64, new DataTypeUInt64, "memory_usage"};
	ColumnWithTypeAndName col_query{new ColumnString, new DataTypeString, "query"};
	ColumnWithTypeAndName col_query_id{new ColumnString, new DataTypeString, "query_id"};

	ProcessList::Info info = context.getProcessList().getInfo();

	for (const auto & process : info)
	{
		col_user.column->insert(process.user);
		col_address.column->insert(process.ip_address.toString());
		col_elapsed.column->insert(process.elapsed_seconds);
		col_rows_read.column->insert(process.rows);
		col_bytes_read.column->insert(process.bytes);
		col_total_rows_approx.column->insert(process.total_rows);
		col_memory_usage.column->insert(static_cast<UInt64>(process.memory_usage));
		col_query.column->insert(process.query);
		col_query_id.column->insert(process.query_id);
	}

	Block block{
		col_user,
		col_address,
		col_elapsed,
		col_rows_read,
		col_bytes_read,
		col_total_rows_approx,
		col_memory_usage,
		col_query,
		col_query_id
	};

	return BlockInputStreams(1, new OneBlockInputStream(block));
}


}
