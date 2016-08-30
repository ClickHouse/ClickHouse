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
		{ "user", 			std::make_shared<DataTypeString>()	},
		{ "address",		std::make_shared<DataTypeString>()	},
		{ "elapsed", 		std::make_shared<DataTypeFloat64>()	},
		{ "rows_read", 		std::make_shared<DataTypeUInt64>()	},
		{ "bytes_read",		std::make_shared<DataTypeUInt64>()	},
		{ "total_rows_approx", std::make_shared<DataTypeUInt64>() },
		{ "memory_usage",	std::make_shared<DataTypeUInt64>()	},
		{ "query", 			std::make_shared<DataTypeString>()	},
		{ "query_id", 		std::make_shared<DataTypeString>()	}
	}
{
}

StoragePtr StorageSystemProcesses::create(const std::string & name_)
{
	return make_shared(name_);
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

	ColumnWithTypeAndName col_user{std::make_shared<ColumnString>(), std::make_shared<DataTypeString>(), "user"};
	ColumnWithTypeAndName col_address{std::make_shared<ColumnString>(), std::make_shared<DataTypeString>(), "address"};
	ColumnWithTypeAndName col_elapsed{std::make_shared<ColumnFloat64>(), std::make_shared<DataTypeFloat64>(), "elapsed"};
	ColumnWithTypeAndName col_rows_read{std::make_shared<ColumnUInt64>(), std::make_shared<DataTypeUInt64>(), "rows_read"};
	ColumnWithTypeAndName col_bytes_read{std::make_shared<ColumnUInt64>(), std::make_shared<DataTypeUInt64>(), "bytes_read"};
	ColumnWithTypeAndName col_total_rows_approx{std::make_shared<ColumnUInt64>(), std::make_shared<DataTypeUInt64>(), "total_rows_approx"};
	ColumnWithTypeAndName col_memory_usage{std::make_shared<ColumnUInt64>(), std::make_shared<DataTypeUInt64>(), "memory_usage"};
	ColumnWithTypeAndName col_query{std::make_shared<ColumnString>(), std::make_shared<DataTypeString>(), "query"};
	ColumnWithTypeAndName col_query_id{std::make_shared<ColumnString>(), std::make_shared<DataTypeString>(), "query_id"};

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

	return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(block));
}


}
