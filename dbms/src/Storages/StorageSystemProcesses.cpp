#include <DB/Columns/ColumnString.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/Storages/StorageSystemProcesses.h>


namespace DB
{


StorageSystemProcesses::StorageSystemProcesses(const std::string & name_, const Context & context_)
	: name(name_), context(context_)
{
	columns.push_back(NameAndTypePair("user", 			new DataTypeString));
	columns.push_back(NameAndTypePair("address",		new DataTypeString));
	columns.push_back(NameAndTypePair("elapsed", 		new DataTypeFloat64));
	columns.push_back(NameAndTypePair("rows_read", 		new DataTypeUInt64));
	columns.push_back(NameAndTypePair("bytes_read",		new DataTypeUInt64));
	columns.push_back(NameAndTypePair("memory_usage",	new DataTypeUInt64));
	columns.push_back(NameAndTypePair("query", 			new DataTypeString));
	columns.push_back(NameAndTypePair("query_id", 		new DataTypeString));
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

	Block block;

	ColumnWithNameAndType col_user;
	col_user.name = "user";
	col_user.type = new DataTypeString;
	col_user.column = new ColumnString;
	block.insert(col_user);

	ColumnWithNameAndType col_address;
	col_address.name = "address";
	col_address.type = new DataTypeString;
	col_address.column = new ColumnString;
	block.insert(col_address);
	
	ColumnWithNameAndType col_elapsed;
	col_elapsed.name = "elapsed";
	col_elapsed.type = new DataTypeFloat64;
	col_elapsed.column = new ColumnFloat64;
	block.insert(col_elapsed);

	ColumnWithNameAndType col_rows_read;
	col_rows_read.name = "rows_read";
	col_rows_read.type = new DataTypeUInt64;
	col_rows_read.column = new ColumnUInt64;
	block.insert(col_rows_read);

	ColumnWithNameAndType col_bytes_read;
	col_bytes_read.name = "bytes_read";
	col_bytes_read.type = new DataTypeUInt64;
	col_bytes_read.column = new ColumnUInt64;
	block.insert(col_bytes_read);

	ColumnWithNameAndType col_memory_usage;
	col_memory_usage.name = "memory_usage";
	col_memory_usage.type = new DataTypeUInt64;
	col_memory_usage.column = new ColumnUInt64;
	block.insert(col_memory_usage);

	ColumnWithNameAndType col_query;
	col_query.name = "query";
	col_query.type = new DataTypeString;
	col_query.column = new ColumnString;
	block.insert(col_query);

	ColumnWithNameAndType col_query_id;
	col_query_id.name = "query_id";
	col_query_id.type = new DataTypeString;
	col_query_id.column = new ColumnString;
	block.insert(col_query_id);

	ProcessList::Containter list = context.getProcessList().get();
	
	for (ProcessList::Containter::const_iterator it = list.begin(); it != list.end(); ++it)
	{
		size_t rows_read = it->rows_processed;
		size_t bytes_read = it->bytes_processed;
		
		col_user.column->insert(it->user);
		col_address.column->insert(it->ip_address.toString());
		col_elapsed.column->insert(it->watch.elapsedSeconds());
		col_rows_read.column->insert(rows_read);
		col_bytes_read.column->insert(bytes_read);
		col_memory_usage.column->insert(UInt64(it->memory_tracker.get()));
		col_query.column->insert(it->query);
		col_query_id.column->insert(it->query_id);
	}
	
	return BlockInputStreams(1, new OneBlockInputStream(block));
}


}
