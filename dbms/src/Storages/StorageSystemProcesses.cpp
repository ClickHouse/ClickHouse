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
	columns.push_back(NameAndTypePair("elapsed", 	new DataTypeFloat64));
	columns.push_back(NameAndTypePair("query", 		new DataTypeString));
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
	
	ColumnWithNameAndType col_elapsed;
	col_elapsed.name = "elapsed";
	col_elapsed.type = new DataTypeFloat64;
	col_elapsed.column = new ColumnFloat64;
	block.insert(col_elapsed);

	ColumnWithNameAndType col_query;
	col_query.name = "query";
	col_query.type = new DataTypeString;
	col_query.column = new ColumnString;
	block.insert(col_query);

	ProcessList::Containter list = context.getProcessList().get();
	
	for (ProcessList::Containter::const_iterator it = list.begin(); it != list.end(); ++it)
	{
		col_elapsed.column->insert(it->watch.elapsedSeconds());
		col_query.column->insert(it->query);
	}
	
	return BlockInputStreams(1, new OneBlockInputStream(block));
}


}
