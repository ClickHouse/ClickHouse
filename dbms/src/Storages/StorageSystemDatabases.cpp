#include <DB/Columns/ColumnString.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/Storages/StorageSystemDatabases.h>


namespace DB
{


StorageSystemDatabases::StorageSystemDatabases(const std::string & name_)
	: name(name_), columns{{"name", new DataTypeString}}
{
}

StoragePtr StorageSystemDatabases::create(const std::string & name_)
{
	return (new StorageSystemDatabases(name_))->thisPtr();
}


BlockInputStreams StorageSystemDatabases::read(
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

	ColumnWithTypeAndName col_name;
	col_name.name = "name";
	col_name.type = new DataTypeString;
	col_name.column = new ColumnString;
	block.insert(col_name);

	Poco::ScopedLock<Poco::Mutex> lock(context.getMutex());

	for (Databases::const_iterator it = context.getDatabases().begin(); it != context.getDatabases().end(); ++it)
		col_name.column->insert(it->first);

	return BlockInputStreams(1, new OneBlockInputStream(block));
}


}
