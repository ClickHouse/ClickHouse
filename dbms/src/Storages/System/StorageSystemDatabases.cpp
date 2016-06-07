#include <DB/Columns/ColumnString.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/Databases/IDatabase.h>
#include <DB/Storages/System/StorageSystemDatabases.h>


namespace DB
{


StorageSystemDatabases::StorageSystemDatabases(const std::string & name_)
	: name(name_),
	columns
	{
		{"name", 	new DataTypeString},
		{"engine", 	new DataTypeString},
	}
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

	ColumnWithTypeAndName col_name{new ColumnString, new DataTypeString, "name"};
	block.insert(col_name);

	ColumnWithTypeAndName col_engine{new ColumnString, new DataTypeString, "engine"};
	block.insert(col_engine);

	auto databases = context.getDatabases();
	for (const auto & database : databases)
	{
		col_name.column->insert(database.first);
		col_engine.column->insert(database.second->getEngineName());
	}

	return BlockInputStreams(1, new OneBlockInputStream(block));
}


}
