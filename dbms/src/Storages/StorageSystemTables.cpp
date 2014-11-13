#include <DB/Columns/ColumnString.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/Storages/StorageSystemTables.h>
#include <DB/Common/VirtualColumnUtils.h>


namespace DB
{


StorageSystemTables::StorageSystemTables(const std::string & name_, const Context & context_)
	: name(name_), context(context_)
{
	columns.push_back(NameAndTypePair("database", 	new DataTypeString));
	columns.push_back(NameAndTypePair("name", 		new DataTypeString));
	columns.push_back(NameAndTypePair("engine", 	new DataTypeString));
}

StoragePtr StorageSystemTables::create(const std::string & name_, const Context & context_)
{
	return (new StorageSystemTables(name_, context_))->thisPtr();
}


BlockInputStreams StorageSystemTables::read(
	const Names & column_names, ASTPtr query, const Settings & settings,
	QueryProcessingStage::Enum & processed_stage, size_t max_block_size, unsigned threads)
{
	check(column_names);
	processed_stage = QueryProcessingStage::FetchColumns;

	Block block;

	ColumnWithNameAndType col_db;
	col_db.name = "database";
	col_db.type = new DataTypeString;
	col_db.column = new ColumnString;
	block.insert(col_db);

	ColumnWithNameAndType col_name;
	col_name.name = "name";
	col_name.type = new DataTypeString;
	col_name.column = new ColumnString;
	block.insert(col_name);

	ColumnWithNameAndType col_engine;
	col_engine.name = "engine";
	col_engine.type = new DataTypeString;
	col_engine.column = new ColumnString;
	block.insert(col_engine);

	Poco::ScopedLock<Poco::Mutex> lock(context.getMutex());

	ColumnWithNameAndType filtered_databases_column = getFilteredDatabases(query);

	for (size_t row_number = 0; row_number < filtered_databases_column.column->size(); ++row_number)
	{
		std::string database_name = filtered_databases_column.column->getDataAt(row_number).toString();
		auto database_it = context.getDatabases().find(database_name);

		if (database_it == context.getDatabases().end())
			throw DB::Exception(std::string("Fail to find database " + database_name), DB::ErrorCodes::LOGICAL_ERROR);

		for (Tables::const_iterator jt = database_it->second.begin(); jt != database_it->second.end(); ++jt)
		{
			col_db.column->insert(database_name);
			col_name.column->insert(jt->first);
			col_engine.column->insert(jt->second->getName());
		}
	}

	return BlockInputStreams(1, new OneBlockInputStream(block));
}

ColumnWithNameAndType StorageSystemTables::getFilteredDatabases(ASTPtr query)
{
	ColumnWithNameAndType filtered_databases_column;
	filtered_databases_column.name = "database";
	filtered_databases_column.type = new DataTypeString;
	filtered_databases_column.column = new ColumnString;

	Block filtered_databases_block;
	filtered_databases_block.insert(filtered_databases_column);
	for (auto database_it = context.getDatabases().begin(); database_it != context.getDatabases().end(); ++database_it)
	{
		filtered_databases_column.column->insert(database_it->first);
	}
	VirtualColumnUtils::filterBlockWithQuery(query, filtered_databases_block, context);

	return filtered_databases_column;
}

}
