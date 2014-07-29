#include <DB/Columns/ColumnString.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/Storages/StorageSystemParts.h>
#include <DB/Common/VirtualColumnUtils.h>


namespace DB
{


StorageSystemParts::StorageSystemParts(const std::string & name_, const Context & context_)
	: name(name_), context(context_)
{
	columns.push_back(NameAndTypePair("name", 				new DataTypeString));
	columns.push_back(NameAndTypePair("replicated",			new DataTypeUInt8));
	columns.push_back(NameAndTypePair("active",				new DataTypeUInt8));
	columns.push_back(NameAndTypePair("marks",				new DataTypeUInt64));
	columns.push_back(NameAndTypePair("bytes",				new DataTypeUInt64));
	columns.push_back(NameAndTypePair("modification_time",	new DataTypeDateTime));
	columns.push_back(NameAndTypePair("remove_time",		new DataTypeDateTime));

	columns.push_back(NameAndTypePair("database", 			new DataTypeString));
	columns.push_back(NameAndTypePair("table", 				new DataTypeString));
	columns.push_back(NameAndTypePair("engine", 			new DataTypeString));
}

StoragePtr StorageSystemParts::create(const std::string & name_, const Context & context_)
{
	return (new StorageSystemParts(name_, context_))->thisPtr();
}


BlockInputStreams StorageSystemParts::read(
	const Names & column_names, ASTPtr query, const Settings & settings,
	QueryProcessingStage::Enum & processed_stage, size_t max_block_size, unsigned threads)
{
	check(column_names);
	processed_stage = QueryProcessingStage::FetchColumns;

	/// Будем поочередно применять WHERE к подмножеству столбцов и добавлять столбцы.

	Block block;

	{
		Poco::ScopedLock<Poco::Mutex> lock(context.getMutex());

		const Databases & databases = context.getDatabases();

		ColumnPtr database_column = new ColumnString;
		for (const auto & database : databases)
			database_column->insert(database.first);
		block.insert(ColumnWithNameAndType(database_column, new DataTypeString, "database"));

		auto filtered_databases = VirtualColumnUtils::getVirtualColumnsBlocks(query->clone(), block, data.context);



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

	for (Databases::const_iterator it = context.getDatabases().begin(); it != context.getDatabases().end(); ++it)
	{
		for (Tables::const_iterator jt = it->second.begin(); jt != it->second.end(); ++jt)
		{
			col_db.column->insert(it->first);
			col_name.column->insert(jt->first);
			col_engine.column->insert(jt->second->getName());
		}
	}

	return BlockInputStreams(1, new OneBlockInputStream(block));
}


}
