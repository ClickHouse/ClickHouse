#include <DB/Columns/ColumnString.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/Storages/System/StorageSystemTables.h>
#include <DB/Common/VirtualColumnUtils.h>
#include <DB/Databases/IDatabase.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int LOGICAL_ERROR;
}


StorageSystemTables::StorageSystemTables(const std::string & name_)
	: name(name_),
	columns
	{
		{"database", 	new DataTypeString},
		{"name", 		new DataTypeString},
		{"engine", 		new DataTypeString},
	}
{
}

StoragePtr StorageSystemTables::create(const std::string & name_)
{
	return (new StorageSystemTables(name_))->thisPtr();
}


static ColumnWithTypeAndName getFilteredDatabases(ASTPtr query, const Context & context)
{
	ColumnWithTypeAndName column;
	column.name = "database";
	column.type = new DataTypeString;
	column.column = new ColumnString;

	Block block;
	block.insert(column);
	for (const auto & db : context.getDatabases())
		column.column->insert(db.first);

	VirtualColumnUtils::filterBlockWithQuery(query, block, context);

	return block.getByPosition(0);
}


BlockInputStreams StorageSystemTables::read(
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

	ColumnWithTypeAndName col_db;
	col_db.name = "database";
	col_db.type = new DataTypeString;
	col_db.column = new ColumnString;
	block.insert(col_db);

	ColumnWithTypeAndName col_name;
	col_name.name = "name";
	col_name.type = new DataTypeString;
	col_name.column = new ColumnString;
	block.insert(col_name);

	ColumnWithTypeAndName col_engine;
	col_engine.name = "engine";
	col_engine.type = new DataTypeString;
	col_engine.column = new ColumnString;
	block.insert(col_engine);

	ColumnWithTypeAndName filtered_databases_column = getFilteredDatabases(query, context);

	for (size_t row_number = 0; row_number < filtered_databases_column.column->size(); ++row_number)
	{
		std::string database_name = filtered_databases_column.column->getDataAt(row_number).toString();
		auto database = context.tryGetDatabase(database_name);

		if (!database)
		{
			/// Базу данных только что удалили.
			continue;
		}

		for (auto iterator = database->getIterator(); iterator->isValid(); iterator->next())
		{
			col_db.column->insert(database_name);
			col_name.column->insert(iterator->name());
			col_engine.column->insert(iterator->table()->getName());
		}
	}

	return BlockInputStreams(1, new OneBlockInputStream(block));
}

}
