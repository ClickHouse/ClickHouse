#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnsNumber.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/Storages/System/StorageSystemTables.h>
#include <DB/Common/VirtualColumnUtils.h>
#include <DB/Databases/IDatabase.h>
#include <DB/Interpreters/Context.h>


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
		{"database", 					std::make_shared<DataTypeString>()},
		{"name", 						std::make_shared<DataTypeString>()},
		{"engine",						std::make_shared<DataTypeString>()},
		{"metadata_modification_time",	std::make_shared<DataTypeDateTime>()}
	}
{
}

StoragePtr StorageSystemTables::create(const std::string & name_)
{
	return make_shared(name_);
}


static ColumnWithTypeAndName getFilteredDatabases(ASTPtr query, const Context & context)
{
	ColumnWithTypeAndName column;
	column.name = "database";
	column.type = std::make_shared<DataTypeString>();
	column.column = std::make_shared<ColumnString>();

	for (const auto & db : context.getDatabases())
		column.column->insert(db.first);

	Block block;
	block.insert(std::move(column));

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
	col_db.type = std::make_shared<DataTypeString>();
	col_db.column = std::make_shared<ColumnString>();
	block.insert(col_db);

	ColumnWithTypeAndName col_name;
	col_name.name = "name";
	col_name.type = std::make_shared<DataTypeString>();
	col_name.column = std::make_shared<ColumnString>();
	block.insert(col_name);

	ColumnWithTypeAndName col_engine;
	col_engine.name = "engine";
	col_engine.type = std::make_shared<DataTypeString>();
	col_engine.column = std::make_shared<ColumnString>();
	block.insert(col_engine);

	ColumnWithTypeAndName col_meta_mod_time;
	col_meta_mod_time.name = "metadata_modification_time";
	col_meta_mod_time.type = std::make_shared<DataTypeDateTime>();
	col_meta_mod_time.column = std::make_shared<ColumnUInt32>();
	block.insert(col_meta_mod_time);

	ColumnWithTypeAndName filtered_databases_column = getFilteredDatabases(query, context);

	for (size_t row_number = 0; row_number < filtered_databases_column.column->size(); ++row_number)
	{
		std::string database_name = filtered_databases_column.column->getDataAt(row_number).toString();
		auto database = context.tryGetDatabase(database_name);

		if (!database)
		{
			/// Database was deleted just now.
			continue;
		}

		for (auto iterator = database->getIterator(); iterator->isValid(); iterator->next())
		{
			auto table_name = iterator->name();
			col_db.column->insert(database_name);
			col_name.column->insert(table_name);
			col_engine.column->insert(iterator->table()->getName());
			col_meta_mod_time.column->insert(static_cast<UInt64>(database->getTableMetadataModificationTime(table_name)));
		}
	}

	return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(block));
}

}
