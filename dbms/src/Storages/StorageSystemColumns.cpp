#include <DB/Storages/StorageSystemColumns.h>
#include <DB/Columns/ColumnString.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/Common/VirtualColumnUtils.h>

namespace DB
{

StorageSystemColumns::StorageSystemColumns(const std::string & name_)
	: name(name_)
	, columns{
		{ "database",           new DataTypeString },
		{ "table",              new DataTypeString },
		{ "name",               new DataTypeString },
		{ "type",               new DataTypeString },
		{ "default_type",       new DataTypeString },
		{ "default_expression", new DataTypeString }
	}
{
}

StoragePtr StorageSystemColumns::create(const std::string & name_)
{
	return (new StorageSystemColumns{name_})->thisPtr();
}

BlockInputStreams StorageSystemColumns::read(
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

	ColumnWithNameAndType column_database{ new ColumnString, new DataTypeString, "database" };
	ColumnWithNameAndType column_table{ new ColumnString, new DataTypeString, "table" };
	ColumnWithNameAndType column_name{ new ColumnString, new DataTypeString, "name" };
	ColumnWithNameAndType column_type{ new ColumnString, new DataTypeString, "type" };
	ColumnWithNameAndType column_default_type{ new ColumnString, new DataTypeString, "default_type" };
	ColumnWithNameAndType column_default_expression{ new ColumnString, new DataTypeString, "default_expression" };

	std::map<std::pair<std::string, std::string>, StoragePtr> storages;

	{
		Poco::ScopedLock<Poco::Mutex> lock(context.getMutex());

		const Databases & databases = context.getDatabases();

		for (const auto & database : databases)
		{
			const Tables & tables = database.second;
			for (const auto & table : tables)
			{
				column_database.column->insert(database.first);
				column_table.column->insert(table.first);
				storages.insert(std::make_pair(std::make_pair(database.first, table.first), table.second));
			}
		}
	}

	{
		Block filtered_block{ column_database, column_table };
		VirtualColumnUtils::filterBlockWithQuery(query, filtered_block, context);

		column_database = filtered_block.getByName("database");
		column_table = filtered_block.getByName("table");
	}

	for (size_t i = 0; i < column_database.column->size(); ++i)
	{
		const auto & database_name = (*column_database.column)[i].safeGet<const std::string &>();
		const auto & table_name = (*column_table.column)[i].safeGet<const std::string &>();

		NamesAndTypesList columns;
		ColumnDefaults column_defaults;

		{
			StoragePtr storage = storages.at(std::make_pair(database_name, table_name));
			auto table_lock = storage->lockStructure(false);

			columns = storage->getColumnsList();
			columns.insert(std::end(columns), std::begin(storage->alias_columns), std::end(storage->alias_columns));
			column_defaults = storage->column_defaults;
		}

		for (const auto & column : columns)
		{
			column_name.column->insert(column.name);
			column_type.column->insert(column.type->getName());

			const auto it = column_defaults.find(column.name);
			if (it == std::end(column_defaults))
			{
				column_default_type.column->insertDefault();
				column_default_expression.column->insertDefault();
			}
			else
			{
				column_default_type.column->insert(toString(it->second.type));
				column_default_expression.column->insert(queryToString(it->second.expression));
			}
		}
	}

	return BlockInputStreams{ 1, new OneBlockInputStream{
			{ column_database, column_table, column_name, column_type, column_default_type, column_default_expression }
		}
	};
}

}
