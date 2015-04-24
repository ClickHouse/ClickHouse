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

	{
		Poco::ScopedLock<Poco::Mutex> lock(context.getMutex());

		const Databases & databases = context.getDatabases();
		for (const auto & database : databases)
		{
			const Tables & tables = database.second;
			for (const auto & table : tables)
			{
				NamesAndTypesList columns;
				ColumnDefaults column_defaults;

				{
					StoragePtr storage = table.second;
					storage->lockStructure(false);

					columns = storage->getColumnsList();
					columns.insert(std::end(columns), std::begin(storage->alias_columns), std::end(storage->alias_columns));
					column_defaults = storage->column_defaults;
				}

				for (const auto & column : columns)
				{
					column_database.column->insert(database.first);
					column_table.column->insert(table.first);
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
		}
	}

	Block filtered_block{ column_database, column_table, column_name, column_type, column_default_type, column_default_expression };
	VirtualColumnUtils::filterBlockWithQuery(query, filtered_block, context);

	column_database = filtered_block.getByName("database");
	column_table = filtered_block.getByName("table");
	column_name = filtered_block.getByName("name");
	column_type = filtered_block.getByName("type");
	column_default_type = filtered_block.getByName("default_type");
	column_default_expression = filtered_block.getByName("default_expression");

	return BlockInputStreams{ 1, new OneBlockInputStream{
			{ column_database, column_table, column_name, column_type, column_default_type, column_default_expression }
		}
	};
}

}
