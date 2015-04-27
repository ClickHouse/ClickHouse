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

	Block block;

	std::map<std::pair<std::string, std::string>, StoragePtr> storages;

	{
		Poco::ScopedLock<Poco::Mutex> lock(context.getMutex());

		const Databases & databases = context.getDatabases();

		/// Добавляем столбец database.
		ColumnPtr database_column = new ColumnString;
		for (const auto & database : databases)
			database_column->insert(database.first);
		block.insert(ColumnWithNameAndType(database_column, new DataTypeString, "database"));

		/// Отфильтруем блок со столбцом database.
		VirtualColumnUtils::filterBlockWithQuery(query, block, context);

		if (!block.rows())
			return BlockInputStreams();

		database_column = block.getByName("database").column;
		size_t rows = database_column->size();

		/// Добавляем столбец table.
		ColumnPtr table_column = new ColumnString;
		IColumn::Offsets_t offsets(rows);
		for (size_t i = 0; i < rows; ++i)
		{
			const std::string database_name = (*database_column)[i].get<std::string>();
			const Tables & tables = databases.at(database_name);
			offsets[i] = i ? offsets[i - 1] : 0;

			for (const auto & table : tables)
			{
				storages.insert(std::make_pair(std::make_pair(database_name, table.first), table.second));
				table_column->insert(table.first);
				offsets[i] += 1;
			}
		}

		for (size_t i = 0; i < block.columns(); ++i)
		{
			ColumnPtr & column = block.getByPosition(i).column;
			column = column->replicate(offsets);
		}

		block.insert(ColumnWithNameAndType(table_column, new DataTypeString, "table"));
	}

	/// Отфильтруем блок со столбцами database и table.
	VirtualColumnUtils::filterBlockWithQuery(query, block, context);

	if (!block.rows())
		return BlockInputStreams();

	ColumnPtr filtered_database_column = block.getByName("database").column;
	ColumnPtr filtered_table_column = block.getByName("table").column;

	/// Составляем результат.
	ColumnPtr database_column = new ColumnString;
	ColumnPtr table_column = new ColumnString;
	ColumnPtr name_column = new ColumnString;
	ColumnPtr type_column = new ColumnString;
	ColumnPtr default_type_column = new ColumnString;
	ColumnPtr default_expression_column = new ColumnString;

	size_t rows = filtered_database_column->size();
	for (size_t i = 0; i < rows; ++i)
	{
		const std::string database_name = (*filtered_database_column)[i].get<std::string>();
		const std::string table_name = (*filtered_table_column)[i].get<std::string>();

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
			database_column->insert(database_name);
			table_column->insert(table_name);
			name_column->insert(column.name);
			type_column->insert(column.type->getName());

			const auto it = column_defaults.find(column.name);
			if (it == std::end(column_defaults))
			{
				default_type_column->insertDefault();
				default_expression_column->insertDefault();
			}
			else
			{
				default_type_column->insert(toString(it->second.type));
				default_expression_column->insert(queryToString(it->second.expression));
			}
		}
	}

	block.clear();

	block.insert(ColumnWithNameAndType(database_column, new DataTypeString, "database"));
	block.insert(ColumnWithNameAndType(table_column, new DataTypeString, "table"));
	block.insert(ColumnWithNameAndType(name_column, new DataTypeString, "name"));
	block.insert(ColumnWithNameAndType(type_column, new DataTypeString, "type"));
	block.insert(ColumnWithNameAndType(default_type_column, new DataTypeString, "default_type"));
	block.insert(ColumnWithNameAndType(default_expression_column, new DataTypeString, "default_expression"));

	return BlockInputStreams{ 1, new OneBlockInputStream(block) };
}

}
