#include <DB/DataStreams/AddingConstColumnBlockInputStream.h>
#include <DB/DataStreams/narrowBlockInputStreams.h>
#include <DB/Storages/StorageMerge.h>
#include <DB/Common/VirtualColumnUtils.h>
#include <DB/Interpreters/InterpreterAlterQuery.h>
#include <DB/Interpreters/ExpressionActions.h>
#include <DB/Storages/VirtualColumnFactory.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Databases/IDatabase.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int ILLEGAL_PREWHERE;
}


StorageMerge::StorageMerge(
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const String & source_database_,
	const String & table_name_regexp_,
	const Context & context_)
	: name(name_), columns(columns_), source_database(source_database_),
	  table_name_regexp(table_name_regexp_), context(context_)
{
}

StorageMerge::StorageMerge(
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const NamesAndTypesList & materialized_columns_,
	const NamesAndTypesList & alias_columns_,
	const ColumnDefaults & column_defaults_,
	const String & source_database_,
	const String & table_name_regexp_,
	const Context & context_)
	: IStorage{materialized_columns_, alias_columns_, column_defaults_},
	name(name_), columns(columns_), source_database(source_database_),
	table_name_regexp(table_name_regexp_), context(context_)
{
}

StoragePtr StorageMerge::create(
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const String & source_database_,
	const String & table_name_regexp_,
	const Context & context_)
{
	return (new StorageMerge{
		name_, columns_,
		source_database_, table_name_regexp_, context_
	})->thisPtr();
}

StoragePtr StorageMerge::create(
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const NamesAndTypesList & materialized_columns_,
	const NamesAndTypesList & alias_columns_,
	const ColumnDefaults & column_defaults_,
	const String & source_database_,
	const String & table_name_regexp_,
	const Context & context_)
{
	return (new StorageMerge{
		name_, columns_, materialized_columns_, alias_columns_, column_defaults_,
		source_database_, table_name_regexp_, context_
	})->thisPtr();
}

NameAndTypePair StorageMerge::getColumn(const String & column_name) const
{
	auto type = VirtualColumnFactory::tryGetType(column_name);
	if (type)
		return NameAndTypePair(column_name, type);

	return IStorage::getColumn(column_name);
}

bool StorageMerge::hasColumn(const String & column_name) const
{
	return VirtualColumnFactory::hasColumn(column_name) || IStorage::hasColumn(column_name);
}

BlockInputStreams StorageMerge::read(
	const Names & column_names,
	ASTPtr query,
	const Context & context,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	const size_t max_block_size,
	const unsigned threads)
{
	BlockInputStreams res;

	Names virt_column_names, real_column_names;
	for (const auto & it : column_names)
		if (it != "_table")
			real_column_names.push_back(it);
		else
			virt_column_names.push_back(it);

	StorageVector selected_tables;

	/// Среди всех стадий, до которых обрабатывается запрос в таблицах-источниках, выберем минимальную.
	processed_stage = QueryProcessingStage::Complete;
	QueryProcessingStage::Enum tmp_processed_stage = QueryProcessingStage::Complete;

	/** Сначала составим список выбранных таблиц, чтобы узнать его размер.
	  * Это нужно, чтобы правильно передать в каждую таблицу рекомендацию по количеству потоков.
	  */
	getSelectedTables(selected_tables);

	/// Если в запросе используется PREWHERE, надо убедиться, что все таблицы это поддерживают.
	if (typeid_cast<const ASTSelectQuery &>(*query).prewhere_expression)
		for (const auto & table : selected_tables)
			if (!table->supportsPrewhere())
				throw Exception("Storage " + table->getName() + " doesn't support PREWHERE.", ErrorCodes::ILLEGAL_PREWHERE);

	TableLocks table_locks;

	/// Нельзя, чтобы эти таблицы кто-нибудь удалил, пока мы их читаем.
	for (auto & table : selected_tables)
		table_locks.push_back(table->lockStructure(false));

	Block virtual_columns_block = getBlockWithVirtualColumns(selected_tables);

	/// Если запрошен хотя бы один виртуальный столбец, пробуем индексировать
	if (!virt_column_names.empty())
		VirtualColumnUtils::filterBlockWithQuery(query, virtual_columns_block, context);

	std::multiset<String> values = VirtualColumnUtils::extractSingleValueFromBlock<String>(virtual_columns_block, "_table");

	/** На всякий случай отключаем оптимизацию "перенос в PREWHERE",
	  * так как нет уверенности, что она работает, когда одна из таблиц MergeTree, а другая - нет.
	  */
	Settings modified_settings = settings;
	modified_settings.optimize_move_to_prewhere = false;

	for (size_t i = 0, size = selected_tables.size(); i < size; ++i)
	{
		StoragePtr table = selected_tables[i];
		auto & table_lock = table_locks[i];

		if (values.find(table->getTableName()) == values.end())
			continue;

		/// Если в запросе только виртуальные столбцы, надо запросить хотя бы один любой другой.
		if (real_column_names.size() == 0)
			real_column_names.push_back(ExpressionActions::getSmallestColumn(table->getColumnsList()));

		/// Подменяем виртуальный столбец на его значение
		ASTPtr modified_query_ast = query->clone();
		VirtualColumnUtils::rewriteEntityInAst(modified_query_ast, "_table", table->getTableName());

		BlockInputStreams source_streams = table->read(
			real_column_names,
			modified_query_ast,
			context,
			modified_settings,
			tmp_processed_stage,
			max_block_size,
			size > threads ? 1 : (threads / size));

		for (auto & stream : source_streams)
			stream->addTableLock(table_lock);

		for (auto & virtual_column : virt_column_names)
		{
			if (virtual_column == "_table")
			{
				for (auto & stream : source_streams)
					stream = new AddingConstColumnBlockInputStream<String>(stream, new DataTypeString, table->getTableName(), "_table");
			}
		}

		res.insert(res.end(), source_streams.begin(), source_streams.end());

		if (tmp_processed_stage < processed_stage)
			processed_stage = tmp_processed_stage;
	}

	return narrowBlockInputStreams(res, threads);
}

/// Построить блок состоящий только из возможных значений виртуальных столбцов
Block StorageMerge::getBlockWithVirtualColumns(const std::vector<StoragePtr> & selected_tables) const
{
	Block res;
	ColumnWithTypeAndName _table(new ColumnString, new DataTypeString, "_table");

	for (StorageVector::const_iterator it = selected_tables.begin(); it != selected_tables.end(); ++it)
		_table.column->insert((*it)->getTableName());

	res.insert(_table);
	return res;
}

void StorageMerge::getSelectedTables(StorageVector & selected_tables) const
{
	auto database = context.getDatabase(source_database);
	auto iterator = database->getIterator();

	while (iterator->isValid())
	{
		if (table_name_regexp.match(iterator->name()))
		{
			auto & table = iterator->table();
			if (table.get() != this)
				selected_tables.emplace_back(table);
		}

		iterator->next();
	}
}


void StorageMerge::alter(const AlterCommands & params, const String & database_name, const String & table_name, const Context & context)
{
	auto lock = lockStructureForAlter();
	params.apply(*columns, materialized_columns, alias_columns, column_defaults);
	InterpreterAlterQuery::updateMetadata(database_name, table_name, *columns,
		materialized_columns, alias_columns, column_defaults, context);
}

}
