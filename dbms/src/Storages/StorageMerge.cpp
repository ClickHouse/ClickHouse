#include <DB/DataStreams/narrowBlockInputStreams.h>
#include <DB/Storages/StorageMerge.h>
#include <DB/Common/VirtualColumnUtils.h>

namespace DB
{

StorageMerge::StorageMerge(
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const String & source_database_,
	const String & table_name_regexp_,
	const Context & context_)
	: name(name_), columns(columns_), source_database(source_database_), table_name_regexp(table_name_regexp_), context(context_)
{
	_table_column_name = "_table" + VirtualColumnUtils::chooseSuffix(getColumnsList(), "_table");
}

StoragePtr StorageMerge::create(
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const String & source_database_,
	const String & table_name_regexp_,
	const Context & context_)
{
	return (new StorageMerge(name_, columns_, source_database_, table_name_regexp_, context_))->thisPtr();
}

NameAndTypePair StorageMerge::getColumn(const String &column_name) const
{
	if (column_name == _table_column_name) return std::make_pair(_table_column_name, new DataTypeString);
	return getRealColumn(column_name);
}

bool StorageMerge::hasColumn(const String &column_name) const
{
	if (column_name == _table_column_name) return true;
	return hasRealColumn(column_name);
}

BlockInputStreams StorageMerge::read(
	const Names & column_names,
	ASTPtr query,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	size_t max_block_size,
	unsigned threads)
{
	BlockInputStreams res;

	Names virt_column_names, real_column_names;
	for (const auto & it : column_names)
		if (it != _table_column_name)
			real_column_names.push_back(it);
		else
			virt_column_names.push_back(it);

	SelectedTables selected_tables;

	/// Среди всех стадий, до которых обрабатывается запрос в таблицах-источниках, выберем минимальную.
	processed_stage = QueryProcessingStage::Complete;
	QueryProcessingStage::Enum tmp_processed_stage = QueryProcessingStage::Complete;

	/// Список таблиц могут менять в другом потоке.
	{
		Poco::ScopedLock<Poco::Mutex> lock(context.getMutex());
		context.assertDatabaseExists(source_database);
		
		/** Сначала составим список выбранных таблиц, чтобы узнать его размер.
		  * Это нужно, чтобы правильно передать в каждую таблицу рекомендацию по количеству потоков.
		  */
		getSelectedTables(selected_tables);
	}

	Block virtual_columns_block = getBlockWithVirtualColumns(selected_tables);
	BlockInputStreamPtr virtual_columns;

	/// Если запрошен хотя бы один виртуальный столбец, пробуем индексировать
	if (!virt_column_names.empty())
		virtual_columns = VirtualColumnUtils::getVirtualColumnsBlocks(query->clone(), virtual_columns_block, context);
	else /// Иначе, считаем допустимыми все возможные значения
		virtual_columns = new OneBlockInputStream(virtual_columns_block);

	std::multiset<String> values = VirtualColumnUtils::extractSingleValueFromBlocks<String>(virtual_columns, _table_column_name);
	bool all_inclusive = (values.size() == virtual_columns_block.rows());

	for (SelectedTables::iterator it = selected_tables.begin(); it != selected_tables.end(); ++it)
	{
		if (!all_inclusive && values.find((*it)->getTableName()) == values.end())
			continue;

		/// Если в запросе только виртуальные столбцы, надо запросить хотя бы один любой другой.
		if (real_column_names.size() == 0)
			real_column_names.push_back(ExpressionActions::getSmallestColumn((*it)->getColumnsList()));

		/// Подменяем виртуальный столбец на его значение
		ASTPtr modified_query_ast = query->clone();
		VirtualColumnUtils::rewriteEntityInAst(modified_query_ast, _table_column_name, (*it)->getTableName());

		BlockInputStreams source_streams = (*it)->read(
			real_column_names,
			modified_query_ast,
			settings,
			tmp_processed_stage,
			max_block_size,
			selected_tables.size() > threads ? 1 : (threads / selected_tables.size()));

		for (auto & virtual_column : virt_column_names)
		{
			if (virtual_column == _table_column_name)
			{
				for (auto & stream : source_streams)
					stream = new AddingConstColumnBlockInputStream<String>(stream, new DataTypeString, (*it)->getTableName(), _table_column_name);
			}
		}

		for (BlockInputStreams::iterator jt = source_streams.begin(); jt != source_streams.end(); ++jt)
			res.push_back(*jt);

		if (tmp_processed_stage < processed_stage)
			processed_stage = tmp_processed_stage;
	}

	/** Если истчоников слишком много, то склеим их в threads источников.
	  */
	if (res.size() > threads)
		res = narrowBlockInputStreams(res, threads);

	return res;
}

/// Построить блок состоящий только из возможных значений виртуальных столбцов
Block StorageMerge::getBlockWithVirtualColumns(const std::vector<StoragePtr> & selected_tables) const
{
	Block res;
	ColumnWithNameAndType _table(new ColumnString, new DataTypeString, _table_column_name);

	for (SelectedTables::const_iterator it = selected_tables.begin(); it != selected_tables.end(); ++it)
		_table.column->insert((*it)->getTableName());

	res.insert(_table);
	return res;
}

void StorageMerge::getSelectedTables(StorageVector & selected_tables)
{
	const Tables & tables = context.getDatabases().at(source_database);
	for (Tables::const_iterator it = tables.begin(); it != tables.end(); ++it)
		if (it->second != this && table_name_regexp.match(it->first))
			selected_tables.push_back(it->second);
}


void StorageMerge::alter(const ASTAlterQuery::Parameters & params)
{
	alterColumns(params, columns, context);
}
}

