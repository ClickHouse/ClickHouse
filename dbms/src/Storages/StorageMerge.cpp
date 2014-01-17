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
	for (auto & it : column_names)
		if (it != _table_column_name)
			real_column_names.push_back(it);
		else
			virt_column_names.push_back(it);


	typedef std::vector<StoragePtr> SelectedTables;
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

	for (SelectedTables::iterator it = selected_tables.begin(); it != selected_tables.end(); ++it)
	{
		BlockInputStreams source_streams = (*it)->read(
			real_column_names,
			query,
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

