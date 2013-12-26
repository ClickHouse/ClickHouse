#include <DB/DataStreams/narrowBlockInputStreams.h>
#include <DB/Storages/StorageMerge.h>
#include <DB/DataTypes/DataTypeString.h>

#include <DB/DataStreams/AddingVirtualColumnsBlockInputStream.h>

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
	/// Создаем виртуальные столбцы и инициализруем их
	virtual_columns = new VirtualColumnList;
	virtual_columns->addColumn(new VirtualColumn<String>("_table", *Extractors::nameExtractor, new DataTypeString));
	virtual_columns->calculateNames(*columns);
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

NamesAndTypesList StorageMerge::getFullColumnsList() const
{
	NamesAndTypesList res = getColumnsList();
	NamesAndTypesList virt = virtual_columns->getColumnsList();
	res.splice(res.end(), virt);
	return res;
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

	Names notvirt;
	VirtualColumnList virt;
	virtual_columns->splitNames(column_names, notvirt, virt);

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
			notvirt,
			query,
			settings,
			tmp_processed_stage,
			max_block_size,
			selected_tables.size() > threads ? 1 : (threads / selected_tables.size()));

		for (BlockInputStreams::iterator jt = source_streams.begin(); jt != source_streams.end(); ++jt)
		{
			res.push_back(new AddingVirtualColumnsBlockInputStream(*jt, virt, *it));
		}

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

