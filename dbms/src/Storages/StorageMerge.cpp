#include <DB/DataStreams/narrowBlockInputStreams.h>
#include <DB/Storages/StorageMerge.h>


namespace DB
{

StorageMerge::StorageMerge(
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const String & source_database_,
	const String & table_name_regexp_,
	Context & context_)
	: name(name_), columns(columns_), source_database(source_database_), table_name_regexp(table_name_regexp_), context(context_)
{
}


BlockInputStreams StorageMerge::read(
	const Names & column_names,
	ASTPtr query,
	QueryProcessingStage::Enum & processed_stage,
	size_t max_block_size,
	unsigned threads)
{
	BlockInputStreams res;

	typedef std::vector<StoragePtr> SelectedTables;
	SelectedTables selected_tables;

	/// Среди всех стадий, до которых обрабатывается запрос в таблицах-источниках, выберем минимальную.
	processed_stage = QueryProcessingStage::Complete;
	QueryProcessingStage::Enum tmp_processed_stage = QueryProcessingStage::Complete;

	/// Список таблиц могут менять в другом потоке.
	{
		Poco::ScopedLock<Poco::Mutex> lock(context.getMutex());
		context.assertDatabaseExists(source_database);

		const Tables & tables = context.getDatabases().at(source_database);

		/** Сначала составим список выбранных таблиц, чтобы узнать его размер.
		  * Это нужно, чтобы правильно передать в каждую таблицу рекомендацию по количеству потоков.
		  */
		for (Tables::const_iterator it = tables.begin(); it != tables.end(); ++it)
			if (it->second != this && table_name_regexp.match(it->first))
				selected_tables.push_back(it->second);
	}

	for (SelectedTables::iterator it = selected_tables.begin(); it != selected_tables.end(); ++it)
	{
		BlockInputStreams source_streams = (*it)->read(
			column_names,
			query,
			tmp_processed_stage,
			max_block_size,
			selected_tables.size() > threads ? 1 : (threads / selected_tables.size()));

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

}
