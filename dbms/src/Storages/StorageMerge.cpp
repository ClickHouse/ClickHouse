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

	/// Список таблиц могут менять в другом потоке.
	Poco::ScopedLock<Poco::Mutex> lock(*context.mutex);

	if (context.databases->end() == context.databases->find(source_database))
		throw Exception("Database " + source_database + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);

	/// Среди всех стадий, до которых обрабатывается запрос в таблицах-источниках, выберем минимальную.
	processed_stage = QueryProcessingStage::Complete;
	QueryProcessingStage::Enum tmp_processed_stage = QueryProcessingStage::Complete;
	
	Tables & tables = context.databases->at(source_database);
	typedef std::vector<Tables::iterator> SelectedTables;
	SelectedTables selected_tables;

	/** Сначала составим список выбранных таблиц, чтобы узнать его размер.
	  * Это нужно, чтобы правильно передать в каждую таблицу рекомендацию по количеству потоков.
	  */
	for (Tables::iterator it = tables.begin(); it != tables.end(); ++it)
		if (it->second != this && table_name_regexp.match(it->first))
			selected_tables.push_back(it);

	for (SelectedTables::iterator it = selected_tables.begin(); it != selected_tables.end(); ++it)
	{
		BlockInputStreams source_streams = (*it)->second->read(
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
