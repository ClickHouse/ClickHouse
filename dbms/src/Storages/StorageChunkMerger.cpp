#include <DB/Storages/StorageChunkMerger.h>
#include <DB/Storages/StorageChunks.h>
#include <DB/Storages/StorageChunkRef.h>
#include <DB/Parsers/ASTCreateQuery.h>
#include <DB/Parsers/ASTNameTypePair.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTDropQuery.h>
#include <DB/Interpreters/InterpreterCreateQuery.h>
#include <DB/Interpreters/InterpreterDropQuery.h>
#include <DB/Interpreters/InterpreterRenameQuery.h>
#include <DB/DataStreams/copyData.h>
#include <DB/DataStreams/ConcatBlockInputStream.h>
#include <DB/DataStreams/narrowBlockInputStreams.h>


namespace DB
{
	
const int SLEEP_AFTER_MERGE = 1;
const int SLEEP_NO_WORK = 10;
const int SLEEP_AFTER_ERROR = 60;

StoragePtr StorageChunkMerger::create(
	const std::string & this_database_,
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const String & source_database_,
	const String & table_name_regexp_,
	const std::string & destination_database_,
	const std::string & destination_name_prefix_,
	size_t chunks_to_merge_,
	Context & context_)
{
	return (new StorageChunkMerger(this_database_, name_, columns_, source_database_, table_name_regexp_, destination_database_, destination_name_prefix_, chunks_to_merge_, context_))->thisPtr();
}

BlockInputStreams StorageChunkMerger::read(
	const Names & column_names,
	ASTPtr query,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	size_t max_block_size,
	unsigned threads)
{
	/// Будем читать из таблиц Chunks, на которые есть хоть одна ChunkRef, подходящая под регэксп, и из прочих таблиц, подходящих под регэксп.
	Storages selected_tables;
	
	{
		Poco::ScopedLock<Poco::Mutex> lock(context.getMutex());
		
		typedef std::set<std::string> StringSet;
		StringSet chunks_table_names;
		
		Databases & databases = context.getDatabases();
		
		if (!databases.count(source_database))
			throw Exception("No database " + source_database, ErrorCodes::UNKNOWN_DATABASE);
		
		Tables & tables = databases[source_database];
		for (Tables::iterator it = tables.begin(); it != tables.end(); ++it)
		{
			StoragePtr table = it->second;
			if (table_name_regexp.match(it->first) &&
				!dynamic_cast<StorageChunks *>(&*table) &&
				!dynamic_cast<StorageChunkMerger *>(&*table))
			{
				if (StorageChunkRef * chunk_ref = dynamic_cast<StorageChunkRef *>(&*table))
				{
					if (chunk_ref->source_database_name != source_database)
					{
						LOG_WARNING(log, "ChunkRef " + chunk_ref->getTableName() + " points to another database, ignoring");
						continue;
					}
					if (!chunks_table_names.count(chunk_ref->source_table_name))
					{
						if (tables.count(chunk_ref->source_table_name))
						{
							chunks_table_names.insert(chunk_ref->source_table_name);
							selected_tables.push_back(tables[chunk_ref->source_table_name]);
						}
						else
						{
							LOG_WARNING(log, "ChunkRef " + chunk_ref->getTableName() + " points to non-existing Chunks table, ignoring");
						}
					}
				}
				else
				{
					selected_tables.push_back(table);
				}
			}
		}
	}
	
	BlockInputStreams res;
	
	/// Среди всех стадий, до которых обрабатывается запрос в таблицах-источниках, выберем минимальную.
	processed_stage = QueryProcessingStage::Complete;
	QueryProcessingStage::Enum tmp_processed_stage = QueryProcessingStage::Complete;
	
	for (Storages::iterator it = selected_tables.begin(); it != selected_tables.end(); ++it)
	{
		BlockInputStreams source_streams = (*it)->read(
			column_names,
			query,
			settings,
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

StorageChunkMerger::StorageChunkMerger(
	const std::string & this_database_,
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const String & source_database_,
	const String & table_name_regexp_,
	const std::string & destination_database_,
	const std::string & destination_name_prefix_,
	size_t chunks_to_merge_,
	Context & context_)
	: this_database(this_database_), name(name_), columns(columns_), source_database(source_database_),
	table_name_regexp(table_name_regexp_), destination_database(destination_database_), destination_name_prefix(destination_name_prefix_), chunks_to_merge(chunks_to_merge_), context(context_),
	thread_should_quit(false), merge_thread(&StorageChunkMerger::mergeThread, this), log(&Logger::get("StorageChunkMerger"))
{
}

StorageChunkMerger::~StorageChunkMerger()
{
	thread_should_quit = true;
	merge_thread.join();
}

void StorageChunkMerger::mergeThread()
{
	while (!thread_should_quit)
	{
		bool merged = false;
		bool error = true;
		
		try
		{
			merged = maybeMergeSomething();
			error = false;
		}
		catch (const DB::Exception & e)
		{
			LOG_ERROR(log, "StorageChunkMerger at " << name << " failed to merge: DB::Exception. Code: " << e.code() << ", e.displayText() = " << e.displayText() << ", e.what() = " << e.what()
			<< ", Stack trace:\n\n" << e.getStackTrace().toString());
		}
		catch (const Poco::Exception & e)
		{
			LOG_ERROR(log, "StorageChunkMerger at " << name << " failed to merge: Poco::Exception. Code: " << ErrorCodes::POCO_EXCEPTION << ", e.code() = " << e.code()
			<< ", e.displayText() = " << e.displayText() << ", e.what() = " << e.what());
		}
		catch (const std::exception & e)
		{
			LOG_ERROR(log, "StorageChunkMerger at " << name << " failed to merge: std::exception. Code: " << ErrorCodes::STD_EXCEPTION << ", e.what() = " << e.what());
		}
		catch (...)
		{
			LOG_ERROR(log, "StorageChunkMerger at " << name << " failed to merge: unknown exception. Code: " << ErrorCodes::UNKNOWN_EXCEPTION);
		}
		
		if (thread_should_quit)
			break;
		
		if (error)
			sleep(SLEEP_AFTER_ERROR);
		else if (merged)
			sleep(SLEEP_AFTER_MERGE);
		else
			sleep(SLEEP_NO_WORK);
	}
}

static std::string MakeName(const std::string & prefix, const std::string & first_chunk, const std::string & last_chunk)
{
	size_t lcp = 0; /// Длина общего префикса имен чанков.
	while (lcp < first_chunk.size() && lcp < last_chunk.size() && first_chunk[lcp] == last_chunk[lcp])
		++lcp;
	return prefix + first_chunk + "_" + last_chunk.substr(lcp);
}

static std::string MakeNameUnique(const std::string & name, const Tables & existing_tables)
{
	if (!existing_tables.count(name))
		return name;
	for (size_t i = 1;; ++i)
	{
		std::string new_name = name + "_" + Poco::NumberFormatter::format(i);
		if (!existing_tables.count(new_name))
			return new_name;
	}
}

bool StorageChunkMerger::maybeMergeSomething()
{
	Storages chunks = selectChunksToMerge();
	if (chunks.empty())
		return false;
	LOG_TRACE(log, "Will merge " << chunks.size() << " chunks: from " << chunks[0]->getTableName() << " to " << chunks.back()->getTableName() << ".");
	mergeChunks(chunks);
	return true;
}

StorageChunkMerger::Storages StorageChunkMerger::selectChunksToMerge()
{
	Poco::ScopedLock<Poco::Mutex> lock(context.getMutex());
	
	Storages res;
	
	Databases & databases = context.getDatabases();
	
	if (!databases.count(source_database))
		throw Exception("No database " + source_database, ErrorCodes::UNKNOWN_DATABASE);
	
	Tables & tables = databases[source_database];
	for (Tables::iterator it = tables.begin(); it != tables.end(); ++it)
	{
		StoragePtr table = it->second;
		if (table_name_regexp.match(it->first) &&
			!dynamic_cast<StorageChunks *>(&*table) &&
			!dynamic_cast<StorageChunkMerger *>(&*table) &&
			!dynamic_cast<StorageChunkRef *>(&*table))
		{
			res.push_back(table);
			
			if (res.size() >= chunks_to_merge)
				break;
		}
	}
	
	if (res.size() < chunks_to_merge)
		res.clear();
	
	return res;
}

static ASTPtr NewIdentifier(const std::string & name, ASTIdentifier::Kind kind)
{
	ASTIdentifier * res = new ASTIdentifier;
	res->name = name;
	res->kind = kind;
	return res;
}

void StorageChunkMerger::mergeChunks(const Storages & chunks)
{
	/// Атомарно выберем таблице уникальное имя и создадим ее.
	std::string new_table_name = MakeName(destination_name_prefix, chunks[0]->getTableName(), chunks.back()->getTableName());
	StoragePtr new_storage_ptr;
	{
		Poco::ScopedLock<Poco::Mutex> lock(context.getMutex());
		
		if (!context.getDatabases().count(destination_database))
			throw Exception("Destination database " + destination_database + " for table " + name + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);
		new_table_name = MakeNameUnique(new_table_name, context.getDatabases()[destination_database]);
		
		/// Составим запрос для создания Chunks таблицы.
		ASTPtr query_ptr = context.getCreateQuery(this_database, name);
		ASTCreateQuery * query = dynamic_cast<ASTCreateQuery *>(&*query_ptr);
		query->attach = false;
		query->if_not_exists = false;
		query->database = destination_database;
		query->table = new_table_name;
		ASTFunction * engine = new ASTFunction;
		query->storage = engine;
		engine->name = "Chunks";
		
		InterpreterCreateQuery interpreter(query_ptr, context);
		new_storage_ptr = interpreter.execute();
	}
	
	/// Скопируем данные в новую таблицу.
	StorageChunks * new_storage = dynamic_cast<StorageChunks *>(&*new_storage_ptr);
	
	Names column_names;
	for (NamesAndTypesList::iterator it = columns->begin(); it != columns->end(); ++it)
		column_names.push_back(it->first);
	
	for (size_t chunk_index = 0; chunk_index < chunks.size(); ++chunk_index)
	{
		StoragePtr src_storage = chunks[chunk_index];
		BlockOutputStreamPtr output = new_storage->writeToNewChunk(src_storage->getTableName());
		
		ASTSelectQuery * select_query = new ASTSelectQuery;
		ASTPtr select_query_ptr;
		
		/// Запрос, вынимающий нужные столбцы.
		ASTPtr select_expression_list;
		ASTPtr database;
		ASTPtr table;	/// Идентификатор или подзапрос (рекурсивно ASTSelectQuery)
		select_query->database = NewIdentifier(source_database, ASTIdentifier::Database);
		select_query->table = NewIdentifier(src_storage->getTableName(), ASTIdentifier::Table);
		ASTExpressionList * select_list = new ASTExpressionList;
		select_query->select_expression_list = select_list;
		for (size_t i = 0; i < column_names.size(); ++i)
			select_list->children.push_back(NewIdentifier(column_names[i], ASTIdentifier::Column));
		
		QueryProcessingStage::Enum processed_stage;
		
		BlockInputStreams input_streams = src_storage->read(
			column_names,
			select_query_ptr,
			context.getSettingsRef(),
			processed_stage);
		
		BlockInputStreamPtr input = new ConcatBlockInputStream(input_streams);
		
		copyData(*input, *output);
	}
	
	/// Атомарно подменим исходные таблицы ссылками на новую.
	do
	{
		Poco::ScopedLock<Poco::Mutex> lock(context.getMutex());
		
		/// Если БД успели удалить, ничего не делаем.
		if (!context.getDatabases().count(source_database))
			break;
		
		for (size_t chunk_index = 0; chunk_index < chunks.size(); ++chunk_index)
		{
			StoragePtr src_storage = chunks[chunk_index];
			
			/// Если таблицу успели удалить, ничего не делаем.
			if (!context.getDatabases()[source_database].count(src_storage->getTableName()))
				continue;
			
			/// Роняем исходную таблицу.
			ASTDropQuery * drop_query = new ASTDropQuery();
			ASTPtr drop_query_ptr = drop_query;
			drop_query->detach = false;
			drop_query->if_exists = false;
			drop_query->database = source_database;
			drop_query->table = src_storage->getTableName();
			
			InterpreterDropQuery interpreter_drop(drop_query_ptr, context);
			interpreter_drop.execute();
			
			/// Создаем на ее месте ChunkRef
			///  (если бы ChunkRef хранил что-то в директории с данными, тут возникли бы проблемы, потому что данные src_storage еще не удалены).
			try
			{
				ASTPtr create_query_ptr = context.getCreateQuery(this_database, name);
				ASTCreateQuery * create_query = dynamic_cast<ASTCreateQuery *>(&*create_query_ptr);
				create_query->attach = false;
				create_query->if_not_exists = false;
				create_query->database = source_database;
				create_query->table = src_storage->getTableName();
				
				ASTFunction * ast_storage = new ASTFunction;
				create_query->storage = ast_storage;
				ast_storage->name = "ChunkRef";
				
				ASTExpressionList * engine_params = new ASTExpressionList;
				ast_storage->children.push_back(engine_params);
				engine_params->children.push_back(NewIdentifier(destination_database, ASTIdentifier::Database));
				engine_params->children.push_back(NewIdentifier(new_table_name, ASTIdentifier::Table));
				
				InterpreterCreateQuery interpreter_create(create_query_ptr, context);
				interpreter_create.execute();
			}
			catch(...)
			{
				LOG_ERROR(log, "Chunk " + src_storage->getTableName() + " was removed but not replaced: data is lost!");
				
				throw;
			}
		}
	}
	while(false);
	
	new_storage->removeReference();
}
	
}
