#include <DB/Storages/StorageChunkMerger.h>
#include <DB/Storages/StorageChunks.h>
#include <DB/Storages/StorageChunkRef.h>
#include <DB/Parsers/ASTCreateQuery.h>
#include <DB/Parsers/ASTNameTypePair.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTDropQuery.h>
#include <DB/Parsers/ParserCreateQuery.h>
#include <DB/Interpreters/InterpreterCreateQuery.h>
#include <DB/Interpreters/InterpreterDropQuery.h>
#include <DB/Interpreters/InterpreterRenameQuery.h>
#include <DB/DataStreams/copyData.h>
#include <DB/DataStreams/ConcatBlockInputStream.h>
#include <DB/DataStreams/narrowBlockInputStreams.h>
#include <DB/DataStreams/AddingDefaultBlockInputStream.h>


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

void StorageChunkMerger::dropImpl()
{
	thread_should_quit = true;
	merge_thread.join();
}

StorageChunkMerger::~StorageChunkMerger()
{
	merge_thread.detach();
}

void StorageChunkMerger::mergeThread()
{
	/// Не дает удалить this посреди итерации.
	StoragePtr this_ptr = thisPtr();
	
	while (!thread_should_quit && this_ptr.use_count() > 1)
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
			LOG_ERROR(log, "StorageChunkMerger at " << this_database << "." << name << " failed to merge: DB::Exception. Code: " << e.code() << ", e.displayText() = " << e.displayText() << ", e.what() = " << e.what()
			<< ", Stack trace:\n\n" << e.getStackTrace().toString());
		}
		catch (const Poco::Exception & e)
		{
			LOG_ERROR(log, "StorageChunkMerger at " << this_database << "." << name << " failed to merge: Poco::Exception. Code: " << ErrorCodes::POCO_EXCEPTION << ", e.code() = " << e.code()
			<< ", e.displayText() = " << e.displayText() << ", e.what() = " << e.what());
		}
		catch (const std::exception & e)
		{
			LOG_ERROR(log, "StorageChunkMerger at " << this_database << "." << name << " failed to merge: std::exception. Code: " << ErrorCodes::STD_EXCEPTION << ", e.what() = " << e.what());
		}
		catch (...)
		{
			LOG_ERROR(log, "StorageChunkMerger at " << this_database << "." << name << " failed to merge: unknown exception. Code: " << ErrorCodes::UNKNOWN_EXCEPTION);
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

bool StorageChunkMerger::maybeMergeSomething()
{
	Storages chunks = selectChunksToMerge();
	if (chunks.empty())
		return false;
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

static std::string FormatColumnsForCreateQuery(NamesAndTypesList & columns)
{
	std::string res;
	res += "(";
	for (NamesAndTypesList::iterator it = columns.begin(); it != columns.end(); ++it)
	{
		if (it != columns.begin())
			res += ", ";
		res += it->first;
		res += " ";
		res += it->second->getName();
	}
	res += ")";
	return res;
}

void StorageChunkMerger::mergeChunks(const Storages & chunks)
{
	typedef std::map<std::string, DataTypePtr> ColumnsMap;
	
	/// Объединим множества столбцов сливаемых чанков.
	ColumnsMap known_columns_types(columns->begin(), columns->end());
	NamesAndTypesListPtr required_columns = new NamesAndTypesList;
	*required_columns = *columns;
	
	for (size_t chunk_index = 0; chunk_index < chunks.size(); ++chunk_index)
	{
		const NamesAndTypesList & current_columns = chunks[chunk_index]->getColumnsList();
		
		for (NamesAndTypesList::const_iterator it = current_columns.begin(); it != current_columns.end(); ++it)
		{
			const std::string & name = it->first;
			const DataTypePtr & type = it->second;
			if (known_columns_types.count(name))
			{
				String current_type_name = type->getName();
				String known_type_name = known_columns_types[name]->getName();
				if (current_type_name != known_type_name)
					throw Exception("Different types of column " + name + " in different chunks: type " + current_type_name + " in chunk " + chunks[chunk_index]->getTableName() + ", type " + known_type_name + " somewhere else", ErrorCodes::TYPE_MISMATCH);
			}
			else
			{
				known_columns_types[name] = type;
				required_columns->push_back(*it);
			}
		}
	}
	
	std::string formatted_columns = FormatColumnsForCreateQuery(*required_columns);
	
	/// Атомарно выберем таблице уникальное имя и создадим ее.
	std::string new_table_name = MakeName(destination_name_prefix, chunks[0]->getTableName(), chunks.back()->getTableName());
	StoragePtr new_storage_ptr;
	{
		Poco::ScopedLock<Poco::Mutex> lock(context.getMutex());
		
		if (!context.getDatabases().count(destination_database))
			throw Exception("Destination database " + destination_database + " for table " + name + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);
		
		LOG_TRACE(log, "Will merge " << chunks.size() << " chunks: from " << chunks[0]->getTableName() << " to " << chunks.back()->getTableName() << " to new table " << new_table_name << ".");
		
		/// Уроним Chunks таблицу с таким именем, если она есть. Она могла остаться в результате прерванного слияния той же группы чанков.
		ASTDropQuery * drop_ast = new ASTDropQuery;
		ASTPtr drop_ptr = drop_ast;
		drop_ast->database = destination_database;
		drop_ast->detach = false;
		drop_ast->if_exists = true;
		drop_ast->table = new_table_name;
		InterpreterDropQuery drop_interpreter(drop_ptr, context);
		drop_interpreter.execute();
		
		/// Составим запрос для создания Chunks таблицы.
		std::string create_query = "CREATE TABLE " + destination_database + "." + new_table_name + " " + formatted_columns + " ENGINE = Chunks";
		
		/// Распарсим запрос.
		const char * begin = create_query.data();
		const char * end = begin + create_query.size();
		const char * pos = begin;
		
		ParserCreateQuery parser;
		ASTPtr ast_create_query;
		String expected;
		bool parse_res = parser.parse(pos, end, ast_create_query, expected);
		
		/// Распарсенный запрос должен заканчиваться на конец входных данных.
		if (!parse_res || pos != end)
			throw DB::Exception("Syntax error while parsing create query made by ChunkMerger."
			" The query is \"" + create_query + "\"."
			+ " Failed at position " + Poco::NumberFormatter::format(pos - begin) + ": "
			+ std::string(pos, std::min(SHOW_CHARS_ON_SYNTAX_ERROR, end - pos))
			+ ", expected " + (parse_res ? "end of query" : expected) + ".",
			DB::ErrorCodes::LOGICAL_ERROR);
		
		/// Выполним запрос.
		InterpreterCreateQuery create_interpreter(ast_create_query, context);
		new_storage_ptr = create_interpreter.execute();
	}
	
	/// Скопируем данные в новую таблицу.
	StorageChunks * new_storage = dynamic_cast<StorageChunks *>(&*new_storage_ptr);
	
	for (size_t chunk_index = 0; chunk_index < chunks.size(); ++chunk_index)
	{
		StoragePtr src_storage = chunks[chunk_index];
		BlockOutputStreamPtr output = new_storage->writeToNewChunk(src_storage->getTableName());
		
		const NamesAndTypesList & src_columns = src_storage->getColumnsList();
		Names src_column_names;
		
		ASTSelectQuery * select_query = new ASTSelectQuery;
		ASTPtr select_query_ptr = select_query;
		
		/// Запрос, вынимающий нужные столбцы.
		ASTPtr select_expression_list;
		ASTPtr database;
		ASTPtr table;	/// Идентификатор или подзапрос (рекурсивно ASTSelectQuery)
		select_query->database = NewIdentifier(source_database, ASTIdentifier::Database);
		select_query->table = NewIdentifier(src_storage->getTableName(), ASTIdentifier::Table);
		ASTExpressionList * select_list = new ASTExpressionList;
		select_query->select_expression_list = select_list;
		for (NamesAndTypesList::const_iterator it = src_columns.begin(); it != src_columns.end(); ++it)
		{
			src_column_names.push_back(it->first);
			select_list->children.push_back(NewIdentifier(it->first, ASTIdentifier::Column));
		}
		
		QueryProcessingStage::Enum processed_stage;

		Settings settings = context.getSettings();
		
		BlockInputStreams input_streams = src_storage->read(
			src_column_names,
			select_query_ptr,
			settings,
			processed_stage);
		
		BlockInputStreamPtr input = new AddingDefaultBlockInputStream(new ConcatBlockInputStream(input_streams), required_columns);
		
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
			std::string src_name = src_storage->getTableName();
			
			/// Если таблицу успели удалить, ничего не делаем.
			if (!context.getDatabases()[source_database].count(src_storage->getTableName()))
				continue;
			
			/// Перед удалением таблицы запомним запрос для ее создания.
			ASTPtr create_query_ptr = context.getCreateQuery(source_database, src_storage->getTableName());
			
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
				ASTCreateQuery * create_query = dynamic_cast<ASTCreateQuery *>(&*create_query_ptr);
				create_query->attach = false;
				create_query->if_not_exists = false;
				
				ASTFunction * ast_storage = new ASTFunction;
				create_query->storage = ast_storage;
				ast_storage->name = "ChunkRef";
				
				ASTExpressionList * engine_params = new ASTExpressionList;
				ast_storage->parameters = engine_params;
				ast_storage->children.push_back(ast_storage->parameters);
				engine_params->children.push_back(NewIdentifier(destination_database, ASTIdentifier::Database));
				engine_params->children.push_back(NewIdentifier(new_table_name, ASTIdentifier::Table));
				
				InterpreterCreateQuery interpreter_create(create_query_ptr, context);
				interpreter_create.execute();
			}
			catch(...)
			{
				LOG_ERROR(log, "Chunk " + src_name + " was removed but not replaced. Its data is stored in table " << new_table_name << ". You may need to resolve this manually.");
				
				throw;
			}
		}
	}
	while (false);

	new_storage->removeReference();

	LOG_TRACE(log, "Merged chunks.");
}
	
}
