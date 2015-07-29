#include <DB/Storages/StorageChunkMerger.h>
#include <DB/Storages/StorageChunks.h>
#include <DB/Storages/StorageChunkRef.h>
#include <DB/Interpreters/executeQuery.h>
#include <DB/Interpreters/InterpreterDropQuery.h>
#include <DB/DataStreams/ConcatBlockInputStream.h>
#include <DB/DataStreams/AddingDefaultBlockInputStream.h>
#include <DB/DataStreams/AddingConstColumnBlockInputStream.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/formatAST.h>
#include <DB/Common/VirtualColumnUtils.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/Columns/ColumnString.h>


namespace DB
{
const int NOTHING_TO_MERGE_PERIOD = 10;

StorageChunkMerger::TableNames StorageChunkMerger::currently_written_groups;

StoragePtr StorageChunkMerger::create(
	const std::string & this_database_,
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const NamesAndTypesList & materialized_columns_,
	const NamesAndTypesList & alias_columns_,
	const ColumnDefaults & column_defaults_,
	const String & source_database_,
	const String & table_name_regexp_,
	const std::string & destination_name_prefix_,
	size_t chunks_to_merge_,
	Context & context_)
{
	return (new StorageChunkMerger{
		this_database_, name_, columns_, materialized_columns_, alias_columns_, column_defaults_,
		source_database_, table_name_regexp_, destination_name_prefix_,
		chunks_to_merge_, context_
	})->thisPtr();
}

NameAndTypePair StorageChunkMerger::getColumn(const String & column_name) const
{
	if (column_name == _table_column_name)
		return NameAndTypePair(_table_column_name, new DataTypeString);
	return IStorage::getColumn(column_name);
}

bool StorageChunkMerger::hasColumn(const String & column_name) const
{
	if (column_name == _table_column_name) return true;
	return IStorage::hasColumn(column_name);
}

BlockInputStreams StorageChunkMerger::read(
	const Names & column_names,
	ASTPtr query,
	const Context & context,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	const size_t max_block_size,
	const unsigned threads)
{
	/// Будем читать из таблиц Chunks, на которые есть хоть одна ChunkRef, подходящая под регэксп, и из прочих таблиц, подходящих под регэксп.
	Storages selected_tables;

	{
		Poco::ScopedLock<Poco::Mutex> lock(context.getMutex());

		typedef std::set<std::string> StringSet;
		StringSet chunks_table_names;

		const Databases & databases = context.getDatabases();

		const auto database_it = databases.find(source_database);
		if (database_it == std::end(databases))
			throw Exception("No database " + source_database, ErrorCodes::UNKNOWN_DATABASE);

		const Tables & tables = database_it->second;
		for (const auto & it : tables)
		{
			const StoragePtr & table = it.second;
			if (table_name_regexp.match(it.first) &&
				!typeid_cast<StorageChunks *>(&*table) &&
				!typeid_cast<StorageChunkMerger *>(&*table))
			{
				if (StorageChunkRef * chunk_ref = typeid_cast<StorageChunkRef *>(&*table))
				{
					if (chunk_ref->source_database_name != source_database)
					{
						LOG_WARNING(log, "ChunkRef " + it.first + " points to another database, ignoring");
						continue;
					}
					if (!chunks_table_names.count(chunk_ref->source_table_name))
					{
						const auto table_it = tables.find(chunk_ref->source_table_name);
						if (table_it != std::end(tables))
						{
							chunks_table_names.insert(chunk_ref->source_table_name);
							selected_tables.push_back(table_it->second);
						}
						else
						{
							LOG_WARNING(log, "ChunkRef " + it.first + " points to non-existing Chunks table, ignoring");
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

	TableLocks table_locks;

	/// Нельзя, чтобы эти таблицы кто-нибудь удалил, пока мы их читаем.
	for (auto table : selected_tables)
	{
		table_locks.push_back(table->lockStructure(false));
	}

	BlockInputStreams res;

	/// Среди всех стадий, до которых обрабатывается запрос в таблицах-источниках, выберем минимальную.
	processed_stage = QueryProcessingStage::Complete;
	QueryProcessingStage::Enum tmp_processed_stage = QueryProcessingStage::Complete;

	bool has_virtual_column = false;

	for (const auto & column : column_names)
		if (column == _table_column_name)
			has_virtual_column = true;

	Block virtual_columns_block = getBlockWithVirtualColumns(selected_tables);

	/// Если запрошен хотя бы один виртуальный столбец, пробуем индексировать
	if (has_virtual_column)
		VirtualColumnUtils::filterBlockWithQuery(query, virtual_columns_block, context);

	std::multiset<String> values = VirtualColumnUtils::extractSingleValueFromBlock<String>(virtual_columns_block, _table_column_name);

	for (size_t i = 0; i < selected_tables.size(); ++i)
	{
		StoragePtr table = selected_tables[i];
		auto table_lock = table_locks[i];

		if (table->getName() != "Chunks" && values.find(table->getTableName()) == values.end())
			continue;

		/// Список виртуальных столбцов, которые мы заполним сейчас и список столбцов, которые передадим дальше
		Names virt_column_names, real_column_names;
		for (const auto & column : column_names)
			if (column == _table_column_name && table->getName() != "Chunks") /// таблица Chunks сама заполняет столбец _table
				virt_column_names.push_back(column);
			else
				real_column_names.push_back(column);

		/// Если в запросе только виртуальные столбцы, надо запросить хотя бы один любой другой.
		if (real_column_names.size() == 0)
			real_column_names.push_back(ExpressionActions::getSmallestColumn(table->getColumnsList()));

		ASTPtr modified_query_ast = query->clone();

		/// Подменяем виртуальный столбец на его значение
		if (!virt_column_names.empty())
			VirtualColumnUtils::rewriteEntityInAst(modified_query_ast, _table_column_name, table->getTableName());

		BlockInputStreams source_streams = table->read(
			real_column_names,
			modified_query_ast,
			context,
			settings,
			tmp_processed_stage,
			max_block_size,
			selected_tables.size() > threads ? 1 : (threads / selected_tables.size()));

		for (auto & stream : source_streams)
		{
			stream->addTableLock(table_lock);
		}

		/// Добавляем в ответ вирутальные столбцы
		for (const auto & virtual_column : virt_column_names)
		{
			if (virtual_column == _table_column_name)
			{
				for (auto & stream : source_streams)
				{
					stream = new AddingConstColumnBlockInputStream<String>(stream, new DataTypeString, table->getTableName(), _table_column_name);
				}
			}
		}

		for (BlockInputStreams::iterator jt = source_streams.begin(); jt != source_streams.end(); ++jt)
			res.push_back(*jt);

		if (tmp_processed_stage < processed_stage)
			processed_stage = tmp_processed_stage;
	}

	return res;
}

/// Построить блок состоящий только из возможных значений виртуальных столбцов
Block StorageChunkMerger::getBlockWithVirtualColumns(const Storages & selected_tables) const
{
	Block res;
	ColumnWithNameAndType _table(new ColumnString, new DataTypeString, _table_column_name);

	for (Storages::const_iterator it = selected_tables.begin(); it != selected_tables.end(); ++it)
		if ((*it)->getName() != "Chunks")
			_table.column->insert((*it)->getTableName());

	res.insert(_table);
	return res;
}

class StorageChunkMerger::MergeTask
{
public:
	MergeTask(const StorageChunkMerger & chunk_merger_, DB::Context & context_, Logger * log_)
		: shutdown_called(false),
		chunk_merger(chunk_merger_),
		context(context_),
		log(log_),
		merging(false)
	{
	}

	bool merge();

public:
	std::atomic<bool> shutdown_called;

private:
	bool maybeMergeSomething();
	Storages selectChunksToMerge();
	bool mergeChunks(const Storages & chunks);

	const StorageChunkMerger & chunk_merger;
	DB::Context & context;

	Logger * log;
	time_t last_nothing_to_merge_time = 0;

	std::atomic<bool> merging;
};

StorageChunkMerger::StorageChunkMerger(
	const std::string & this_database_,
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const NamesAndTypesList & materialized_columns_,
	const NamesAndTypesList & alias_columns_,
	const ColumnDefaults & column_defaults_,
	const String & source_database_,
	const String & table_name_regexp_,
	const std::string & destination_name_prefix_,
	size_t chunks_to_merge_,
	Context & context_)
	: IStorage{materialized_columns_, alias_columns_, column_defaults_},
	this_database(this_database_), name(name_), columns(columns_), source_database(source_database_),
	table_name_regexp(table_name_regexp_), destination_name_prefix(destination_name_prefix_), chunks_to_merge(chunks_to_merge_),
	context(context_), settings(context.getSettings()),
	log(&Logger::get("StorageChunkMerger")),
	merge_task(std::make_shared<MergeTask>(*this, context, log))
{
	_table_column_name = "_table" + VirtualColumnUtils::chooseSuffix(getColumnsList(), "_table");

	auto & backgroud_pool = context.getBackgroundPool();
	MergeTaskPtr tmp_merge_task = merge_task;
	DB::BackgroundProcessingPool::Task task = [tmp_merge_task](BackgroundProcessingPool::Context & pool_context) -> bool
	{
		return tmp_merge_task->merge();
	};

	merge_task_handle = backgroud_pool.addTask(task);
}

void StorageChunkMerger::shutdown()
{
	if (merge_task->shutdown_called)
		return;

	merge_task->shutdown_called.store(true);
	context.getBackgroundPool().removeTask(merge_task_handle);
}

StorageChunkMerger::~StorageChunkMerger()
{
	shutdown();
}

struct BoolLock
{
	BoolLock(std::atomic<bool> & flag_) : flag(flag_), locked(false) {}

	bool trylock()
	{
		bool expected = false;
		locked = flag.compare_exchange_weak(expected, true);
		return locked;
	}

	~BoolLock()
	{
		if (locked)
			flag.store(false);
	}

	std::atomic<bool> & flag;
	bool locked;
};

bool StorageChunkMerger::MergeTask::merge()
{
	time_t now = time(0);
	if (last_nothing_to_merge_time + NOTHING_TO_MERGE_PERIOD > now)
		return false;

	if (shutdown_called)
		return false;

	BoolLock lock(merging);
	if (!lock.trylock())
		return false;

	bool merged = false;

	try
	{
		merged = maybeMergeSomething();
	}
	catch (const Exception & e)
	{
		LOG_ERROR(log, "StorageChunkMerger at " << chunk_merger.this_database << "." << chunk_merger.name << " failed to merge: Code: " << e.code() << ", e.displayText() = " << e.displayText() << ", e.what() = " << e.what()
		<< ", Stack trace:\n\n" << e.getStackTrace().toString());
	}
	catch (const Poco::Exception & e)
	{
		LOG_ERROR(log, "StorageChunkMerger at " << chunk_merger.this_database << "." << chunk_merger.name << " failed to merge: Poco::Exception. Code: " << ErrorCodes::POCO_EXCEPTION << ", e.code() = " << e.code()
		<< ", e.displayText() = " << e.displayText() << ", e.what() = " << e.what());
	}
	catch (const std::exception & e)
	{
		LOG_ERROR(log, "StorageChunkMerger at " << chunk_merger.this_database << "." << chunk_merger.name << " failed to merge: std::exception. Code: " << ErrorCodes::STD_EXCEPTION << ", e.what() = " << e.what());
	}
	catch (...)
	{
		LOG_ERROR(log, "StorageChunkMerger at " << chunk_merger.this_database << "." << chunk_merger.name << " failed to merge: unknown exception. Code: " << ErrorCodes::UNKNOWN_EXCEPTION);
	}

	if (!merged)
		last_nothing_to_merge_time = now;

	return merged;
}

static std::string makeName(const std::string & prefix, const std::string & first_chunk, const std::string & last_chunk)
{
	size_t lcp = 0; /// Длина общего префикса имен чанков.
	while (lcp < first_chunk.size() && lcp < last_chunk.size() && first_chunk[lcp] == last_chunk[lcp])
		++lcp;
	return prefix + first_chunk + "_" + last_chunk.substr(lcp);
}

bool StorageChunkMerger::MergeTask::maybeMergeSomething()
{
	Storages chunks = selectChunksToMerge();
	if (chunks.empty() || shutdown_called)
		return false;
	return mergeChunks(chunks);
}

StorageChunkMerger::Storages StorageChunkMerger::MergeTask::selectChunksToMerge()
{
	Poco::ScopedLock<Poco::Mutex> lock(context.getMutex());

	Storages res;

	Databases & databases = context.getDatabases();

	if (!databases.count(chunk_merger.source_database))
		throw Exception("No database " + chunk_merger.source_database, ErrorCodes::UNKNOWN_DATABASE);

	Tables & tables = databases[chunk_merger.source_database];
	for (Tables::iterator it = tables.begin(); it != tables.end(); ++it)
	{
		StoragePtr table = it->second;
		if (chunk_merger.table_name_regexp.match(it->first) &&
			!typeid_cast<StorageChunks *>(&*table) &&
			!typeid_cast<StorageChunkMerger *>(&*table) &&
			!typeid_cast<StorageChunkRef *>(&*table))
		{
			res.push_back(table);

			if (res.size() >= chunk_merger.chunks_to_merge)
				break;
		}
	}

	if (res.size() < chunk_merger.chunks_to_merge)
		res.clear();

	return res;
}

static ASTPtr newIdentifier(const std::string & name, ASTIdentifier::Kind kind)
{
	ASTIdentifier * res = new ASTIdentifier;
	res->name = name;
	res->kind = kind;
	return res;
}

bool StorageChunkMerger::MergeTask::mergeChunks(const Storages & chunks)
{
	typedef std::map<std::string, DataTypePtr> ColumnsMap;

	TableLocks table_locks;

	/// Нельзя, чтобы эти таблицы кто-нибудь удалил, пока мы их читаем.
	for (auto table : chunks)
	{
		table_locks.push_back(table->lockStructure(false));
	}

	/// Объединим множества столбцов сливаемых чанков.
	ColumnsMap known_columns_types;
	for (const NameAndTypePair & column : chunk_merger.getColumnsList())
		known_columns_types.insert(std::make_pair(column.name, column.type));

	NamesAndTypesListPtr required_columns = new NamesAndTypesList;
	*required_columns = *chunk_merger.columns;

	for (size_t chunk_index = 0; chunk_index < chunks.size(); ++chunk_index)
	{
		const NamesAndTypesList & current_columns = chunks[chunk_index]->getColumnsList();

		for (NamesAndTypesList::const_iterator it = current_columns.begin(); it != current_columns.end(); ++it)
		{
			const std::string & name = it->name;
			const DataTypePtr & type = it->type;
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

	std::string formatted_columns = formatColumnsForCreateQuery(*required_columns);

	std::string new_table_name = makeName(chunk_merger.destination_name_prefix, chunks.front()->getTableName(), chunks.back()->getTableName());
	std::string new_table_full_name = backQuoteIfNeed(chunk_merger.source_database) + "." + backQuoteIfNeed(new_table_name);
	StoragePtr new_storage_ptr;

	try
	{
		{
			Poco::ScopedLock<Poco::Mutex> lock(context.getMutex());

			if (!context.getDatabases().count(chunk_merger.source_database))
				throw Exception("Destination database " + chunk_merger.source_database + " for table " + chunk_merger.name + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);

			LOG_TRACE(log, "Will merge " << chunks.size() << " chunks: from " << chunks[0]->getTableName() << " to " << chunks.back()->getTableName() << " to new table " << new_table_name << ".");

			if (currently_written_groups.count(new_table_full_name))
			{
				LOG_WARNING(log, "Table " + new_table_full_name + " is already being written. Aborting merge.");
				return false;
			}

			currently_written_groups.insert(new_table_full_name);
		}

		/// Уроним Chunks таблицу с таким именем, если она есть. Она могла остаться в результате прерванного слияния той же группы чанков.
		executeQuery("DROP TABLE IF EXISTS " + new_table_full_name, context, true);

		/// Выполним запрос для создания Chunks таблицы.
		executeQuery("CREATE TABLE " + new_table_full_name + " " + formatted_columns + " ENGINE = Chunks", context, true);

		new_storage_ptr = context.getTable(chunk_merger.source_database, new_table_name);

		/// Скопируем данные в новую таблицу.
		StorageChunks & new_storage = typeid_cast<StorageChunks &>(*new_storage_ptr);

		for (size_t chunk_index = 0; chunk_index < chunks.size(); ++chunk_index)
		{
			StoragePtr src_storage = chunks[chunk_index];
			BlockOutputStreamPtr output = new_storage.writeToNewChunk(src_storage->getTableName());

			const NamesAndTypesList & src_columns = src_storage->getColumnsList();
			Names src_column_names;

			ASTSelectQuery * select_query = new ASTSelectQuery;
			ASTPtr select_query_ptr = select_query;

			/// Запрос, вынимающий нужные столбцы.
			ASTPtr select_expression_list;
			ASTPtr database;
			ASTPtr table;	/// Идентификатор или подзапрос (рекурсивно ASTSelectQuery)
			select_query->database = newIdentifier(chunk_merger.source_database, ASTIdentifier::Database);
			select_query->table = newIdentifier(src_storage->getTableName(), ASTIdentifier::Table);
			ASTExpressionList * select_list = new ASTExpressionList;
			select_query->select_expression_list = select_list;
			for (NamesAndTypesList::const_iterator it = src_columns.begin(); it != src_columns.end(); ++it)
			{
				src_column_names.push_back(it->name);
				select_list->children.push_back(newIdentifier(it->name, ASTIdentifier::Column));
			}

			QueryProcessingStage::Enum processed_stage = QueryProcessingStage::Complete;

			BlockInputStreams input_streams = src_storage->read(
				src_column_names,
				select_query_ptr,
				context,
				chunk_merger.settings,
				processed_stage,
				DEFAULT_MERGE_BLOCK_SIZE);

			BlockInputStreamPtr input{new AddingDefaultBlockInputStream{
				new ConcatBlockInputStream{input_streams},
				required_columns, src_storage->column_defaults, context
			}};

			input->readPrefix();
			output->writePrefix();

			Block block;
			while (!shutdown_called && (block = input->read()))
				output->write(block);

			if (shutdown_called)
			{
				LOG_INFO(log, "Shutdown requested while merging chunks.");
				output->writeSuffix();
				new_storage.removeReference();	/// После этого временные данные удалятся.
				return false;
			}

			input->readSuffix();
			output->writeSuffix();
		}

		/// Атомарно подменим исходные таблицы ссылками на новую.
		/// При этом удалять таблицы под мьютексом контекста нельзя, пока только отцепим их.
		Storages tables_to_drop;

		{
			Poco::ScopedLock<Poco::Mutex> lock(context.getMutex());

			/// Если БД успели удалить, ничего не делаем.
			if (context.getDatabases().count(chunk_merger.source_database))
			{
				for (size_t chunk_index = 0; chunk_index < chunks.size(); ++chunk_index)
				{
					StoragePtr src_storage = chunks[chunk_index];
					std::string src_name = src_storage->getTableName();

					/// Если таблицу успели удалить, ничего не делаем.
					if (!context.isTableExist(chunk_merger.source_database, src_name))
						continue;

					/// Отцепляем исходную таблицу. Ее данные и метаданные остаются на диске.
					tables_to_drop.push_back(context.detachTable(chunk_merger.source_database, src_name));

					/// Создаем на ее месте ChunkRef. Это возможно только потому что у ChunkRef нет ни, ни метаданных.
					try
					{
						context.addTable(chunk_merger.source_database, src_name, StorageChunkRef::create(src_name, context, chunk_merger.source_database, new_table_name, false));
					}
					catch (...)
					{
						LOG_ERROR(log, "Chunk " + src_name + " was removed but not replaced. Its data is stored in table " << new_table_name << ". You may need to resolve this manually.");

						throw;
					}
				}
			}

			currently_written_groups.erase(new_table_full_name);
		}

		/// Теперь удалим данные отцепленных таблиц.
		table_locks.clear();
		for (StoragePtr table : tables_to_drop)
		{
			InterpreterDropQuery::dropDetachedTable(chunk_merger.source_database, table, context);
			/// NOTE: Если между подменой таблицы и этой строчкой кто-то успеет попытаться создать новую таблицу на ее месте,
			///  что-нибудь может сломаться.
		}

		/// Сейчас на new_storage ссылаются таблицы типа ChunkRef. Удалим лишнюю ссылку, которая была при создании.
		new_storage.removeReference();

		LOG_TRACE(log, "Merged chunks.");

		return true;
	}
	catch(...)
	{
		Poco::ScopedLock<Poco::Mutex> lock(context.getMutex());

		currently_written_groups.erase(new_table_full_name);

		throw;
	}
}

}
