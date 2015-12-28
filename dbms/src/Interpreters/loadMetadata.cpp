#include <iomanip>
#include <thread>
#include <future>

#include <common/threadpool.hpp>

#include <Poco/DirectoryIterator.h>
#include <Poco/FileStream.h>

#include <DB/Parsers/ParserCreateQuery.h>
#include <DB/Parsers/ASTCreateQuery.h>
#include <DB/Parsers/parseQuery.h>

#include <DB/Interpreters/InterpreterCreateQuery.h>
#include <DB/Interpreters/loadMetadata.h>

#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/copyData.h>
#include <DB/Common/escapeForFileName.h>

#include <DB/Common/Stopwatch.h>


namespace DB
{


static void executeCreateQuery(const String & query, Context & context, const String & database, const String & file_name)
{
	ParserCreateQuery parser;
	ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "in file " + file_name);

	ASTCreateQuery & ast_create_query = typeid_cast<ASTCreateQuery &>(*ast);
	ast_create_query.attach = true;
	ast_create_query.database = database;

	try
	{
		InterpreterCreateQuery(ast, context).executeLoadExisting();
	}
	catch (const Exception & e)
	{
		/// Исправление для ChunkMerger.
		if (e.code() == ErrorCodes::TABLE_ALREADY_EXISTS)
		{
			if (const auto id = dynamic_cast<const ASTFunction *>(ast_create_query.storage.get()))
			{
				if (id->name == "TinyLog" || id->name == "StripeLog")
				{
					tryLogCurrentException(__PRETTY_FUNCTION__);
					return;
				}
			}
		}

		throw;
	}
}


struct Table
{
	String database_name;
	String dir_name;
	String file_name;
};


static constexpr size_t MIN_TABLES_TO_PARALLEL_LOAD = 1;
static constexpr size_t PRINT_MESSAGE_EACH_N_TABLES = 256;
static constexpr size_t PRINT_MESSAGE_EACH_N_SECONDS = 5;
static constexpr size_t METADATA_FILE_BUFFER_SIZE = 32768;
static constexpr size_t TABLES_PARALLEL_LOAD_BUNCH_SIZE = 100;


static void loadTable(Context & context, const String & path, const Table & table)
{
	Logger * log = &Logger::get("loadTable");

	const String path_to_metadata = path + "/" + table.dir_name + "/" + table.file_name;

	String s;
	{
		char in_buf[METADATA_FILE_BUFFER_SIZE];
		ReadBufferFromFile in(path_to_metadata, METADATA_FILE_BUFFER_SIZE, -1, in_buf);
		WriteBufferFromString out(s);
		copyData(in, out);
	}

	/** Пустые файлы с метаданными образуются после грубого перезапуска сервера.
	  * Удаляем эти файлы, чтобы чуть-чуть уменьшить работу админов по запуску.
	  */
	if (s.empty())
	{
		LOG_ERROR(log, "File " << path_to_metadata << " is empty. Removing.");
		Poco::File(path_to_metadata).remove();
		return;
	}

	try
	{
		executeCreateQuery(s, context, table.database_name, path_to_metadata);
	}
	catch (const Exception & e)
	{
		throw Exception("Cannot create table from metadata file " + path_to_metadata + ", error: " + e.displayText() +
			", stack trace:\n" + e.getStackTrace().toString(),
			ErrorCodes::CANNOT_CREATE_TABLE_FROM_METADATA);
	}
}


static bool endsWith(const String & s, const char * suffix)
{
	return s.size() >= strlen(suffix) && 0 == s.compare(s.size() - strlen(suffix), strlen(suffix), suffix);
}


void loadMetadata(Context & context)
{
	Logger * log = &Logger::get("loadMetadata");

	/// Здесь хранятся определения таблиц
	String path = context.getPath() + "metadata";

	using Tables = std::vector<Table>;
	Tables tables;

	/** Часть таблиц должны быть загружены раньше других, так как используются в конструкторе этих других.
	  * Это таблицы, имя которых начинается на .inner.
	  * NOTE Это довольно криво. Можно сделать лучше.
	  */
	Tables tables_to_load_first;

	/// Цикл по базам данных
	Poco::DirectoryIterator dir_end;
	for (Poco::DirectoryIterator it(path); it != dir_end; ++it)
	{
		if (!it->isDirectory())
			continue;

		/// Для директории .svn
		if (it.name().at(0) == '.')
			continue;

		String database = unescapeForFileName(it.name());

		LOG_INFO(log, "Looking for tables in database " << database);
		executeCreateQuery("ATTACH DATABASE " + backQuoteIfNeed(database), context, database, it->path());

		/// Цикл по таблицам
		typedef std::vector<std::string> FileNames;
		FileNames file_names;

		for (Poco::DirectoryIterator jt(it->path()); jt != dir_end; ++jt)
		{
			/// Для директории .svn
			if (jt.name().at(0) == '.')
				continue;

			/// Есть файлы .sql.bak - пропускаем.
			if (endsWith(jt.name(), ".sql.bak"))
				continue;

			/// Нужные файлы имеют имена вида table_name.sql
			if (endsWith(jt.name(), ".sql"))
				file_names.push_back(jt.name());
			else
				throw Exception("Incorrect file extension: " + jt.name() + " in metadata directory " + it->path(), ErrorCodes::INCORRECT_FILE_NAME);
		}

		/** Таблицы быстрее грузятся, если их грузить в сортированном (по именам) порядке.
		  * Иначе (для файловой системы ext4) DirectoryIterator перебирает их в некотором порядке,
		  *  который не соответствует порядку создания таблиц и не соответствует порядку их расположения на диске.
		  */
		std::sort(file_names.begin(), file_names.end());

		for (const auto & name : file_names)
		{
			(0 == name.compare(0, strlen("%2Einner%2E"), "%2Einner%2E")
				? tables_to_load_first
				: tables).emplace_back(
				Table{
					.database_name = database,
					.dir_name = it.name(),
					.file_name = name});
		}

		LOG_INFO(log, "Found " << file_names.size() << " tables.");
	}

	if (!tables_to_load_first.empty())
	{
		LOG_INFO(log, "Loading inner tables for materialized views (total " << tables_to_load_first.size() << " tables).");

		for (const auto & table : tables_to_load_first)
			loadTable(context, path, table);
	}

	size_t total_tables = tables.size();
	LOG_INFO(log, "Total " << total_tables << " tables.");

	StopwatchWithLock watch;
	size_t tables_processed = 0;

	size_t num_threads = std::min(total_tables, SettingMaxThreads().getAutoValue());

	std::unique_ptr<boost::threadpool::pool> thread_pool;
	if (total_tables > MIN_TABLES_TO_PARALLEL_LOAD && num_threads > 1)
		thread_pool.reset(new boost::threadpool::pool(num_threads));

	size_t bunch_size = TABLES_PARALLEL_LOAD_BUNCH_SIZE;
	if (total_tables < bunch_size * num_threads)
		bunch_size = total_tables / num_threads;

	auto task_function = [&](Tables::const_iterator begin, Tables::const_iterator end)
	{
		for (Tables::const_iterator it = begin; it != end; ++it)
		{
			const Table & table = *it;

			/// Сообщения, чтобы было не скучно ждать, когда сервер долго загружается.
			if (__sync_add_and_fetch(&tables_processed, 1) % PRINT_MESSAGE_EACH_N_TABLES == 0
				|| watch.lockTestAndRestart(PRINT_MESSAGE_EACH_N_SECONDS))
			{
				LOG_INFO(log, std::fixed << std::setprecision(2) << tables_processed * 100.0 / total_tables << "%");
				watch.restart();
			}

			loadTable(context, path, table);
		}
	};

	/** packaged_task используются, чтобы исключения автоматически прокидывались в основной поток.
	  * Недостаток - исключения попадают в основной поток только после окончания работы всех task-ов.
	  */

	size_t num_bunches = (total_tables + bunch_size - 1) / bunch_size;
	std::vector<std::packaged_task<void()>> tasks(num_bunches);

	for (size_t i = 0; i < num_bunches; ++i)
	{
		auto begin = tables.begin() + i * bunch_size;
		auto end = (i + 1 == num_bunches)
			? tables.end()
			: (tables.begin() + (i + 1) * bunch_size);

		tasks[i] = std::packaged_task<void()>(std::bind(task_function, begin, end));

		if (thread_pool)
			thread_pool->schedule([i, &tasks]{ tasks[i](); });
		else
			tasks[i]();
	}

	if (thread_pool)
		thread_pool->wait();

	for (auto & task : tasks)
		task.get_future().get();
}


}
