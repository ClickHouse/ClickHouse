#include <iomanip>
#include <thread>
#include <future>

#include <statdaemons/threadpool.hpp>

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

#include <statdaemons/Stopwatch.h>


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


void loadMetadata(Context & context)
{
	/// Создадим все таблицы атомарно (иначе какой-нибудь движок таблицы может успеть в фоновом потоке попытаться выполнить запрос).
	Poco::ScopedLock<Poco::Mutex> lock(context.getMutex());

	Logger * log = &Logger::get("loadMetadata");

	/// Здесь хранятся определения таблиц
	String path = context.getPath() + "metadata";

	struct Table
	{
		String database_name;
		String dir_name;
		String file_name;
	};

	using Tables = std::vector<Table>;
	Tables tables;

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

			/// Файлы имеют имена вида table_name.sql
			if (jt.name().compare(jt.name().size() - 4, 4, ".sql"))
				throw Exception("Incorrect file extension: " + jt.name() + " in metadata directory " + it->path(), ErrorCodes::INCORRECT_FILE_NAME);

			file_names.push_back(jt.name());
		}

		/** Таблицы быстрее грузятся, если их грузить в сортированном (по именам) порядке.
		  * Иначе (для файловой системы ext4) DirectoryIterator перебирает их в некотором порядке,
		  *  который не соответствует порядку создания таблиц и не соответствует порядку их расположения на диске.
		  */
		std::sort(file_names.begin(), file_names.end());

		for (const auto & name : file_names)
			tables.emplace_back(Table{
				.database_name = database,
				.dir_name = it.name(),
				.file_name = name});

		LOG_INFO(log, "Found " << file_names.size() << " tables.");
	}

	size_t total_tables = tables.size();
	LOG_INFO(log, "Total " << total_tables << " tables.");

	StopwatchWithLock watch;
	size_t tables_processed = 0;

	static constexpr size_t MIN_TABLES_TO_PARALLEL_LOAD = 1000;
	static constexpr size_t PRINT_MESSAGE_EACH_N_TABLES = 256;
	static constexpr size_t PRINT_MESSAGE_EACH_N_SECONDS = 5;
	static constexpr size_t METADATA_FILE_BUFFER_SIZE = 32768;
	static constexpr size_t TABLES_PARALLEL_LOAD_BUNCH_SIZE = 100;

	std::unique_ptr<boost::threadpool::pool> thread_pool;
	if (total_tables > MIN_TABLES_TO_PARALLEL_LOAD)
	{
		LOG_INFO(log, "Will load in multiple threads");
		thread_pool.reset(new boost::threadpool::pool(SettingMaxThreads().getAutoValue()));
	}

	auto task_function = [&](Tables::const_iterator begin, Tables::const_iterator end)
	{
		for (Tables::const_iterator it = begin; it != end; ++it)
		{
			const Table & table = *it;
			const String path_to_metadata = path + "/" + table.dir_name + "/" + table.file_name;

			/// Сообщения, чтобы было не скучно ждать, когда сервер долго загружается.
			if (__sync_add_and_fetch(&tables_processed, 1) % PRINT_MESSAGE_EACH_N_TABLES == 0
				|| watch.lockTestAndRestart(PRINT_MESSAGE_EACH_N_SECONDS))
			{
				LOG_INFO(log, std::fixed << std::setprecision(2) << tables_processed * 100.0 / total_tables << "%");
				watch.restart();
			}

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
				continue;
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
	};

	/** packaged_task используются, чтобы исключения автоматически прокидывались в основной поток.
	  * Недостаток - исключения попадают в основной поток только после окончания работы всех task-ов.
	  */

	size_t num_bunches = (total_tables + TABLES_PARALLEL_LOAD_BUNCH_SIZE - 1) / TABLES_PARALLEL_LOAD_BUNCH_SIZE;
	std::vector<std::packaged_task<void()>> tasks(num_bunches);

	for (size_t i = 0; i < num_bunches; ++i)
	{
		auto begin = tables.begin() + i * TABLES_PARALLEL_LOAD_BUNCH_SIZE;
		auto end = (i + 1 == num_bunches)
			? tables.end()
			: (tables.begin() + (i + 1) * TABLES_PARALLEL_LOAD_BUNCH_SIZE);

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
