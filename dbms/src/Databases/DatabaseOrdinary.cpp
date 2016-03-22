#include <future>
#include <Poco/DirectoryIterator.h>

#include <DB/Databases/DatabaseOrdinary.h>
#include <DB/Common/escapeForFileName.h>
#include <DB/Parsers/ASTCreateQuery.h>
#include <DB/Parsers/formatAST.h>
#include <DB/Parsers/parseQuery.h>
#include <DB/Parsers/ParserCreateQuery.h>
#include <DB/Storages/StorageFactory.h>
#include <DB/Interpreters/InterpreterCreateQuery.h>
#include <DB/IO/WriteBufferFromFile.h>
#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/copyData.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int TABLE_ALREADY_EXISTS;
	extern const int UNKNOWN_TABLE;
	extern const int TABLE_METADATA_DOESNT_EXIST;
	extern const int CANNOT_CREATE_TABLE_FROM_METADATA;
	extern const int INCORRECT_FILE_NAME;
}


static constexpr size_t PRINT_MESSAGE_EACH_N_TABLES = 256;
static constexpr size_t PRINT_MESSAGE_EACH_N_SECONDS = 5;
static constexpr size_t METADATA_FILE_BUFFER_SIZE = 32768;
static constexpr size_t TABLES_PARALLEL_LOAD_BUNCH_SIZE = 100;


static void executeCreateQuery(
	const String & query,
	Context & context,
	DatabaseOrdinary & database,
	const String & database_name,
	const String & database_data_path,
	const String & table_metadata_path)
{
	ParserCreateQuery parser;
	ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "in file " + table_metadata_path);

	ASTCreateQuery & ast_create_query = typeid_cast<ASTCreateQuery &>(*ast);
	ast_create_query.attach = true;
	ast_create_query.database = database_name;

	/// Не используем напрямую InterpreterCreateQuery::execute, так как:
	/// - база данных ещё не создана;
	/// - код проще, так как запрос уже приведён к подходящему виду.

	InterpreterCreateQuery::ColumnsInfo columns_info = InterpreterCreateQuery::getColumnsInfo(ast_create_query.columns, context);

	String storage_name;

	if (ast_create_query.is_view)
		storage_name = "View";
	else if (ast_create_query.is_materialized_view)
		storage_name = "MaterializedView";
	else
		storage_name = typeid_cast<ASTFunction &>(*ast_create_query.storage).name;

	StoragePtr res = StorageFactory::instance().get(
		storage_name, database_data_path, ast_create_query.table, database_name, context,
		context.getGlobalContext(), ast, columns_info.columns,
		columns_info.materialized_columns, columns_info.alias_columns, columns_info.column_defaults,
		true);

	database.attachTable(ast_create_query.table, res);
}


static void loadTable(
	Context & context,
	const String & database_metadata_path,
	DatabaseOrdinary & database,
	const String & database_name,
	const String & database_data_path,
	const String & file_name)
{
	Logger * log = &Logger::get("loadTable");

	const String table_metadata_path = database_metadata_path + "/" + file_name;

	String s;
	{
		char in_buf[METADATA_FILE_BUFFER_SIZE];
		ReadBufferFromFile in(table_metadata_path, METADATA_FILE_BUFFER_SIZE, -1, in_buf);
		WriteBufferFromString out(s);
		copyData(in, out);
	}

	/** Пустые файлы с метаданными образуются после грубого перезапуска сервера.
	  * Удаляем эти файлы, чтобы чуть-чуть уменьшить работу админов по запуску.
	  */
	if (s.empty())
	{
		LOG_ERROR(log, "File " << table_metadata_path << " is empty. Removing.");
		Poco::File(table_metadata_path).remove();
		return;
	}

	try
	{
		executeCreateQuery(s, context, database, database_name, database_data_path, table_metadata_path);
	}
	catch (const Exception & e)
	{
		throw Exception("Cannot create table from metadata file " + table_metadata_path + ", error: " + e.displayText() +
			", stack trace:\n" + e.getStackTrace().toString(),
			ErrorCodes::CANNOT_CREATE_TABLE_FROM_METADATA);
	}
}


static bool endsWith(const String & s, const char * suffix)
{
	return s.size() >= strlen(suffix) && 0 == s.compare(s.size() - strlen(suffix), strlen(suffix), suffix);
}



DatabaseOrdinary::DatabaseOrdinary(
	const String & name_, const String & path_, Context & context, boost::threadpool::pool * thread_pool)
	: name(name_), path(path_)
{
	log = &Logger::get("DatabaseOrdinary (" + name + ")");

	using Tables = std::vector<String>;
	Tables tables;

	/** Часть таблиц должны быть загружены раньше других, так как используются в конструкторе этих других.
	  * Это таблицы, имя которых начинается на .inner.
	  * NOTE Это довольно криво. Можно сделать лучше.
	  */
	Tables tables_to_load_first;

	/// Цикл по таблицам
	using FileNames = std::vector<std::string>;
	FileNames file_names;

	Poco::DirectoryIterator dir_end;
	for (Poco::DirectoryIterator dir_it(path); dir_it != dir_end; ++dir_it)
	{
		/// Для директории .svn
		if (dir_it.name().at(0) == '.')
			continue;

		/// Есть файлы .sql.bak - пропускаем.
		if (endsWith(dir_it.name(), ".sql.bak"))
			continue;

		/// Есть файлы .sql.tmp - удаляем.
		if (endsWith(dir_it.name(), ".sql.tmp"))
		{
			LOG_INFO(log, "Removing file " << dir_it->path());
			Poco::File(dir_it->path()).remove();
			continue;
		}

		/// Нужные файлы имеют имена вида table_name.sql
		if (endsWith(dir_it.name(), ".sql"))
			file_names.push_back(dir_it.name());
		else
			throw Exception("Incorrect file extension: " + dir_it.name() + " in metadata directory " + path,
				ErrorCodes::INCORRECT_FILE_NAME);
	}

	/** Таблицы быстрее грузятся, если их грузить в сортированном (по именам) порядке.
	  * Иначе (для файловой системы ext4) DirectoryIterator перебирает их в некотором порядке,
	  *  который не соответствует порядку создания таблиц и не соответствует порядку их расположения на диске.
	  */
	std::sort(file_names.begin(), file_names.end());

	for (const auto & file_name : file_names)
	{
		(0 == file_name.compare(0, strlen("%2Einner%2E"), "%2Einner%2E")
			? tables_to_load_first
			: tables).emplace_back(file_name);
	}

	size_t total_tables = tables.size();
	LOG_INFO(log, "Total " << total_tables << " tables.");

	String data_path = context.getPath() + "/data/" + escapeForFileName(name) + "/";

	if (!tables_to_load_first.empty())
	{
		LOG_INFO(log, "Loading inner tables for materialized views (total " << tables_to_load_first.size() << " tables).");

		for (const auto & table : tables_to_load_first)
			loadTable(context, path, *this, name, data_path, table);
	}

	StopwatchWithLock watch;
	size_t tables_processed = 0;

	auto task_function = [&](Tables::const_iterator begin, Tables::const_iterator end)
	{
		for (Tables::const_iterator it = begin; it != end; ++it)
		{
			const String & table = *it;

			/// Сообщения, чтобы было не скучно ждать, когда сервер долго загружается.
			if (__sync_add_and_fetch(&tables_processed, 1) % PRINT_MESSAGE_EACH_N_TABLES == 0
				|| watch.lockTestAndRestart(PRINT_MESSAGE_EACH_N_SECONDS))
			{
				LOG_INFO(log, std::fixed << std::setprecision(2) << tables_processed * 100.0 / total_tables << "%");
				watch.restart();
			}

			loadTable(context, path, *this, name, data_path, table);
		}
	};

	/** packaged_task используются, чтобы исключения автоматически прокидывались в основной поток.
	  * Недостаток - исключения попадают в основной поток только после окончания работы всех task-ов.
	  */

	const size_t bunch_size = TABLES_PARALLEL_LOAD_BUNCH_SIZE;
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


bool DatabaseOrdinary::isTableExist(const String & table_name) const
{
	std::lock_guard<std::mutex> lock(mutex);
	return tables.count(table_name);
}


StoragePtr DatabaseOrdinary::tryGetTable(const String & table_name)
{
	std::lock_guard<std::mutex> lock(mutex);
	auto it = tables.find(table_name);
	if (it == tables.end())
		return {};
	return it->second;
}


/// Копирует список таблиц. Таким образом, итерируется по их снапшоту.
class DatabaseOrdinaryIterator : public IDatabaseIterator
{
private:
	Tables tables;
	Tables::iterator it;

public:
	DatabaseOrdinaryIterator(Tables & tables_)
		: tables(tables_), it(tables.begin()) {}

	void next() override
	{
		++it;
	}

	bool isValid() const override
	{
		return it != tables.end();
	}

	const String & name() const override
	{
		return it->first;
	}

	StoragePtr & table() const
	{
		return it->second;
	}
};


DatabaseIteratorPtr DatabaseOrdinary::getIterator()
{
	std::lock_guard<std::mutex> lock(mutex);
	return std::make_unique<DatabaseOrdinaryIterator>(tables);
}


bool DatabaseOrdinary::empty() const
{
	std::lock_guard<std::mutex> lock(mutex);
	return tables.empty();
}


void DatabaseOrdinary::attachTable(const String & table_name, const StoragePtr & table)
{
	/// Добавляем таблицу в набор.
	std::lock_guard<std::mutex> lock(mutex);
	if (!tables.emplace(table_name, table).second)
		throw Exception("Table " + name + "." + table_name + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);
}


void DatabaseOrdinary::createTable(const String & table_name, const StoragePtr & table, const ASTPtr & query, const String & engine)
{
	/// Создаём файл с метаданными, если нужно - если запрос не ATTACH.
	/// В него записывается запрос на ATTACH таблицы.

	/** Код исходит из допущения, что во всех потоках виден один и тот же порядок действий:
	  * - создание файла .sql.tmp;
	  * - добавление таблицы в tables;
	  * - переименование .sql.tmp в .sql.
	  */

	/// NOTE Возможен race condition, если таблицу с одним именем одновременно создают с помощью CREATE и с помощью ATTACH.

	ASTPtr query_clone = query->clone();
	ASTCreateQuery & create = typeid_cast<ASTCreateQuery &>(*query_clone.get());

	{
		std::lock_guard<std::mutex> lock(mutex);
		if (tables.count(table_name))
			throw Exception("Table " + name + "." + table_name + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);
	}

	String table_name_escaped = escapeForFileName(table_name);
	String table_metadata_tmp_path = path + "/" + table_name_escaped + ".sql.tmp";
	String table_metadata_path = path + "/" + table_name_escaped + ".sql";
	String statement;

	{
		/// Удаляем из запроса всё, что не нужно для ATTACH.
		create.attach = true;
		create.database.clear();
		create.as_database.clear();
		create.as_table.clear();
		create.if_not_exists = false;
		create.is_populate = false;

		/// Для engine VIEW необходимо сохранить сам селект запрос, для остальных - наоборот
		if (engine != "View" && engine != "MaterializedView")
			create.select = nullptr;

		std::ostringstream statement_stream;
		formatAST(create, statement_stream, 0, false);
		statement_stream << '\n';
		statement = statement_stream.str();

		/// Гарантирует, что таблица не создаётся прямо сейчас.
		WriteBufferFromFile out(table_metadata_tmp_path, statement.size(), O_WRONLY | O_CREAT | O_EXCL);
		writeString(statement, out);
		out.next();
		out.sync();
		out.close();
	}

	try
	{
		/// Добавляем таблицу в набор.
		{
			std::lock_guard<std::mutex> lock(mutex);
			if (!tables.emplace(table_name, table).second)
				throw Exception("Table " + name + "." + table_name + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);
		}

		/// Если запрос ATTACH, и метаданные таблицы уже существуют
		/// (то есть, ATTACH сделан после DETACH), то rename атомарно заменяет старый файл новым.
		Poco::File(table_metadata_tmp_path).renameTo(table_metadata_path);
	}
	catch (...)
	{
		Poco::File(table_metadata_tmp_path).remove();
		throw;
	}
}


StoragePtr DatabaseOrdinary::detachTable(const String & table_name)
{
	StoragePtr res;

	{
		std::lock_guard<std::mutex> lock(mutex);
		auto it = tables.find(table_name);
		if (it == tables.end())
			throw Exception("Table " + name + "." + table_name + " doesn't exist.", ErrorCodes::TABLE_ALREADY_EXISTS);
		res = it->second;
		tables.erase(it);
	}

	return res;
}


StoragePtr DatabaseOrdinary::removeTable(const String & table_name)
{
	StoragePtr res = detachTable(table_name);

	String table_name_escaped = escapeForFileName(table_name);
	String table_metadata_path = path + "/" + table_name_escaped + ".sql";

	try
	{
		Poco::File(table_metadata_path).remove();
	}
	catch (...)
	{
		attachTable(table_name, res);
		throw;
	}

	return res;
}


static ASTPtr getCreateQueryImpl(const String & path, const String & table_name)
{
	String table_name_escaped = escapeForFileName(table_name);
	String table_metadata_path = path + "/" + table_name_escaped + ".sql";

	String query;
	{
		ReadBufferFromFile in(table_metadata_path, 4096);
		WriteBufferFromString out(query);
		copyData(in, out);
	}

	ParserCreateQuery parser;
	return parseQuery(parser, query.data(), query.data() + query.size(), "in file " + table_metadata_path);
}


void DatabaseOrdinary::renameTable(
	const Context & context, const String & table_name, IDatabase & to_database, const String & to_table_name)
{
	DatabaseOrdinary * to_database_concrete = typeid_cast<DatabaseOrdinary *>(&to_database);

	if (!to_database_concrete)
		throw Exception("Moving tables between databases of different engines is not supported", ErrorCodes::NOT_IMPLEMENTED);

	StoragePtr table = tryGetTable(table_name);

	if (!table)
		throw Exception("Table " + name + "." + table_name + " doesn't exist.", ErrorCodes::TABLE_ALREADY_EXISTS);

	/// Уведомляем таблицу о том, что она переименовывается. Если таблица не поддерживает переименование - кинется исключение.
	try
	{
		table->rename(context.getPath() + "/data/" + escapeForFileName(to_database_concrete->name) + "/",
			to_database_concrete->name,
			to_table_name);
	}
	catch (const Poco::Exception & e)
	{
		/// Более хорошая диагностика.
		throw Exception{e};
	}

	ASTPtr ast = getCreateQueryImpl(path, table_name);
	ASTCreateQuery & ast_create_query = typeid_cast<ASTCreateQuery &>(*ast);
	ast_create_query.table = to_table_name;

	/// NOTE Неатомарно.
	to_database_concrete->createTable(to_table_name, table, ast, table->getName());
	removeTable(table_name);
}


ASTPtr DatabaseOrdinary::getCreateQuery(const String & table_name) const
{
	ASTPtr ast = getCreateQueryImpl(path, table_name);

	ASTCreateQuery & ast_create_query = typeid_cast<ASTCreateQuery &>(*ast);
	ast_create_query.attach = false;
	ast_create_query.database = name;

	return ast;
}


void DatabaseOrdinary::shutdown()
{
	std::lock_guard<std::mutex> lock(mutex);

	for (auto & table : tables)
		table.second->shutdown();

	tables.clear();
}

}
