#include <DB/Databases/DatabaseOrdinary.h>
#include <DB/Common/escapeForFileName.h>
#include <DB/Parsers/ASTCreateQuery.h>
#include <DB/Parsers/formatAST.h>
#include <DB/Parsers/parseQuery.h>
#include <DB/Parsers/ParserCreateQuery.h>
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
}


DatabaseOrdinary::DatabaseOrdinary(const String & name_, const String & path_, boost::threadpool::pool * thread_pool_)
	: name(name_), path(path_)
{
	/// TODO Удаление файлов .sql.tmp
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
	DatabaseOrdinaryIterator(Tables & tables_) : tables(tables_) {}

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

	String table_name_escaped;
	String table_metadata_tmp_path;
	String table_metadata_path;
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

		table_name_escaped = escapeForFileName(table_name);
		table_metadata_tmp_path = path + "/" + table_name_escaped + ".sql.tmp";
		table_metadata_path = path + "/" + table_name_escaped;

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
	String table_metadata_path = path + "/" + table_name_escaped;

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
	String table_metadata_path = path + "/" + table_name_escaped;

	String query;
	{
		ReadBufferFromFile in(table_metadata_path, 4096);
		WriteBufferFromString out(query);
		copyData(in, out);
	}

	ParserCreateQuery parser;
	return parseQuery(parser, query.data(), query.data() + query.size(), "in file " + table_metadata_path);
}


void DatabaseOrdinary::renameTable(const String & table_name, IDatabase & to_database, const String & to_table_name)
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
		table->rename(path + "data/" + escapeForFileName(to_database_concrete->name) + "/",
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
