#include <DB/DatabaseEngines/DatabaseEngineOrdinary.h>
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


DatabaseEngineOrdinary::DatabaseEngineOrdinary(const String & path_, boost::threadpool::pool & thread_pool_)
	: path(path_)
{
	/// TODO Удаление файлов .sql.tmp
}


bool DatabaseEngineOrdinary::isTableExist(const String & name) const
{
	std::lock_guard<std::mutex> lock(mutex);
	return tables.count(name);
}


StoragePtr DatabaseEngineOrdinary::tryGetTable(const String & name)
{
	std::lock_guard<std::mutex> lock(mutex);
	auto it = tables.find(name);
	if (it == tables.end())
		return {};
	return it->second;
}


/// Копирует список таблиц. Таким образом, итерируется по их снапшоту.
class DatabaseEngineOrdinaryIterator : public IDatabaseIterator
{
private:
	Tables tables;
	Tables::iterator it;

public:
	DatabaseEngineOrdinaryIterator(Tables & tables_) : tables(tables_) {}

	StoragePtr next() override
	{
		if (it == tables.end())
			return {};

		auto res = it->second;
		++it;
		return res;
	}
};


DatabaseIteratorPtr DatabaseEngineOrdinary::getIterator()
{
	std::lock_guard<std::mutex> lock(mutex);
	return std::make_shared<DatabaseEngineOrdinaryIterator>(tables);
}


void DatabaseEngineOrdinary::addTable(const String & name, StoragePtr & table, const ASTPtr & query, const String & engine)
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

	/// Если запрос ATTACH, то считается, что метаданные уже есть.
	bool need_write_metadata = !create.attach;

	if (need_write_metadata)
	{
		std::lock_guard<std::mutex> lock(mutex);
		if (tables.count(name))
			throw Exception("Table " + name + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);
	}

	String table_name_escaped;
	String table_metadata_tmp_path;
	String table_metadata_path;
	String statement;

	if (need_write_metadata)
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

		table_name_escaped = escapeForFileName(name);
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
			if (!tables.emplace(name, table).second)
				throw Exception("Table " + name + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);
		}

		if (need_write_metadata)
			Poco::File(table_metadata_tmp_path).renameTo(table_metadata_path);
	}
	catch (...)
	{
		if (need_write_metadata)
			Poco::File(table_metadata_tmp_path).remove();

		throw;
	}
}


StoragePtr DatabaseEngineOrdinary::detachTable(const String & name, bool remove_metadata)
{
	StoragePtr res;

	{
		std::lock_guard<std::mutex> lock(mutex);
		auto it = tables.find(name);
		if (it == tables.end())
			throw Exception("Table " + name + " doesn't exist.", ErrorCodes::TABLE_ALREADY_EXISTS);
		res = it->second;
		tables.erase(it);
	}

	if (remove_metadata)
	{
		String table_name_escaped = escapeForFileName(name);
		String table_metadata_path = path + "/" + table_name_escaped;

		Poco::File(table_metadata_path).remove();
	}

	return res;
}


ASTPtr DatabaseEngineOrdinary::getCreateQuery(const String & name) const
{
	String table_name_escaped = escapeForFileName(name);
	String table_metadata_path = path + "/" + table_name_escaped;

	if (!Poco::File(table_metadata_path).exists())
	{
/*TODO	StoragePtr table = tryGetTable(name);

		if (!table)
			throw Exception("Table " + name + " doesn't exist", ErrorCodes::UNKNOWN_TABLE);

		try
		{
			/// Если файл .sql не предусмотрен (например, для таблиц типа ChunkRef), то движок может сам предоставить запрос CREATE.
			return table->getCustomCreateQuery(*this);
		}
		catch (...)
		{*/
			throw Exception("Metadata file " + table_metadata_path + " for table " + name + " doesn't exist.",
				ErrorCodes::TABLE_METADATA_DOESNT_EXIST);
		//}
	}

	StringPtr query = new String();
	{
		ReadBufferFromFile in(table_metadata_path, 4096);
		WriteBufferFromString out(*query);
		copyData(in, out);
	}

	ParserCreateQuery parser;
	ASTPtr ast = parseQuery(parser, query->data(), query->data() + query->size(), "in file " + table_metadata_path);

	ASTCreateQuery & ast_create_query = typeid_cast<ASTCreateQuery &>(*ast);
	ast_create_query.attach = false;
	ast_create_query.database = db;
	ast_create_query.query_string = query;

	return ast;
}

}
