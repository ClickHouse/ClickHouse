#include <iomanip>
#include <thread>
#include <future>

#include <threadpool.hpp>

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

static void executeCreateQuery(
	const String & query,
	Context & context,
	const String & database,
	const String & file_name,
	boost::threadpool::pool & pool)
{
	ParserCreateQuery parser;
	ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "in file " + file_name);

	ASTCreateQuery & ast_create_query = typeid_cast<ASTCreateQuery &>(*ast);
	ast_create_query.attach = true;
	ast_create_query.database = database;

	InterpreterCreateQuery interpreter(ast, context);
	interpreter.setDatabaseLoadingThreadpool(pool);
	interpreter.execute();
}


void loadMetadata(Context & context)
{
	String path = context.getPath() + "metadata";

	/// Используется для параллельной загрузки таблиц.
	boost::threadpool::pool thread_pool(SettingMaxThreads().getAutoValue());

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

		/// Для базы данных может быть расположен .sql файл, где описан запрос на её создание.
		/// А если такого файла нет, то создаётся база данных с движком по-умолчанию.

		String database_attach_query;
		String database_metadata_file = it->path() + ".sql";

		if (Poco::File(database_metadata_file).exists())
		{
			ReadBufferFromFile in(database_metadata_file, 1024);
			WriteBufferFromString out(database_attach_query);
			copyData(in, out);
		}
		else
			database_attach_query = "ATTACH DATABASE " + backQuoteIfNeed(database);

		executeCreateQuery(database_attach_query, context, database, it->path(), thread_pool);
	}

	thread_pool.wait();
}

}
