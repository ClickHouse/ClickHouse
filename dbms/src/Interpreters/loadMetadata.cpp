#include <iomanip>
#include <thread>
#include <future>

#include <DB/Common/ThreadPool.h>

#include <Poco/DirectoryIterator.h>
#include <Poco/FileStream.h>

#include <DB/Parsers/ParserCreateQuery.h>
#include <DB/Parsers/ASTCreateQuery.h>
#include <DB/Parsers/parseQuery.h>

#include <DB/Interpreters/InterpreterCreateQuery.h>
#include <DB/Interpreters/loadMetadata.h>

#include <DB/IO/ReadBufferFromFile.h>
#include <DB/Common/escapeForFileName.h>

#include <DB/Common/Stopwatch.h>


namespace DB
{

static void executeCreateQuery(
	const String & query,
	Context & context,
	const String & database,
	const String & file_name,
	ThreadPool & pool,
	bool has_force_restore_data_flag)
{
	ParserCreateQuery parser;
	ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "in file " + file_name);

	ASTCreateQuery & ast_create_query = typeid_cast<ASTCreateQuery &>(*ast);
	ast_create_query.attach = true;
	ast_create_query.database = database;

	InterpreterCreateQuery interpreter(ast, context);
	interpreter.setDatabaseLoadingThreadpool(pool);
	interpreter.setForceRestoreData(has_force_restore_data_flag);
	interpreter.execute();
}


void loadMetadata(Context & context)
{
	String path = context.getPath() + "metadata";

	/** There may exist 'force_restore_data' file, that means,
	  *  skip safety threshold on difference of data parts while initializing tables.
	  * This file is deleted after successful loading of tables.
	  * (flag is "one-shot")
	  */
	Poco::File force_restore_data_flag_file(context.getFlagsPath() + "force_restore_data");
	bool has_force_restore_data_flag = force_restore_data_flag_file.exists();

	/// For parallel tables loading.
	ThreadPool thread_pool(SettingMaxThreads().getAutoValue());

	/// Loop over databases.
	Poco::DirectoryIterator dir_end;
	for (Poco::DirectoryIterator it(path); it != dir_end; ++it)
	{
		if (!it->isDirectory())
			continue;

		/// For '.svn', '.gitignore' directory and similar.
		if (it.name().at(0) == '.')
			continue;

		String database = unescapeForFileName(it.name());

		/// There may exist .sql file with database creation statement.
		/// Or, if it is absent, then database with default engine is created.

		String database_attach_query;
		String database_metadata_file = it->path() + ".sql";

		if (Poco::File(database_metadata_file).exists())
		{
			ReadBufferFromFile in(database_metadata_file, 1024);
			readStringUntilEOF(database_attach_query, in);
		}
		else
			database_attach_query = "ATTACH DATABASE " + backQuoteIfNeed(database);

		executeCreateQuery(database_attach_query, context, database, it->path(), thread_pool, has_force_restore_data_flag);
	}

	thread_pool.wait();

	if (has_force_restore_data_flag)
		force_restore_data_flag_file.remove();
}

}
