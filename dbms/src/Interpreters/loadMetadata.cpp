#include <Poco/DirectoryIterator.h>
#include <Poco/FileStream.h>

#include <DB/Parsers/ParserCreateQuery.h>
#include <DB/Parsers/ASTCreateQuery.h>

#include <DB/Interpreters/InterpreterCreateQuery.h>
#include <DB/Interpreters/loadMetadata.h>

#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/copyData.h>


namespace DB
{


static void executeCreateQuery(const String & query, Context & context, const String & database, const String & file_name)
{
	const char * begin = query.data();
	const char * end = begin + query.size();
	const char * pos = begin;

	ParserCreateQuery parser;
	ASTPtr ast;
	String expected;
	bool parse_res = parser.parse(pos, end, ast, expected);

	/// Распарсенный запрос должен заканчиваться на конец входных данных или на точку с запятой.
	if (!parse_res || (pos != end && *pos != ';'))
		throw DB::Exception("Syntax error while executing query from file " + file_name + ": failed at position "
			+ Poco::NumberFormatter::format(pos - begin) + ": "
			+ std::string(pos, std::min(SHOW_CHARS_ON_SYNTAX_ERROR, end - pos))
			+ ", expected " + (parse_res ? "end of query" : expected) + ".",
			DB::ErrorCodes::SYNTAX_ERROR);

	ASTCreateQuery & ast_create_query = dynamic_cast<ASTCreateQuery &>(*ast);
	ast_create_query.attach = true;
	ast_create_query.database = database;

	InterpreterCreateQuery interpreter(ast, context);
	interpreter.execute(true);
}
	

void loadMetadata(Context & context)
{
	/// Создадим все таблицы атомарно (иначе какой-нибудь движок таблицы может успеть в фоновом потоке попытаться выполнить запрос).
	Poco::ScopedLock<Poco::Mutex> lock(context.getMutex());
	
	/// Здесь хранятся определения таблиц
	String path = context.getPath() + "metadata";

	/// Цикл по базам данных
	Poco::DirectoryIterator dir_end;
	for (Poco::DirectoryIterator it(path); it != dir_end; ++it)
	{
		if (!it->isDirectory())
			continue;

		/// Для директории .svn
		if (it.name().at(0) == '.')
			continue;

		executeCreateQuery("ATTACH DATABASE " + it.name(), context, it.name(), it->path());

		/// Цикл по таблицам
		for (Poco::DirectoryIterator jt(it->path()); jt != dir_end; ++jt)
		{
			if (jt.name() == ".svn")
				continue;
			
			/// Файлы имеют имена вида table_name.sql
			if (jt.name().compare(jt.name().size() - 4, 4, ".sql"))
				throw Exception("Incorrect file extension: " + jt.name() + " in metadata directory " + it->path(), ErrorCodes::INCORRECT_FILE_NAME);

			String s;
			{
				ReadBufferFromFile in(jt->path(), 32768);
				WriteBufferFromString out(s);
				copyData(in, out);
			}

			try
			{
				executeCreateQuery(s, context, it.name(), jt->path());
			}
			catch (const DB::Exception & e)
			{
				throw Exception("Cannot create table from metadata file " + jt->path() + ", error: " + e.displayText(),
					ErrorCodes::CANNOT_CREATE_TABLE_FROM_METADATA);
			}
		}
	}
}


}
