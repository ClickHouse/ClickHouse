#include <iomanip>

#include <Poco/DirectoryIterator.h>
#include <Poco/FileStream.h>

#include <DB/Parsers/ParserCreateQuery.h>
#include <DB/Parsers/ASTCreateQuery.h>

#include <DB/Interpreters/InterpreterCreateQuery.h>
#include <DB/Interpreters/loadMetadata.h>

#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/copyData.h>

#include <statdaemons/Stopwatch.h>


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
			+ toString(pos - begin) + ": "
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

		LOG_INFO(&Logger::get("loadMetadata"), "Loading database " << it.name());

		executeCreateQuery("ATTACH DATABASE " + it.name(), context, it.name(), it->path());

		/// Цикл по таблицам
		typedef std::vector<std::string> Tables;
		Tables tables;
			
		for (Poco::DirectoryIterator jt(it->path()); jt != dir_end; ++jt)
		{
			if (jt.name() == ".svn")
				continue;
			
			/// Файлы имеют имена вида table_name.sql
			if (jt.name().compare(jt.name().size() - 4, 4, ".sql"))
				throw Exception("Incorrect file extension: " + jt.name() + " in metadata directory " + it->path(), ErrorCodes::INCORRECT_FILE_NAME);

			tables.push_back(jt->path());
		}

		LOG_INFO(&Logger::get("loadMetadata"), "Found " << tables.size() << " tables.");

		/** Таблицы быстрее грузятся, если их грузить в сортированном (по именам) порядке.
		  * Иначе (для файловой системы ext4) DirectoryIterator перебирает их в некотором порядке,
		  *  который не соответствует порядку создания таблиц и не соответствует порядку их расположения на диске.
		  */
		std::sort(tables.begin(), tables.end());

		Stopwatch watch;

		for (size_t j = 0, size = tables.size(); j < size; ++j)
		{
			/// Сообщения, чтобы было не скучно ждать, когда сервер долго загружается.
			if (j % 256 == 0 && watch.elapsedSeconds() > 5)
			{
				LOG_INFO(&Logger::get("loadMetadata"), std::fixed << std::setprecision(2) << j * 100.0 / size << "%");
				watch.restart();
			}
			
			String s;
			{
				static const size_t in_buf_size = 32768;
				char in_buf[in_buf_size];
				ReadBufferFromFile in(tables[j], 32768, in_buf);
				WriteBufferFromString out(s);
				copyData(in, out);
			}

			try
			{
				executeCreateQuery(s, context, it.name(), tables[j]);
			}
			catch (const DB::Exception & e)
			{
				throw Exception("Cannot create table from metadata file " + tables[j] + ", error: " + e.displayText(),
					ErrorCodes::CANNOT_CREATE_TABLE_FROM_METADATA);
			}
		}
	}
}


}
