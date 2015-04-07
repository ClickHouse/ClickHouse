#include <Poco/File.h>
#include <Poco/FileStream.h>

#include <DB/Common/escapeForFileName.h>

#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/copyData.h>

#include <DB/Parsers/ASTRenameQuery.h>
#include <DB/Parsers/ASTCreateQuery.h>
#include <DB/Parsers/ParserCreateQuery.h>
#include <DB/Parsers/formatAST.h>

#include <DB/Interpreters/InterpreterRenameQuery.h>



namespace DB
{


InterpreterRenameQuery::InterpreterRenameQuery(ASTPtr query_ptr_, Context & context_)
	: query_ptr(query_ptr_), context(context_)
{
}


void InterpreterRenameQuery::execute()
{
	String path = context.getPath();
	String current_database = context.getCurrentDatabase();

	ASTRenameQuery & rename = typeid_cast<ASTRenameQuery &>(*query_ptr);

	/** Если в процессе переименования произошла ошибка, то может быть переименована только часть таблиц,
	  *  или состояние может стать неконсистентным. (Это имеет смысл исправить.)
	  */

	for (ASTRenameQuery::Elements::const_iterator it = rename.elements.begin(); it != rename.elements.end(); ++it)
	{
		String from_database_name = it->from.database.empty() ? current_database : it->from.database;
		String from_database_name_escaped = escapeForFileName(from_database_name);
		String from_table_name = it->from.table;
		String from_table_name_escaped = escapeForFileName(from_table_name);
		String from_metadata_path = path + "metadata/" + from_database_name_escaped + "/"
			+ (!from_table_name.empty() ?  from_table_name_escaped + ".sql" : "");

		String to_database_name = it->to.database.empty() ? current_database : it->to.database;
		String to_database_name_escaped = escapeForFileName(to_database_name);
		String to_table_name = it->to.table;
		String to_table_name_escaped = escapeForFileName(to_table_name);
		String to_metadata_path = path + "metadata/" + to_database_name_escaped + "/"
			+ (!to_table_name.empty() ?  to_table_name_escaped + ".sql" : "");

		/// Заблокировать таблицу нужно при незаблокированном контексте.
		StoragePtr table = context.getTable(from_database_name, from_table_name);
		auto table_lock = table->lockForAlter();

		/** Все таблицы переименовываются под глобальной блокировкой. */
		Poco::ScopedLock<Poco::Mutex> lock(context.getMutex());

		context.assertTableDoesntExist(to_database_name, to_table_name);

		/// Уведомляем таблицу о том, что она переименовывается. Если таблица не поддерживает переименование - кинется исключение.

		table->rename(path + "data/" + to_database_name_escaped + "/", to_database_name, to_table_name);

		/// Пишем новый файл с метаданными.
		{
			String create_query;
			{
				ReadBufferFromFile in(from_metadata_path, 1024);
				WriteBufferFromString out(create_query);
				copyData(in, out);
			}

			ParserCreateQuery parser;
			const char * pos = create_query.data();
			const char * end = pos + create_query.size();
			ASTPtr ast;
			Expected expected = "";
			bool parse_res = parser.parse(pos, end, ast, expected);

			/// Распарсенный запрос должен заканчиваться на конец входных данных или на точку с запятой.
			if (!parse_res || (pos != end && *pos != ';'))
				throw Exception(getSyntaxErrorMessage(parse_res, create_query.data(), end, pos, expected, "in file " + from_metadata_path),
					ErrorCodes::SYNTAX_ERROR);

			typeid_cast<ASTCreateQuery &>(*ast).table = to_table_name;

			Poco::FileOutputStream ostr(to_metadata_path);
			formatAST(*ast, ostr, 0, false);
			ostr << '\n';
		}

		/// Переименовываем таблицу в контексте.
		context.addTable(to_database_name, to_table_name,
			context.detachTable(from_database_name, from_table_name));

		/// Удаляем старый файл с метаданными.
		Poco::File(from_metadata_path).remove();
	}
}


}
