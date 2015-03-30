#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTInsertQuery.h>

#include <DB/Parsers/CommonParsers.h>
#include <DB/Parsers/ExpressionElementParsers.h>
#include <DB/Parsers/ExpressionListParsers.h>
#include <DB/Parsers/ParserSelectQuery.h>
#include <DB/Parsers/ParserInsertQuery.h>


namespace DB
{


bool ParserInsertQuery::parseImpl(Pos & pos, Pos end, ASTPtr & node, Expected & expected)
{
	Pos begin = pos;

	ParserWhiteSpaceOrComments ws;
	ParserString s_insert("INSERT", true, true);
	ParserString s_into("INTO", true, true);
	ParserString s_dot(".");
	ParserString s_id("ID");
	ParserString s_eq("=");
	ParserStringLiteral id_p;
	ParserString s_values("VALUES", true, true);
	ParserString s_format("FORMAT", true, true);
	ParserString s_select("SELECT", true, true);
	ParserString s_lparen("(");
	ParserString s_rparen(")");
	ParserIdentifier name_p;
	ParserList columns_p(ParserPtr(new ParserIdentifier), ParserPtr(new ParserString(",")), false);

	ASTPtr database;
	ASTPtr table;
	ASTPtr columns;
	ASTPtr format;
	ASTPtr select;
	ASTPtr id;
	/// Данные для вставки
	const char * data = nullptr;

	ws.ignore(pos, end);

	/// INSERT INTO
	if (!s_insert.ignore(pos, end, expected)
		|| !ws.ignore(pos, end)
		|| !s_into.ignore(pos, end, expected))
		return false;

	ws.ignore(pos, end);

	if (!name_p.parse(pos, end, table, expected))
		return false;

	ws.ignore(pos, end);

	if (s_dot.ignore(pos, end, expected))
	{
		database = table;
		if (!name_p.parse(pos, end, table, expected))
			return false;

		ws.ignore(pos, end);
	}

	ws.ignore(pos, end);

	if (s_id.ignore(pos, end, expected))
	{
		if (!s_eq.ignore(pos, end, expected))
			return false;

		if (!id_p.parse(pos, end, id, expected))
			return false;
	}

	ws.ignore(pos, end);

	/// Есть ли список столбцов
	if (s_lparen.ignore(pos, end, expected))
	{
		if (!columns_p.parse(pos, end, columns, expected))
			return false;
		ws.ignore(pos, end);
		if (!s_rparen.ignore(pos, end, expected))
			return false;
	}

	ws.ignore(pos, end);

	Pos before_select = pos;

	/// VALUES или FORMAT или SELECT
	if (s_values.ignore(pos, end, expected))
	{
		ws.ignore(pos, end);
		data = pos;
		pos = end;
	}
	else if (s_format.ignore(pos, end, expected))
	{
		ws.ignore(pos, end);

		if (!name_p.parse(pos, end, format, expected))
			return false;

		/// Данные начинаются после первого перевода строки, если такой есть, или после всех пробельных символов, иначе.
		ParserWhiteSpaceOrComments ws_without_nl(false);

		ws_without_nl.ignore(pos, end);
		if (pos != end && *pos == ';')
			throw Exception("You have excessive ';' symbol before data for INSERT.\n"
				"Example:\n\n"
				"INSERT INTO t (x, y) FORMAT TabSeparated\n"
				"1\tHello\n"
				"2\tWorld\n"
				"\n"
				"Note that there is no ';' in first line.", ErrorCodes::SYNTAX_ERROR);

		if (pos != end && *pos == '\n')
			++pos;

		data = pos;
		pos = end;
	}
	else if (s_select.ignore(pos, end, expected))
	{
		pos = before_select;
		ParserSelectQuery select_p;
		select_p.parse(pos, end, select, expected);
	}
	else
	{
		expected = "VALUES or FORMAT or SELECT";
		return false;
	}

	ASTInsertQuery * query = new ASTInsertQuery(StringRange(begin, data ? data : pos));
	node = query;

	if (database)
		query->database = typeid_cast<ASTIdentifier &>(*database).name;

	query->table = typeid_cast<ASTIdentifier &>(*table).name;

	if (id)
		query->insert_id = safeGet<const String &>(typeid_cast<ASTLiteral &>(*id).value);

	if (format)
		query->format = typeid_cast<ASTIdentifier &>(*format).name;

	query->columns = columns;
	query->select = select;
	query->data = data != end ? data : NULL;
	query->end = end;

	if (columns)
		query->children.push_back(columns);
	if (select)
		query->children.push_back(select);

	return true;
}


}
