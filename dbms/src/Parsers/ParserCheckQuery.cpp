#include <DB/Parsers/ParserCheckQuery.h>
#include <DB/Parsers/CommonParsers.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ExpressionElementParsers.h>
#include <DB/Parsers/ASTCheckQuery.h>

using namespace DB;

bool ParserCheckQuery::parseImpl(IParser::Pos & pos, IParser::Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
	ParserWhiteSpaceOrComments ws;
	ParserString s_check("CHECK", true, true);
	ParserString s_table("TABLE", true, true);
	ParserString s_format("FORMAT", true, true);
	ParserString s_dot(".");

	ParserIdentifier table_parser;

	ASTPtr table;
	ASTPtr database;

	Poco::SharedPtr<ASTCheckQuery> query = new ASTCheckQuery(StringRange(pos, end));

	ws.ignore(pos, end);

	if (!s_check.ignore(pos, end, max_parsed_pos, expected))
		return false;

	ws.ignore(pos, end);
	s_table.ignore(pos, end, max_parsed_pos, expected);

	ws.ignore(pos, end);
	if (!table_parser.parse(pos, end, database, max_parsed_pos, expected))
		return false;

	if (s_dot.ignore(pos, end))
	{
		if (!table_parser.parse(pos, end, table, max_parsed_pos, expected))
			return false;

		query->database = typeid_cast<ASTIdentifier &>(*database).name;
		query->table = typeid_cast<ASTIdentifier &>(*table).name;
	}
	else
	{
		table = database;
		query->table = typeid_cast<ASTIdentifier &>(*table).name;
	}

	ws.ignore(pos, end);

	/// FORMAT format_name
	if (s_format.ignore(pos, end, max_parsed_pos, expected))
	{
		ws.ignore(pos, end);

		ParserIdentifier format_p;

		if (!format_p.parse(pos, end, query->format, max_parsed_pos, expected))
			return false;
		typeid_cast<ASTIdentifier &>(*query->format).kind = ASTIdentifier::Format;

		ws.ignore(pos, end);
	}

	node = query;
	return true;
}
