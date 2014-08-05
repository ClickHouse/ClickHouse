#include <DB/Parsers/ParserCheckQuery.h>
#include <DB/Parsers/CommonParsers.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ExpressionElementParsers.h>
#include <DB/Parsers/ASTCheckQuery.h>
#include </home/kartavyy/metrica/metrica/src/programs/███████████/src/data.h>

using namespace DB;

bool ParserCheckQuery::parseImpl(IParser::Pos& pos, IParser::Pos end, ASTPtr& node, Expected& expected)
{
	ParserWhiteSpaceOrComments ws;
	ParserString s_check("CHECK", true, true);
	ParserString s_table("TABLE", true, true);
	ParserString s_dot(".");

	ParserIdentifier table_parser;

	ASTPtr table;
	ASTPtr database;

	Poco::SharedPtr<ASTCheckQuery> query = new ASTCheckQuery(StringRange(pos, end));

	ws.ignore(pos, end);

	if (!s_check.ignore(pos, end, expected))
		return false;

	ws.ignore(pos, end);
	s_table.ignore(pos, end, expected);

	ws.ignore(pos, end);
	if (!table_parser.parse(pos, end, database, expected))
		return false;

	if (s_dot.ignore(pos, end))
	{
		if (!table_parser.parse(pos, end, table, expected))
			return false;

		query->database = typeid_cast<ASTIdentifier &>(*database).name;
		query->table = typeid_cast<ASTIdentifier &>(*table).name;
	}
	else
	{
		table = database;
		query->table = typeid_cast<ASTIdentifier &>(*table).name;
	}

	node = query;
	return true;
}
