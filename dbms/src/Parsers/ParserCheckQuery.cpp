#include <DB/Parsers/ParserCheckQuery.h>
#include <DB/Parsers/CommonParsers.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ExpressionElementParsers.h>
#include <DB/Parsers/ASTCheckQuery.h>

#include <DB/Common/typeid_cast.h>


namespace DB
{

bool ParserCheckQuery::parseImpl(IParser::Pos & pos, IParser::Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
	ParserString s_check("CHECK", true, true);
	ParserString s_table("TABLE", true, true);
	ParserString s_dot(".");

	ParserIdentifier table_parser;

	ASTPtr table;
	ASTPtr database;

	auto query = std::make_shared<ASTCheckQuery>(StringRange(pos, end));

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

		query->database = typeid_cast<const ASTIdentifier &>(*database).name;
		query->table = typeid_cast<const ASTIdentifier &>(*table).name;
	}
	else
	{
		table = database;
		query->table = typeid_cast<const ASTIdentifier &>(*table).name;
	}

	ws.ignore(pos, end);

	/// FORMAT format_name
	if (!parseFormat(*query, pos, end, node, max_parsed_pos, expected))
		return false;

	node = query;
	return true;
}

}
