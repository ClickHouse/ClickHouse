#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTOptimizeQuery.h>

#include <DB/Parsers/CommonParsers.h>
#include <DB/Parsers/ParserOptimizeQuery.h>


namespace DB
{


bool ParserOptimizeQuery::parseImpl(Pos & pos, Pos end, ASTPtr & node, Expected & expected)
{
	Pos begin = pos;

	ParserWhiteSpaceOrComments ws;
	ParserString s_optimize("OPTIMIZE", true, true);
	ParserString s_table("TABLE", true, true);
	ParserString s_dot(".");
	ParserIdentifier name_p;

	ASTPtr database;
	ASTPtr table;

	ws.ignore(pos, end);

	if (!s_optimize.ignore(pos, end, expected))
		return false;

	ws.ignore(pos, end);

	if (!s_table.ignore(pos, end, expected))
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

	ASTOptimizeQuery * query = new ASTOptimizeQuery(StringRange(begin, pos));
	node = query;

	if (database)
		query->database = dynamic_cast<ASTIdentifier &>(*database).name;
	if (table)
		query->table = dynamic_cast<ASTIdentifier &>(*table).name;

	return true;
}


}
