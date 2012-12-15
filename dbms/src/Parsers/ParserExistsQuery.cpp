#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTExistsQuery.h>

#include <DB/Parsers/CommonParsers.h>
#include <DB/Parsers/ParserExistsQuery.h>


namespace DB
{


bool ParserExistsQuery::parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
{
	Pos begin = pos;

	ParserWhiteSpaceOrComments ws;
	ParserString s_exists("EXISTS", true, true);
	ParserString s_table("TABLE", true, true);
	ParserString s_dot(".");
	ParserIdentifier name_p;

	ASTPtr database;
	ASTPtr table;

	ws.ignore(pos, end);

	if (!s_exists.ignore(pos, end, expected))
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

	ASTExistsQuery * query = new ASTExistsQuery(StringRange(begin, pos));
	node = query;

	if (database)
		query->database = dynamic_cast<ASTIdentifier &>(*database).name;
	if (table)
		query->table = dynamic_cast<ASTIdentifier &>(*table).name;

	return true;
}


}
