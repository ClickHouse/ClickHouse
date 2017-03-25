#include <DB/Parsers/ParserUseQuery.h>

#include <DB/Common/typeid_cast.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ExpressionElementParsers.h>
#include <DB/Parsers/CommonParsers.h>
#include <DB/Parsers/ASTUseQuery.h>


namespace DB
{
bool ParserUseQuery::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
	Pos begin = pos;

	ParserWhiteSpaceOrComments ws;
	ParserString s_use("USE", true, true);
	ParserIdentifier name_p;

	ASTPtr database;

	ws.ignore(pos, end);

	if (!s_use.ignore(pos, end, max_parsed_pos, expected))
		return false;

	ws.ignore(pos, end);

	if (!name_p.parse(pos, end, database, max_parsed_pos, expected))
		return false;

	ws.ignore(pos, end);

	auto query = std::make_shared<ASTUseQuery>(StringRange(begin, pos));
	query->database = typeid_cast<ASTIdentifier &>(*database).name;
	node = query;

	return true;
}
}
