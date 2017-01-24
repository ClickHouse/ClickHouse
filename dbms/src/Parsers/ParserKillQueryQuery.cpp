#include <DB/Parsers/ParserKillQueryQuery.h>
#include <DB/Parsers/ASTKillQueryQuery.h>

#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Parsers/CommonParsers.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTTablesInSelectQuery.h>
#include <DB/Parsers/ParserSelectQuery.h>
#include <DB/Parsers/ExpressionElementParsers.h>
#include <DB/Parsers/ExpressionListParsers.h>

#include <DB/Common/typeid_cast.h>

namespace DB
{


bool ParserKillQueryQuery::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
	Pos begin = pos;
	auto query = std::make_shared<ASTKillQueryQuery>();

	ParserWhiteSpaceOrComments ws;

	ws.ignore(pos, end);

	if (!ParserString{"KILL", true, true}.ignore(pos, end, max_parsed_pos, expected))
		return false;

	ws.ignore(pos, end);

	if (!ParserString{"QUERY", true, true}.ignore(pos, end, max_parsed_pos, expected))
		return false;

	ws.ignore(pos, end);

	if (!ParserString{"WHERE", true, true}.ignore(pos, end, max_parsed_pos, expected))
		return false;

	ws.ignore(pos, end);

	ParserExpressionWithOptionalAlias p_where_expression(false);
	if (!p_where_expression.parse(pos, end, query->where_expression, max_parsed_pos, expected))
		return false;

	ws.ignore(pos, end);

	if (ParserString{"SYNC", true, true}.ignore(pos, end))
		query->sync = true;
	else if (ParserString{"ASYNC", true, true}.ignore(pos, end))
		query->sync = false;
	else if (ParserString{"TEST", true, true}.ignore(pos, end))
		query->test = true;
	else
		expected = "[SYNC|ASYNC|TEST]";

	ws.ignore(pos, end);

	query->range = StringRange(begin, pos);

	node = std::move(query);

	return true;
}

}
