#include <DB/Parsers/ASTJoin.h>
#include <DB/Parsers/CommonParsers.h>
#include <DB/Parsers/ExpressionElementParsers.h>
#include <DB/Parsers/ExpressionListParsers.h>
#include <DB/Parsers/ParserJoin.h>


namespace DB
{


bool ParserJoin::parseImpl(Pos & pos, Pos end, ASTPtr & node, Expected & expected)
{
	Pos begin = pos;

	ASTJoin * join = new ASTJoin(StringRange(begin, pos));
	node = join;

	ParserWhiteSpaceOrComments ws;
	ParserString s_global("GLOBAL", true, true);
	ParserString s_any("ANY", true, true);
	ParserString s_all("ALL", true, true);
	ParserString s_inner("INNER", true, true);
	ParserString s_left("LEFT", true, true);
	ParserString s_join("JOIN", true, true);
	ParserString s_using("USING", true, true);

	ParserNotEmptyExpressionList exp_list;
	ParserSubquery subquery;
	ParserIdentifier identifier;

	ws.ignore(pos, end);

	if (s_global.ignore(pos, end))
		join->locality = ASTJoin::Global;
	else
		join->locality = ASTJoin::Local;

	ws.ignore(pos, end);

	if (s_any.ignore(pos, end))
		join->strictness = ASTJoin::Any;
	else if (s_all.ignore(pos, end))
		join->strictness = ASTJoin::All;
	else
	{
		expected = "ANY|ALL";
		return false;
	}

	ws.ignore(pos, end);

	if (s_inner.ignore(pos, end))
		join->kind = ASTJoin::Inner;
	else if (s_left.ignore(pos, end))
		join->kind = ASTJoin::Left;
	else
	{
		expected = "INNER|LEFT";
		return false;
	}

	ws.ignore(pos, end);

	if (!s_join.ignore(pos, end, expected))
		return false;

	ws.ignore(pos, end);

	if (!identifier.parse(pos, end, join->table, expected)
		&& !subquery.parse(pos, end, join->table, expected))
		return false;

	ws.ignore(pos, end);

	/// Может быть указан алиас. На данный момент, он ничего не значит и не используется.
	ParserAlias().ignore(pos, end);
	ws.ignore(pos, end);

	if (!s_using.ignore(pos, end, expected))
		return false;

	ws.ignore(pos, end);

	if (!exp_list.parse(pos, end, join->using_expr_list, expected))
		return false;

	ws.ignore(pos, end);

	join->children.push_back(join->table);
	join->children.push_back(join->using_expr_list);

	return true;
}

}
