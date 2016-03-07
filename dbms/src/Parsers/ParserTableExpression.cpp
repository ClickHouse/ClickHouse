#include <DB/Parsers/CommonParsers.h>
#include <DB/Parsers/ExpressionElementParsers.h>
#include <DB/Parsers/ExpressionListParsers.h>
#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ParserSelectQuery.h>
#include <DB/Parsers/ParserTableExpression.h>


namespace DB
{


bool ParserTableExpression::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
	ParserWhiteSpaceOrComments ws;
	ParserString s_lparen("(");
	ParserString s_rparen(")");
	ParserCompoundIdentifier ident;
	ParserFunction table_function;
	Pos before = pos;

	if (s_lparen.ignore(pos, end, max_parsed_pos, expected))
	{
		/// Подзапрос.
		ws.ignore(pos, end);

		ParserSelectQuery select_p;
		if (!select_p.parse(pos, end, node, max_parsed_pos, expected))
			return false;

		ws.ignore(pos, end);

		if (!s_rparen.ignore(pos, end, max_parsed_pos, expected))
			return false;

		ws.ignore(pos, end);
	}
	else if (ident.parse(pos, end, node, max_parsed_pos, expected))
	{
		/// Если сразу после identifier идет скобка, значит это должна быть табличная функция
		if (s_lparen.ignore(pos, end, max_parsed_pos, expected))
		{
			pos = before;
			if (!table_function.parse(pos, end, node, max_parsed_pos, expected))
				return false;
			if (node)
				typeid_cast<ASTFunction &>(*node).kind = ASTFunction::TABLE_FUNCTION;
			ws.ignore(pos, end);
		}
		else
		{
			ws.ignore(pos, end);
			typeid_cast<ASTIdentifier &>(*node).kind = ASTIdentifier::Table;
		}
	}
	else
		return false;

	return true;
}

}
