#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/IParserBase.h>
#include <DB/Parsers/CommonParsers.h>
#include <DB/Parsers/ExpressionElementParsers.h>
#include <DB/Parsers/ExpressionListParsers.h>
#include <DB/Parsers/ParserSelectQuery.h>


namespace DB
{


bool ParserSelectQuery::parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
{
	Pos begin = pos;

	ASTSelectQuery * select_query = new ASTSelectQuery(StringRange(begin, pos));
	node = select_query;

	ParserWhiteSpaceOrComments ws;
	ParserString s_select("SELECT", true, true);
	ParserString s_from("FROM", true, true);
	ParserString s_where("WHERE", true, true);
	ParserString s_group("GROUP", true, true);
	ParserString s_by("BY", true, true);
	ParserString s_having("HAVING", true, true);
	ParserString s_order("ORDER", true, true);
	ParserString s_limit("LIMIT", true, true);
	ParserNotEmptyExpressionList exp_list;
	ParserLogicalOrExpression exp_elem;
	ParserOrderByExpressionList order_list;

	ws.ignore(pos, end);

	/// SELECT expr list
	{
		if (!s_select.ignore(pos, end, expected))
			return false;

		ws.ignore(pos, end);

		if (!exp_list.parse(pos, end, select_query->select_expression_list, expected))
			return false;

		ws.ignore(pos, end);
	}

	/// FROM database.table или FROM table TODO subquery
	if (s_from.ignore(pos, end, expected))
	{
		ws.ignore(pos, end);

		ParserString s_dot(".");
		ParserIdentifier ident;

		if (!ident.parse(pos, end, select_query->table, expected))
			return false;

		ws.ignore(pos, end);
		
		if (s_dot.ignore(pos, end, expected))
		{
			select_query->database = select_query->table;
			if (!ident.parse(pos, end, select_query->table, expected))
				return false;

			ws.ignore(pos, end);
		}

		if (select_query->database)
			dynamic_cast<ASTIdentifier &>(*select_query->database).kind = ASTIdentifier::Database;
		dynamic_cast<ASTIdentifier &>(*select_query->table).kind = ASTIdentifier::Table;
	}

	/// WHERE expr
	if (s_where.ignore(pos, end, expected))
	{
		ws.ignore(pos, end);

		if (!exp_elem.parse(pos, end, select_query->where_expression, expected))
			return false;

		ws.ignore(pos, end);
	}

	/// GROUP BY expr list
	if (s_group.ignore(pos, end, expected))
	{
		ws.ignore(pos, end);
		if (!s_by.ignore(pos, end, expected))
			return false;

		if (!exp_list.parse(pos, end, select_query->group_expression_list, expected))
			return false;

		ws.ignore(pos, end);
	}

	/// HAVING expr
	if (s_having.ignore(pos, end, expected))
	{
		ws.ignore(pos, end);

		if (!exp_elem.parse(pos, end, select_query->having_expression, expected))
			return false;

		ws.ignore(pos, end);
	}

	/// ORDER BY expr ASC|DESC list
	if (s_order.ignore(pos, end, expected))
	{
		ws.ignore(pos, end);
		if (!s_by.ignore(pos, end, expected))
			return false;

		if (!order_list.parse(pos, end, select_query->order_expression_list, expected))
			return false;

		ws.ignore(pos, end);
	}

	/// LIMIT length или LIMIT offset, length
	if (s_limit.ignore(pos, end, expected))
	{
		ws.ignore(pos, end);

		ParserString s_comma(",");
		ParserNumber num;

		if (!num.parse(pos, end, select_query->limit_length, expected))
			return false;

		ws.ignore(pos, end);

		if (s_comma.ignore(pos, end, expected))
		{
			select_query->limit_offset = select_query->limit_length;
			if (!num.parse(pos, end, select_query->limit_length, expected))
				return false;

			ws.ignore(pos, end);
		}
	}

	select_query->children.push_back(select_query->select_expression_list);
	if (select_query->database)
		select_query->children.push_back(select_query->database);
	if (select_query->table)
		select_query->children.push_back(select_query->table);
	if (select_query->where_expression)
		select_query->children.push_back(select_query->where_expression);
	if (select_query->group_expression_list)
		select_query->children.push_back(select_query->group_expression_list);
	if (select_query->having_expression)
		select_query->children.push_back(select_query->having_expression);
	if (select_query->order_expression_list)
		select_query->children.push_back(select_query->order_expression_list);
	if (select_query->limit_offset)
		select_query->children.push_back(select_query->limit_offset);
	if (select_query->limit_length)
		select_query->children.push_back(select_query->limit_length);

	return true;
}

}
