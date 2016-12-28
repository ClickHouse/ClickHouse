#include <memory>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/IParserBase.h>
#include <DB/Parsers/CommonParsers.h>
#include <DB/Parsers/ExpressionElementParsers.h>
#include <DB/Parsers/ExpressionListParsers.h>
#include <DB/Parsers/ParserSetQuery.h>
#include <DB/Parsers/ParserSampleRatio.h>
#include <DB/Parsers/ParserSelectQuery.h>
#include <DB/Parsers/ParserTablesInSelectQuery.h>

#include <iostream>

namespace DB
{

namespace ErrorCodes
{
	extern const int SYNTAX_ERROR;
}


bool ParserSelectQuery::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
	Pos begin = pos;

	auto select_query = std::make_shared<ASTSelectQuery>();
	node = select_query;

	ParserString s_select("SELECT", true, true);
	ParserString s_distinct("DISTINCT", true, true);
	ParserString s_from("FROM", true, true);
	ParserString s_prewhere("PREWHERE", true, true);
	ParserString s_where("WHERE", true, true);
	ParserString s_group("GROUP", true, true);
	ParserString s_by("BY", true, true);
	ParserString s_with("WITH", true, true);
	ParserString s_totals("TOTALS", true, true);
	ParserString s_having("HAVING", true, true);
	ParserString s_order("ORDER", true, true);
	ParserString s_limit("LIMIT", true, true);
	ParserString s_settings("SETTINGS", true, true);
	ParserString s_union("UNION", true, true);
	ParserString s_all("ALL", true, true);

	ParserNotEmptyExpressionList exp_list(false);
	ParserNotEmptyExpressionList exp_list_for_select_clause(true);	/// Allows aliases without AS keyword.
	ParserExpressionWithOptionalAlias exp_elem(false);
	ParserOrderByExpressionList order_list;

	ws.ignore(pos, end);

	/// SELECT [DISTINCT] expr list
	{
		if (!s_select.ignore(pos, end, max_parsed_pos, expected))
			return false;

		ws.ignore(pos, end);

		if (s_distinct.ignore(pos, end, max_parsed_pos, expected))
		{
			select_query->distinct = true;
			ws.ignore(pos, end);
		}

		if (!exp_list_for_select_clause.parse(pos, end, select_query->select_expression_list, max_parsed_pos, expected))
			return false;

		ws.ignore(pos, end);
	}

	/// FROM database.table или FROM table или FROM (subquery) или FROM tableFunction
	if (s_from.ignore(pos, end, max_parsed_pos, expected))
	{
		ws.ignore(pos, end);

		if (!ParserTablesInSelectQuery().parse(pos, end, select_query->tables, max_parsed_pos, expected))
			return false;

		ws.ignore(pos, end);
	}

	/// PREWHERE expr
	if (s_prewhere.ignore(pos, end, max_parsed_pos, expected))
	{
		ws.ignore(pos, end);

		if (!exp_elem.parse(pos, end, select_query->prewhere_expression, max_parsed_pos, expected))
			return false;

		ws.ignore(pos, end);
	}

	/// WHERE expr
	if (s_where.ignore(pos, end, max_parsed_pos, expected))
	{
		ws.ignore(pos, end);

		if (!exp_elem.parse(pos, end, select_query->where_expression, max_parsed_pos, expected))
			return false;

		ws.ignore(pos, end);
	}

	/// GROUP BY expr list
	if (s_group.ignore(pos, end, max_parsed_pos, expected))
	{
		ws.ignore(pos, end);
		if (!s_by.ignore(pos, end, max_parsed_pos, expected))
			return false;

		if (!exp_list.parse(pos, end, select_query->group_expression_list, max_parsed_pos, expected))
			return false;

		ws.ignore(pos, end);
	}

	/// WITH TOTALS
	if (s_with.ignore(pos, end, max_parsed_pos, expected))
	{
		ws.ignore(pos, end);
		if (!s_totals.ignore(pos, end, max_parsed_pos, expected))
			return false;

		select_query->group_by_with_totals = true;

		ws.ignore(pos, end);
	}

	/// HAVING expr
	if (s_having.ignore(pos, end, max_parsed_pos, expected))
	{
		ws.ignore(pos, end);

		if (!exp_elem.parse(pos, end, select_query->having_expression, max_parsed_pos, expected))
			return false;

		ws.ignore(pos, end);
	}

	/// ORDER BY expr ASC|DESC COLLATE 'locale' list
	if (s_order.ignore(pos, end, max_parsed_pos, expected))
	{
		ws.ignore(pos, end);
		if (!s_by.ignore(pos, end, max_parsed_pos, expected))
			return false;

		if (!order_list.parse(pos, end, select_query->order_expression_list, max_parsed_pos, expected))
			return false;

		ws.ignore(pos, end);
	}

	/// LIMIT length | LIMIT offset, length | LIMIT count BY expr-list
	if (s_limit.ignore(pos, end, max_parsed_pos, expected))
	{
		ws.ignore(pos, end);

		ParserString s_comma(",");
		ParserNumber num;

		if (!num.parse(pos, end, select_query->limit_length, max_parsed_pos, expected))
			return false;

		ws.ignore(pos, end);

		if (s_comma.ignore(pos, end, max_parsed_pos, expected))
		{
			select_query->limit_offset = select_query->limit_length;
			if (!num.parse(pos, end, select_query->limit_length, max_parsed_pos, expected))
				return false;

			ws.ignore(pos, end);
		}
		else if (s_by.ignore(pos, end, max_parsed_pos, expected))
		{
			select_query->limit_by_value = select_query->limit_length;
			select_query->limit_length = nullptr;

			ws.ignore(pos, end);

			if (!exp_list.parse(pos, end, select_query->limit_by_expression_list, max_parsed_pos, expected))
				return false;

			ws.ignore(pos, end);
		}
	}

	/// LIMIT length | LIMIT offset, length
	if (s_limit.ignore(pos, end, max_parsed_pos, expected))
	{
		if (!select_query->limit_by_value || select_query->limit_length)
			return false;

		ws.ignore(pos, end);

		ParserString s_comma(",");
		ParserNumber num;

		if (!num.parse(pos, end, select_query->limit_length, max_parsed_pos, expected))
			return false;

		ws.ignore(pos, end);

		if (s_comma.ignore(pos, end, max_parsed_pos, expected))
		{
			select_query->limit_offset = select_query->limit_length;
			if (!num.parse(pos, end, select_query->limit_length, max_parsed_pos, expected))
				return false;

			ws.ignore(pos, end);
		}
	}

	/// SETTINGS key1 = value1, key2 = value2, ...
	if (s_settings.ignore(pos, end, max_parsed_pos, expected))
	{
		ws.ignore(pos, end);

		ParserSetQuery parser_settings(true);

		if (!parser_settings.parse(pos, end, select_query->settings, max_parsed_pos, expected))
			return false;

		ws.ignore(pos, end);
	}

	/// FORMAT format_name
	if (!parseFormat(*select_query, pos, end, node, max_parsed_pos, expected))
		return false;

	// UNION ALL select query
	if (s_union.ignore(pos, end, max_parsed_pos, expected))
	{
		ws.ignore(pos, end);

		if (s_all.ignore(pos, end, max_parsed_pos, expected))
		{
			if (select_query->format)
			{
				/// FORMAT может быть задан только в последнем запросе цепочки UNION ALL.
				expected = "FORMAT only in the last SELECT of the UNION ALL chain";
				return false;
			}

			ParserSelectQuery select_p;
			if (!select_p.parse(pos, end, select_query->next_union_all, max_parsed_pos, expected))
				return false;
			auto next_select_query = static_cast<ASTSelectQuery *>(&*select_query->next_union_all);
			next_select_query->prev_union_all = node.get();
		}
		else
			return false;

		ws.ignore(pos, end);
	}

	select_query->range = StringRange(begin, pos);

	select_query->children.push_back(select_query->select_expression_list);
	if (select_query->tables)
		select_query->children.push_back(select_query->tables);
	if (select_query->prewhere_expression)
		select_query->children.push_back(select_query->prewhere_expression);
	if (select_query->where_expression)
		select_query->children.push_back(select_query->where_expression);
	if (select_query->group_expression_list)
		select_query->children.push_back(select_query->group_expression_list);
	if (select_query->having_expression)
		select_query->children.push_back(select_query->having_expression);
	if (select_query->order_expression_list)
		select_query->children.push_back(select_query->order_expression_list);
	if (select_query->limit_by_value)
		select_query->children.push_back(select_query->limit_by_value);
	if (select_query->limit_by_expression_list)
		select_query->children.push_back(select_query->limit_by_expression_list);
	if (select_query->limit_offset)
		select_query->children.push_back(select_query->limit_offset);
	if (select_query->limit_length)
		select_query->children.push_back(select_query->limit_length);
	if (select_query->settings)
		select_query->children.push_back(select_query->settings);
	if (select_query->format)
		select_query->children.push_back(select_query->format);
	if (select_query->next_union_all)
		select_query->children.push_back(select_query->next_union_all);

	return true;
}

}
