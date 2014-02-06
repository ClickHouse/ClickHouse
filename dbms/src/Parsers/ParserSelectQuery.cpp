#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/IParserBase.h>
#include <DB/Parsers/CommonParsers.h>
#include <DB/Parsers/ExpressionElementParsers.h>
#include <DB/Parsers/ExpressionListParsers.h>
#include <DB/Parsers/ParserSelectQuery.h>
//#include <DB/Parsers/ParserCreateQuery.h>


namespace DB
{


bool ParserSelectQuery::parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
{
	Pos begin = pos;

	ASTSelectQuery * select_query = new ASTSelectQuery(StringRange(begin, pos));
	node = select_query;

	ParserWhiteSpaceOrComments ws;
	ParserString s_select("SELECT", true, true);
	ParserString s_distinct("DISTINCT", true, true);
	ParserString s_from("FROM", true, true);
	ParserString s_array("ARRAY", true, true);
	ParserString s_join("JOIN", true, true);
	ParserString s_prewhere("PREWHERE", true, true);
	ParserString s_where("WHERE", true, true);
	ParserString s_final("FINAL", true, true);
	ParserString s_sample("SAMPLE", true, true);
	ParserString s_group("GROUP", true, true);
	ParserString s_by("BY", true, true);
	ParserString s_with("WITH", true, true);
	ParserString s_totals("TOTALS", true, true);
	ParserString s_having("HAVING", true, true);
	ParserString s_order("ORDER", true, true);
	ParserString s_limit("LIMIT", true, true);
	ParserString s_format("FORMAT", true, true);
	ParserNotEmptyExpressionList exp_list;
	ParserExpressionWithOptionalAlias exp_elem;
	ParserOrderByExpressionList order_list;

	ws.ignore(pos, end);

	/// SELECT [DISTINCT] expr list
	{
		if (!s_select.ignore(pos, end, expected))
			return false;

		ws.ignore(pos, end);

		if (s_distinct.ignore(pos, end, expected))
		{
			select_query->distinct = true;
			ws.ignore(pos, end);
		}

		if (!exp_list.parse(pos, end, select_query->select_expression_list, expected))
			return false;

		ws.ignore(pos, end);
	}

	/// FROM database.table или FROM table или FROM (subquery) или FROM tableFunction
	if (s_from.ignore(pos, end, expected))
	{
		ws.ignore(pos, end);

		ParserString s_lparen("(");
		ParserString s_rparen(")");
		ParserString s_dot(".");
		ParserIdentifier ident;
		ParserFunction table_function;
		Pos before = pos;

		if (s_lparen.ignore(pos, end, expected))
		{
			ws.ignore(pos, end);

			ParserSelectQuery select_p;
			if (!select_p.parse(pos, end, select_query->table, expected))
				return false;

			ws.ignore(pos, end);

			if (!s_rparen.ignore(pos, end, expected))
				return false;
			
			ws.ignore(pos, end);
		}
		else if (ident.parse(pos, end, select_query->table, expected))
		{
			/// Если сразу после identifier идет скобка, значит это должна быть табличная функция
			if (s_lparen.ignore(pos, end, expected))
			{
				pos = before;
				if (!table_function.parse(pos, end, select_query->table, expected))
					return false;
				ws.ignore(pos, end);
			}
			else
			{
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
		}
		else
			return false;
	}
	
	/// ARRAY JOIN expr list
	if (s_array.ignore(pos, end, expected))
	{
		ws.ignore(pos, end);

		if (!s_join.ignore(pos, end, expected))
			return false;

		ws.ignore(pos, end);

		if (!exp_list.parse(pos, end, select_query->array_join_expression_list, expected))
			return false;

		ws.ignore(pos, end);
	}

	/// FINAL
	if (s_final.ignore(pos, end, expected))
	{
		select_query->final = true;
		
		ws.ignore(pos, end);
	}

	/// SAMPLE number
	if (s_sample.ignore(pos, end, expected))
	{
		ws.ignore(pos, end);
		
		ParserNumber num;
		
		if (!num.parse(pos, end, select_query->sample_size, expected))
			return false;
		
		ws.ignore(pos, end);
	}
	
	/// PREWHERE expr
	if (s_prewhere.ignore(pos, end, expected))
	{
		ws.ignore(pos, end);

		if (!exp_elem.parse(pos, end, select_query->prewhere_expression, expected))
			return false;

		ws.ignore(pos, end);
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

		/// WITH TOTALS
		if (s_with.ignore(pos, end, expected))
		{
			ws.ignore(pos, end);
			if (!s_totals.ignore(pos, end, expected))
				return false;

			select_query->group_by_with_totals = true;

			ws.ignore(pos, end);
		}
	}

	/// HAVING expr
	if (s_having.ignore(pos, end, expected))
	{
		ws.ignore(pos, end);

		if (!exp_elem.parse(pos, end, select_query->having_expression, expected))
			return false;

		ws.ignore(pos, end);
	}

	/// ORDER BY expr ASC|DESC COLLATE 'locale' list
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

	/// FORMAT format_name
	if (s_format.ignore(pos, end, expected))
	{
		ws.ignore(pos, end);

		ParserIdentifier format_p;

		if (!format_p.parse(pos, end, select_query->format, expected))
			return false;
		dynamic_cast<ASTIdentifier &>(*select_query->format).kind = ASTIdentifier::Format;

		ws.ignore(pos, end);
	}

	select_query->children.push_back(select_query->select_expression_list);
	if (select_query->database)
		select_query->children.push_back(select_query->database);
	if (select_query->table)
		select_query->children.push_back(select_query->table);
	if (select_query->array_join_expression_list)
		select_query->children.push_back(select_query->array_join_expression_list);
	if (select_query->sample_size)
		select_query->children.push_back(select_query->sample_size);
	if (select_query->where_expression)
		select_query->children.push_back(select_query->where_expression);
	if (select_query->prewhere_expression)
		select_query->children.push_back(select_query->prewhere_expression);
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
	if (select_query->format)
		select_query->children.push_back(select_query->format);

	return true;
}

}
