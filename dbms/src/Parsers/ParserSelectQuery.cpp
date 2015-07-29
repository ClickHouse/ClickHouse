#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/IParserBase.h>
#include <DB/Parsers/CommonParsers.h>
#include <DB/Parsers/ExpressionElementParsers.h>
#include <DB/Parsers/ExpressionListParsers.h>
#include <DB/Parsers/ParserJoin.h>
#include <DB/Parsers/ParserSetQuery.h>
#include <DB/Parsers/ParserSelectQuery.h>

namespace DB
{


bool ParserSelectQuery::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
	Pos begin = pos;

	ASTSelectQuery * select_query = new ASTSelectQuery;
	node = select_query;

	ParserWhiteSpaceOrComments ws;
	ParserString s_select("SELECT", true, true);
	ParserString s_distinct("DISTINCT", true, true);
	ParserString s_from("FROM", true, true);
	ParserString s_left("LEFT", true, true);
	ParserString s_array("ARRAY", true, true);
	ParserString s_join("JOIN", true, true);
	ParserString s_using("USING", true, true);
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
	ParserString s_settings("SETTINGS", true, true);
	ParserString s_format("FORMAT", true, true);
	ParserString s_union("UNION", true, true);
	ParserString s_all("ALL", true, true);

	ParserNotEmptyExpressionList exp_list;
	ParserExpressionWithOptionalAlias exp_elem;
	ParserJoin join;
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

		if (!exp_list.parse(pos, end, select_query->select_expression_list, max_parsed_pos, expected))
			return false;

		ws.ignore(pos, end);
	}

	/// FROM database.table или FROM table или FROM (subquery) или FROM tableFunction
	if (s_from.ignore(pos, end, max_parsed_pos, expected))
	{
		ws.ignore(pos, end);

		ParserString s_lparen("(");
		ParserString s_rparen(")");
		ParserString s_dot(".");
		ParserIdentifier ident;
		ParserFunction table_function;
		Pos before = pos;

		if (s_lparen.ignore(pos, end, max_parsed_pos, expected))
		{
			ws.ignore(pos, end);

			ParserSelectQuery select_p;
			if (!select_p.parse(pos, end, select_query->table, max_parsed_pos, expected))
				return false;

			ws.ignore(pos, end);

			if (!s_rparen.ignore(pos, end, max_parsed_pos, expected))
				return false;

			ws.ignore(pos, end);
		}
		else if (ident.parse(pos, end, select_query->table, max_parsed_pos, expected))
		{
			/// Если сразу после identifier идет скобка, значит это должна быть табличная функция
			if (s_lparen.ignore(pos, end, max_parsed_pos, expected))
			{
				pos = before;
				if (!table_function.parse(pos, end, select_query->table, max_parsed_pos, expected))
					return false;
				if (select_query->table)
					typeid_cast<ASTFunction &>(*select_query->table).kind = ASTFunction::TABLE_FUNCTION;
				ws.ignore(pos, end);
			}
			else
			{
				ws.ignore(pos, end);
				if (s_dot.ignore(pos, end, max_parsed_pos, expected))
				{
					select_query->database = select_query->table;
					if (!ident.parse(pos, end, select_query->table, max_parsed_pos, expected))
						return false;

					ws.ignore(pos, end);
				}

				if (select_query->database)
					typeid_cast<ASTIdentifier &>(*select_query->database).kind = ASTIdentifier::Database;
				typeid_cast<ASTIdentifier &>(*select_query->table).kind = ASTIdentifier::Table;
			}
		}
		else
			return false;

		/// Может быть указан алиас. На данный момент, он ничего не значит и не используется.
		ParserAlias().ignore(pos, end);
		ws.ignore(pos, end);
	}

	/** FINAL и SAMPLE может быть здесь или после всех JOIN-ов
	  *  (второй вариант был изначально сделан по ошибке, и его приходится поддерживать).
	  */
	auto parse_final_and_sample = [&]() -> bool
	{
		/// FINAL
		if (!select_query->final
			&& s_final.ignore(pos, end, max_parsed_pos, expected))
		{
			select_query->final = true;

			ws.ignore(pos, end);
		}

		/// SAMPLE number
		if (!select_query->sample_size
			&& s_sample.ignore(pos, end, max_parsed_pos, expected))
		{
			ws.ignore(pos, end);

			ParserNumber num;

			if (!num.parse(pos, end, select_query->sample_size, max_parsed_pos, expected))
				return false;

			ws.ignore(pos, end);
		}

		return true;
	};

	if (!parse_final_and_sample())
		return false;

	/// [LEFT] ARRAY JOIN expr list
	Pos saved_pos = pos;
	bool has_array_join = false;
	if (s_left.ignore(pos, end, max_parsed_pos, expected) && ws.ignore(pos, end) && s_array.ignore(pos, end, max_parsed_pos, expected))
	{
		select_query->array_join_is_left = true;
		has_array_join = true;
	}
	else
	{
		pos = saved_pos;
		if (s_array.ignore(pos, end, max_parsed_pos, expected))
			has_array_join = true;
	}

	if (has_array_join)
	{
		ws.ignore(pos, end);

		if (!s_join.ignore(pos, end, max_parsed_pos, expected))
			return false;

		ws.ignore(pos, end);

		if (!exp_list.parse(pos, end, select_query->array_join_expression_list, max_parsed_pos, expected))
			return false;

		ws.ignore(pos, end);
	}

	/// [GLOBAL] [ANY|ALL] INNER|LEFT|RIGHT|FULL|CROSS [OUTER] JOIN (subquery)|table_name USING tuple
	join.parse(pos, end, select_query->join, max_parsed_pos, expected);

	if (!parse_final_and_sample())
		return false;

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

	/// LIMIT length или LIMIT offset, length
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

	bool has_format = false;

	/// FORMAT format_name
	if (s_format.ignore(pos, end, max_parsed_pos, expected))
	{
		ws.ignore(pos, end);

		ParserIdentifier format_p;

		if (!format_p.parse(pos, end, select_query->format, max_parsed_pos, expected))
			return false;
		typeid_cast<ASTIdentifier &>(*select_query->format).kind = ASTIdentifier::Format;

		ws.ignore(pos, end);
		has_format = true;
	}

	// UNION ALL select query
	if (s_union.ignore(pos, end, max_parsed_pos, expected))
	{
		ws.ignore(pos, end);

		if (s_all.ignore(pos, end, max_parsed_pos, expected))
		{
			if (has_format)
			{
				/// FORMAT может быть задан только в последнем запросе цепочки UNION ALL.
				expected = "FORMAT only in the last SELECT of the UNION ALL chain";
				return false;
			}

			ParserSelectQuery select_p;
			if (!select_p.parse(pos, end, select_query->next_union_all, max_parsed_pos, expected))
				return false;
			auto next_select_query = static_cast<ASTSelectQuery *>(&*select_query->next_union_all);
			next_select_query->prev_union_all = node;
		}
		else
			return false;

		ws.ignore(pos, end);
	}

	select_query->range = StringRange(begin, pos);

	select_query->children.push_back(select_query->select_expression_list);
	if (select_query->database)
		select_query->children.push_back(select_query->database);
	if (select_query->table)
		select_query->children.push_back(select_query->table);
	if (select_query->array_join_expression_list)
		select_query->children.push_back(select_query->array_join_expression_list);
	if (select_query->join)
		select_query->children.push_back(select_query->join);
	if (select_query->sample_size)
		select_query->children.push_back(select_query->sample_size);
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
