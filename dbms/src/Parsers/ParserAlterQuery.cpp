#include <DB/Parsers/ParserAlterQuery.h>
#include <DB/Parsers/CommonParsers.h>
#include <DB/Parsers/ExpressionElementParsers.h>
#include <DB/Parsers/ParserCreateQuery.h>

#include <boost/concept_check.hpp>

#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTAlterQuery.h>

namespace DB
{
bool ParserAlterQuery::parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
{
	Pos begin = pos;

	ParserWhiteSpaceOrComments ws;
	ParserString s_alter("ALTER", true, true);
	ParserString s_table("TABLE", true, true);
	ParserString s_dot(".");

	ParserString s_add("ADD", true, true);
	ParserString s_column("COLUMN", true, true);
	ParserString s_after("AFTER", true, true);

	ParserString s_drop("DROP", true, true);
	ParserString s_comma(",");

	ParserIdentifier parser_name;
	ParserNameTypePair parser_name_type;

	ASTPtr table;
	ASTPtr database;
	ASTPtr col_type;
	ASTPtr col_after;
	ASTPtr col_drop;

	ASTAlterQuery * query = new ASTAlterQuery();
	ASTPtr query_ptr = query;

	ws.ignore(pos, end);
	if (!s_alter.ignore(pos, end, expected))
		return false;

	ws.ignore(pos, end);
	if (!s_table.ignore(pos, end, expected))
		return false;

	ws.ignore(pos, end);

	if (!parser_name.parse(pos, end, database, expected))
		return false;

	/// Parse [db].name
	if (s_dot.ignore(pos, end))
	{
		if (!parser_name.parse(pos, end, table, expected))
			return false;

		query->table = dynamic_cast<ASTIdentifier &>(*table).name;
		query->database = dynamic_cast<ASTIdentifier &>(*database).name;
	}
	else
	{
		table = database;
		query->table = dynamic_cast<ASTIdentifier &>(*table).name;
	}

	bool parsing_finished = false;
	do
	{
		ASTAlterQuery::Parameters params;
		ws.ignore(pos, end);

		if (s_add.ignore(pos, end, expected))
		{
			ws.ignore(pos, end);
			s_column.ignore(pos, end, expected);
			ws.ignore(pos, end);

			parser_name_type.parse(pos, end, params.name_type, expected);

			ws.ignore(pos, end);
			if (s_after.ignore(pos, end, expected))
			{
				ws.ignore(pos, end);

				if(!parser_name.parse(pos, end, params.column, expected))
					return false;
			}

			params.type = ASTAlterQuery::ADD;
		}
		else
		{
			if (s_drop.ignore(pos, end, expected))
			{
				ws.ignore(pos, end);
				s_column.ignore(pos, end, expected);
				ws.ignore(pos, end);

				parser_name.parse(pos, end, params.column, expected);

				params.type = ASTAlterQuery::DROP;
			}
			else
				return false;
		}

		ws.ignore(pos, end);

		if (!s_comma.ignore(pos, end, expected))
		{
			ws.ignore(pos, end);
			parsing_finished = true;
		}

		query->parameters.push_back(params);
	}
	while (!parsing_finished);

	query->range = StringRange(begin, end);
	node = query_ptr;

	return true;
}
}
