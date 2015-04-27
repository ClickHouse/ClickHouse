#include <DB/Parsers/ParserAlterQuery.h>
#include <DB/Parsers/CommonParsers.h>
#include <DB/Parsers/ExpressionElementParsers.h>
#include <DB/Parsers/ParserCreateQuery.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ExpressionElementParsers.h>
#include <DB/Parsers/ASTAlterQuery.h>
#include <DB/Parsers/ASTLiteral.h>

namespace DB
{
bool ParserAlterQuery::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
	Pos begin = pos;

	ParserWhiteSpaceOrComments ws;
	ParserString s_alter("ALTER", true, true);
	ParserString s_table("TABLE", true, true);
	ParserString s_dot(".");

	ParserString s_add("ADD", true, true);
	ParserString s_column("COLUMN", true, true);
	ParserString s_after("AFTER", true, true);
	ParserString s_modify("MODIFY", true, true);

	ParserString s_drop("DROP", true, true);
	ParserString s_detach("DETACH", true, true);
	ParserString s_attach("ATTACH", true, true);
	ParserString s_fetch("FETCH", true, true);
	ParserString s_freeze("FREEZE", true, true);
	ParserString s_unreplicated("UNREPLICATED", true, true);
	ParserString s_part("PART", true, true);
	ParserString s_partition("PARTITION", true, true);
	ParserString s_from("FROM", true, true);
	ParserString s_comma(",");

	ParserIdentifier table_parser;
	ParserCompoundIdentifier parser_name;
	ParserCompoundColumnDeclaration parser_col_decl;
	ParserLiteral parser_literal;
	ParserStringLiteral parser_string_literal;

	ASTPtr table;
	ASTPtr database;
	ASTPtr col_type;
	ASTPtr col_after;
	ASTPtr col_drop;

	ASTAlterQuery * query = new ASTAlterQuery();
	ASTPtr query_ptr = query;

	ws.ignore(pos, end);
	if (!s_alter.ignore(pos, end, max_parsed_pos, expected))
		return false;

	ws.ignore(pos, end);
	if (!s_table.ignore(pos, end, max_parsed_pos, expected))
		return false;

	ws.ignore(pos, end);

	if (!table_parser.parse(pos, end, database, max_parsed_pos, expected))
		return false;

	/// Parse [db].name
	if (s_dot.ignore(pos, end))
	{
		if (!table_parser.parse(pos, end, table, max_parsed_pos, expected))
			return false;

		query->table = typeid_cast<ASTIdentifier &>(*table).name;
		query->database = typeid_cast<ASTIdentifier &>(*database).name;
	}
	else
	{
		table = database;
		query->table = typeid_cast<ASTIdentifier &>(*table).name;
	}

	bool parsing_finished = false;
	do
	{
		ASTAlterQuery::Parameters params;
		ws.ignore(pos, end);

		if (s_add.ignore(pos, end, max_parsed_pos, expected))
		{
			ws.ignore(pos, end);
			if (!s_column.ignore(pos, end, max_parsed_pos, expected))
				return false;
			ws.ignore(pos, end);

			parser_col_decl.parse(pos, end, params.col_decl, max_parsed_pos, expected);

			ws.ignore(pos, end);
			if (s_after.ignore(pos, end, max_parsed_pos, expected))
			{
				ws.ignore(pos, end);

				if(!parser_name.parse(pos, end, params.column, max_parsed_pos, expected))
					return false;
			}

			params.type = ASTAlterQuery::ADD_COLUMN;
		}
		else if (s_drop.ignore(pos, end, max_parsed_pos, expected))
		{
			ws.ignore(pos, end);

			if (s_unreplicated.ignore(pos, end, max_parsed_pos, expected))
			{
				params.unreplicated = true;
				ws.ignore(pos, end);

				if (s_partition.ignore(pos, end, max_parsed_pos, expected))
				{
					ws.ignore(pos, end);

					if (!parser_literal.parse(pos, end, params.partition, max_parsed_pos, expected))
						return false;

					params.type = ASTAlterQuery::DROP_PARTITION;
				}
				else
					return false;
			}
			else if (s_partition.ignore(pos, end, max_parsed_pos, expected))
			{
				ws.ignore(pos, end);

				if (!parser_literal.parse(pos, end, params.partition, max_parsed_pos, expected))
					return false;

				params.type = ASTAlterQuery::DROP_PARTITION;
			}
			else if (s_column.ignore(pos, end, max_parsed_pos, expected))
			{
				ws.ignore(pos, end);

				if (!parser_name.parse(pos, end, params.column, max_parsed_pos, expected))
					return false;

				params.type = ASTAlterQuery::DROP_COLUMN;
				params.detach = false;
			}
			else
				return false;
		}
		else if (s_detach.ignore(pos, end, max_parsed_pos, expected))
		{
			ws.ignore(pos, end);

			if (!s_partition.ignore(pos, end, max_parsed_pos, expected))
				return false;

			ws.ignore(pos, end);

			if (!parser_literal.parse(pos, end, params.partition, max_parsed_pos, expected))
				return false;

			params.type = ASTAlterQuery::DROP_PARTITION;
			params.detach = true;
		}
		else if (s_attach.ignore(pos, end, max_parsed_pos, expected))
		{
			ws.ignore(pos, end);

			if (s_unreplicated.ignore(pos, end, max_parsed_pos, expected))
			{
				params.unreplicated = true;
				ws.ignore(pos, end);
			}

			if (s_part.ignore(pos, end, max_parsed_pos, expected))
				params.part = true;
			else if (!s_partition.ignore(pos, end, max_parsed_pos, expected))
				return false;

			ws.ignore(pos, end);

			if (!parser_literal.parse(pos, end, params.partition, max_parsed_pos, expected))
				return false;

			params.type = ASTAlterQuery::ATTACH_PARTITION;
		}
		else if (s_fetch.ignore(pos, end, max_parsed_pos, expected))
		{
			ws.ignore(pos, end);

			if (!s_partition.ignore(pos, end, max_parsed_pos, expected))
				return false;

			ws.ignore(pos, end);

			if (!parser_literal.parse(pos, end, params.partition, max_parsed_pos, expected))
				return false;

			ws.ignore(pos, end);

			if (!s_from.ignore(pos, end, max_parsed_pos, expected))
				return false;

			ws.ignore(pos, end);

			ASTPtr ast_from;
			if (!parser_string_literal.parse(pos, end, ast_from, max_parsed_pos, expected))
				return false;

			params.from = typeid_cast<const ASTLiteral &>(*ast_from).value.get<const String &>();
			params.type = ASTAlterQuery::FETCH_PARTITION;
		}
		else if (s_freeze.ignore(pos, end, max_parsed_pos, expected))
		{
			ws.ignore(pos, end);

			if (!s_partition.ignore(pos, end, max_parsed_pos, expected))
				return false;

			ws.ignore(pos, end);

			if (!parser_literal.parse(pos, end, params.partition, max_parsed_pos, expected))
				return false;

			ws.ignore(pos, end);

			params.type = ASTAlterQuery::FREEZE_PARTITION;
		}
		else if (s_modify.ignore(pos, end, max_parsed_pos, expected))
		{
			ws.ignore(pos, end);
			if (!s_column.ignore(pos, end, max_parsed_pos, expected))
				return false;
			ws.ignore(pos, end);

			if (!parser_col_decl.parse(pos, end, params.col_decl, max_parsed_pos, expected))
				return false;

			ws.ignore(pos, end);

			params.type = ASTAlterQuery::MODIFY_COLUMN;
		}
		else
			return false;

		ws.ignore(pos, end);

		if (!s_comma.ignore(pos, end, max_parsed_pos, expected))
		{
			ws.ignore(pos, end);
			parsing_finished = true;
		}

		query->addParameters(params);
	}
	while (!parsing_finished);

	query->range = StringRange(begin, end);
	node = query_ptr;

	return true;
}
}
