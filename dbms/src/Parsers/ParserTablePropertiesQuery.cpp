#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/TablePropertiesQueriesASTs.h>

#include <DB/Parsers/CommonParsers.h>
#include <DB/Parsers/ParserTablePropertiesQuery.h>


namespace DB
{


bool ParserTablePropertiesQuery::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
	Pos begin = pos;

	ParserWhiteSpaceOrComments ws;
	ParserString s_exists("EXISTS", true, true);
	ParserString s_describe("DESCRIBE", true, true);
	ParserString s_desc("DESC", true, true);
	ParserString s_show("SHOW", true, true);
	ParserString s_create("CREATE", true, true);
	ParserString s_table("TABLE", true, true);
	ParserString s_format("FORMAT", true, true);
	ParserString s_dot(".");
	ParserIdentifier name_p;

	ASTPtr database;
	ASTPtr table;
	ASTPtr format;
	ASTPtr query_ptr;

	ws.ignore(pos, end);

	if (s_exists.ignore(pos, end, max_parsed_pos, expected))
	{
		query_ptr = new ASTExistsQuery;
	}
	else if (s_describe.ignore(pos, end, max_parsed_pos, expected) || s_desc.ignore(pos, end, max_parsed_pos, expected))
	{
		query_ptr = new ASTDescribeQuery;
	}
	else if (s_show.ignore(pos, end, max_parsed_pos, expected))
	{
		ws.ignore(pos, end);

		if (!s_create.ignore(pos, end, max_parsed_pos, expected))
			return false;

		query_ptr = new ASTShowCreateQuery;
	}
	else
	{
		return false;
	}


	ws.ignore(pos, end);

	s_table.ignore(pos, end, max_parsed_pos, expected);

	ws.ignore(pos, end);

	if (!name_p.parse(pos, end, table, max_parsed_pos, expected))
		return false;

	ws.ignore(pos, end);

	if (s_dot.ignore(pos, end, max_parsed_pos, expected))
	{
		database = table;
		if (!name_p.parse(pos, end, table, max_parsed_pos, expected))
			return false;

		ws.ignore(pos, end);
	}

	ws.ignore(pos, end);

	if (s_format.ignore(pos, end, max_parsed_pos, expected))
	{
		ws.ignore(pos, end);

		ParserIdentifier format_p;

		if (!format_p.parse(pos, end, format, max_parsed_pos, expected))
			return false;
		typeid_cast<ASTIdentifier &>(*format).kind = ASTIdentifier::Format;

		ws.ignore(pos, end);
	}

	ASTQueryWithTableAndOutput * query = dynamic_cast<ASTQueryWithTableAndOutput *>(&*query_ptr);

	query->range = StringRange(begin, pos);

	if (database)
		query->database = typeid_cast<ASTIdentifier &>(*database).name;
	if (table)
		query->table = typeid_cast<ASTIdentifier &>(*table).name;
	if (format)
	{
		query->format = format;
		query->children.push_back(format);
	}

	node = query_ptr;

	return true;
}


}
