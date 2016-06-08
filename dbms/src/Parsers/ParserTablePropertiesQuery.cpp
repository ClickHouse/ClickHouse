#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/TablePropertiesQueriesASTs.h>

#include <DB/Parsers/CommonParsers.h>
#include <DB/Parsers/ParserTablePropertiesQuery.h>


namespace DB
{


bool ParserTablePropertiesQuery::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
	Pos begin = pos;

	ParserString s_exists("EXISTS", true, true);
	ParserString s_describe("DESCRIBE", true, true);
	ParserString s_desc("DESC", true, true);
	ParserString s_show("SHOW", true, true);
	ParserString s_create("CREATE", true, true);
	ParserString s_table("TABLE", true, true);
	ParserString s_dot(".");
	ParserIdentifier name_p;

	ASTPtr database;
	ASTPtr table;
	ASTPtr query_ptr;

	ws.ignore(pos, end);

	if (s_exists.ignore(pos, end, max_parsed_pos, expected))
	{
		query_ptr = std::make_shared<ASTExistsQuery>();
	}
	else if (s_describe.ignore(pos, end, max_parsed_pos, expected) || s_desc.ignore(pos, end, max_parsed_pos, expected))
	{
		query_ptr = std::make_shared<ASTDescribeQuery>();
	}
	else if (s_show.ignore(pos, end, max_parsed_pos, expected))
	{
		ws.ignore(pos, end);

		if (!s_create.ignore(pos, end, max_parsed_pos, expected))
			return false;

		query_ptr = std::make_shared<ASTShowCreateQuery>();
	}
	else
	{
		return false;
	}

	ASTQueryWithTableAndOutput * query = dynamic_cast<ASTQueryWithTableAndOutput *>(&*query_ptr);

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

	/// FORMAT format_name
	if (!parseFormat(*query, pos, end, node, max_parsed_pos, expected))
		return false;

	query->range = StringRange(begin, pos);

	if (database)
		query->database = typeid_cast<ASTIdentifier &>(*database).name;
	if (table)
		query->table = typeid_cast<ASTIdentifier &>(*table).name;
	if (query->format)
		query->children.push_back(query->format);

	node = query_ptr;

	return true;
}


}
