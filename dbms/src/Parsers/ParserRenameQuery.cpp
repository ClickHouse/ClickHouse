#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTRenameQuery.h>

#include <DB/Parsers/CommonParsers.h>
#include <DB/Parsers/ParserRenameQuery.h>


namespace DB
{


/// Парсит database.table или table.
static bool parseDatabaseAndTable(ASTRenameQuery::Table & db_and_table, IParser::Pos & pos, IParser::Pos end, const char *& expected)
{
	ParserIdentifier name_p;
	ParserWhiteSpaceOrComments ws;
	ParserString s_dot(".");
	
	ASTPtr database;
	ASTPtr table;

	ws.ignore(pos, end);

	if (!name_p.parse(pos, end, table, expected))
		return false;

	ws.ignore(pos, end);

	if (s_dot.ignore(pos, end, expected))
	{
		database = table;
		if (!name_p.parse(pos, end, table, expected))
			return false;

		ws.ignore(pos, end);
	}

	db_and_table.database = database ? dynamic_cast<const ASTIdentifier &>(*database).name : "";
	db_and_table.table = dynamic_cast<const ASTIdentifier &>(*table).name;

	return true;
}

	
bool ParserRenameQuery::parseImpl(Pos & pos, Pos end, ASTPtr & node, const char *& expected)
{
	Pos begin = pos;

	ParserWhiteSpaceOrComments ws;
	ParserString s_rename("RENAME", true, true);
	ParserString s_table("TABLE", true, true);
	ParserString s_to("TO", true, true);
	ParserString s_comma(",");
	
	ws.ignore(pos, end);

	if (!s_rename.ignore(pos, end, expected))
		return false;

	ws.ignore(pos, end);

	if (!s_table.ignore(pos, end, expected))
		return false;

	ASTRenameQuery::Elements elements;

	while (true)
	{
		ws.ignore(pos, end);
		
		if (!elements.empty() && !s_comma.ignore(pos, end))
			break;

		ws.ignore(pos, end);

		elements.push_back(ASTRenameQuery::Element());

		if (!parseDatabaseAndTable(elements.back().from, pos, end, expected)
			|| !s_to.ignore(pos, end)
			|| !parseDatabaseAndTable(elements.back().to, pos, end, expected))
			return false;
	}

	ASTRenameQuery * query = new ASTRenameQuery(StringRange(begin, pos));
	node = query;

	query->elements = elements;
	return true;
}


}
