#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTExistsQuery.h>

#include <DB/Parsers/CommonParsers.h>
#include <DB/Parsers/ParserExistsQuery.h>


namespace DB
{


bool ParserExistsQuery::parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
{
	Pos begin = pos;

	ParserWhiteSpaceOrComments ws;
	ParserString s_exists("EXISTS", true, true);
	ParserString s_table("TABLE", true, true);
	ParserString s_format("FORMAT", true, true);
	ParserString s_dot(".");
	ParserIdentifier name_p;

	ASTPtr database;
	ASTPtr table;
	ASTPtr format;

	ws.ignore(pos, end);

	if (!s_exists.ignore(pos, end, expected))
		return false;

	ws.ignore(pos, end);

	if (!s_table.ignore(pos, end, expected))
		return false;

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

	ws.ignore(pos, end);
	
	if (s_format.ignore(pos, end, expected))
	{
		ws.ignore(pos, end);
		
		ParserIdentifier format_p;
		
		if (!format_p.parse(pos, end, format, expected))
			return false;
		dynamic_cast<ASTIdentifier &>(*format).kind = ASTIdentifier::Format;
		
		ws.ignore(pos, end);
	}

	ASTExistsQuery * query = new ASTExistsQuery(StringRange(begin, pos));
	node = query;

	if (database)
		query->database = dynamic_cast<ASTIdentifier &>(*database).name;
	if (table)
		query->table = dynamic_cast<ASTIdentifier &>(*table).name;
	if (format)
	{
		query->format = format;
		query->children.push_back(format);
	}

	return true;
}


}
