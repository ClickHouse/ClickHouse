#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTDropQuery.h>

#include <DB/Parsers/CommonParsers.h>
#include <DB/Parsers/ParserDropQuery.h>


namespace DB
{


bool ParserDropQuery::parseImpl(Pos & pos, Pos end, ASTPtr & node, const char *& expected)
{
	Pos begin = pos;

	ParserWhiteSpaceOrComments ws;
	ParserString s_drop("DROP", true, true);
	ParserString s_detach("DETACH", true, true);
	ParserString s_table("TABLE", true, true);
	ParserString s_database("DATABASE", true, true);
	ParserString s_dot(".");
	ParserString s_if("IF", true, true);
	ParserString s_exists("EXISTS", true, true);
	ParserIdentifier name_p;

	ASTPtr database;
	ASTPtr table;
	bool detach = false;
	bool if_exists = false;

	ws.ignore(pos, end);

	if (!s_drop.ignore(pos, end, expected))
	{
		if (s_detach.ignore(pos, end, expected))
			detach = true;
		else
			return false;
	}

	ws.ignore(pos, end);

	if (s_database.ignore(pos, end, expected))
	{
		ws.ignore(pos, end);

		if (s_if.ignore(pos, end, expected)
			&& ws.ignore(pos, end)
			&& s_exists.ignore(pos, end, expected)
			&& ws.ignore(pos, end))
			if_exists = true;

		if (!name_p.parse(pos, end, database, expected))
			return false;
	}
	else
	{
		if (!s_table.ignore(pos, end, expected))
			return false;

		ws.ignore(pos, end);

		if (s_if.ignore(pos, end, expected)
			&& ws.ignore(pos, end)
			&& s_exists.ignore(pos, end, expected)
			&& ws.ignore(pos, end))
			if_exists = true;

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
	}

	ws.ignore(pos, end);

	ASTDropQuery * query = new ASTDropQuery(StringRange(begin, pos));
	node = query;

	query->detach = detach;
	query->if_exists = if_exists;
	if (database)
		query->database = dynamic_cast<ASTIdentifier &>(*database).name;
	if (table)
		query->table = dynamic_cast<ASTIdentifier &>(*table).name;

	return true;
}


}
