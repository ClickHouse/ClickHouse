#include <DB/Parsers/ASTLiteral.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTShowTablesQuery.h>

#include <DB/Parsers/CommonParsers.h>
#include <DB/Parsers/ParserShowTablesQuery.h>
#include <DB/Parsers/ExpressionElementParsers.h>


namespace DB
{


bool ParserShowTablesQuery::parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
{
	Pos begin = pos;

	ParserWhiteSpaceOrComments ws;
	ParserString s_show("SHOW", true, true);
	ParserString s_tables("TABLES", true, true);
	ParserString s_databases("DATABASES", true, true);
	ParserString s_from("FROM", true, true);
	ParserString s_not("NOT", true, true);
	ParserString s_like("LIKE", true, true);
	ParserString s_format("FORMAT", true, true);
	ParserStringLiteral like_p;
	ParserIdentifier name_p;

	ASTPtr like;
	ASTPtr database;
	ASTPtr format;
	
	ASTShowTablesQuery * query = new ASTShowTablesQuery;
	ASTPtr query_ptr = query;

	ws.ignore(pos, end);

	if (!s_show.ignore(pos, end, expected))
		return false;

	ws.ignore(pos, end);

	if (s_databases.ignore(pos, end))
	{
		query->databases = true;
	}
	else if (s_tables.ignore(pos, end, expected))
	{
		ws.ignore(pos, end);

		if (s_from.ignore(pos, end, expected))
		{
			ws.ignore(pos, end);

			if (!name_p.parse(pos, end, database, expected))
				return false;
		}

		ws.ignore(pos, end);

		if (s_not.ignore(pos, end, expected))
			query->not_like = true;
		
		if (s_like.ignore(pos, end, expected))
		{
			ws.ignore(pos, end);

			if (!like_p.parse(pos, end, like, expected))
				return false;
		}
		else if (query->not_like)
			return false;
	}
	else
	{
		pos = begin;
		return false;
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

	query->range = StringRange(begin, pos);
	
	if (database)
		query->from = dynamic_cast<ASTIdentifier &>(*database).name;
	if (like)
		query->like = safeGet<const String &>(dynamic_cast<ASTLiteral &>(*like).value);
	if (format)
	{
		query->format = format;
		query->children.push_back(format);
	}
	
	node = query_ptr;

	return true;
}


}
