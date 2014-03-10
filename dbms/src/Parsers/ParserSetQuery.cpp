#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Parsers/ASTSetQuery.h>

#include <DB/Parsers/CommonParsers.h>
#include <DB/Parsers/ParserSetQuery.h>


namespace DB
{


/// Парсит name = value.
static bool parseNameValuePair(ASTSetQuery::Change & change, IParser::Pos & pos, IParser::Pos end, const char *& expected)
{
	ParserIdentifier name_p;
	ParserLiteral value_p;
	ParserWhiteSpaceOrComments ws;
	ParserString s_eq("=");
	
	ASTPtr name;
	ASTPtr value;

	ws.ignore(pos, end);

	if (!name_p.parse(pos, end, name, expected))
		return false;

	ws.ignore(pos, end);

	if (!s_eq.ignore(pos, end, expected))
		return false;

	ws.ignore(pos, end);

	if (!value_p.parse(pos, end, value, expected))
		return false;

	ws.ignore(pos, end);

	change.name = dynamic_cast<const ASTIdentifier &>(*name).name;
	change.value = dynamic_cast<const ASTLiteral &>(*value).value;

	return true;
}

	
bool ParserSetQuery::parseImpl(Pos & pos, Pos end, ASTPtr & node, const char *& expected)
{
	Pos begin = pos;

	ParserWhiteSpaceOrComments ws;
	ParserString s_set("SET", true, true);
	ParserString s_global("GLOBAL", true, true);
	ParserString s_comma(",");
	
	ws.ignore(pos, end);

	if (!s_set.ignore(pos, end, expected))
		return false;

	ws.ignore(pos, end);

	bool global = s_global.ignore(pos, end, expected);

	ASTSetQuery::Changes changes;

	while (true)
	{
		ws.ignore(pos, end);
		
		if (!changes.empty() && !s_comma.ignore(pos, end))
			break;

		ws.ignore(pos, end);

		changes.push_back(ASTSetQuery::Change());

		if (!parseNameValuePair(changes.back(), pos, end, expected))
			return false;
	}

	ASTSetQuery * query = new ASTSetQuery(StringRange(begin, pos));
	node = query;

	query->changes = changes;
	query->global = global;
	
	return true;
}


}
