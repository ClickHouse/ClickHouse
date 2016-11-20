#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Parsers/ASTSetQuery.h>

#include <DB/Parsers/CommonParsers.h>
#include <DB/Parsers/ParserSetQuery.h>

#include <DB/Common/typeid_cast.h>


namespace DB
{


/// Парсит name = value.
static bool parseNameValuePair(ASTSetQuery::Change & change, IParser::Pos & pos, IParser::Pos end, IParser::Pos & max_parsed_pos, Expected & expected)
{
	ParserIdentifier name_p;
	ParserLiteral value_p;
	ParserWhiteSpaceOrComments ws;
	ParserString s_eq("=");

	ASTPtr name;
	ASTPtr value;

	ws.ignore(pos, end);

	if (!name_p.parse(pos, end, name, max_parsed_pos, expected))
		return false;

	ws.ignore(pos, end);

	if (!s_eq.ignore(pos, end, max_parsed_pos, expected))
		return false;

	ws.ignore(pos, end);

	if (!value_p.parse(pos, end, value, max_parsed_pos, expected))
		return false;

	ws.ignore(pos, end);

	change.name = typeid_cast<const ASTIdentifier &>(*name).name;
	change.value = typeid_cast<const ASTLiteral &>(*value).value;

	return true;
}


bool ParserSetQuery::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
	Pos begin = pos;

	ParserWhiteSpaceOrComments ws;
	ParserString s_comma(",");

	bool global = false;

	if (!parse_only_internals)
	{
		ParserString s_set("SET", true, true);
		ParserString s_global("GLOBAL", true, true);

		ws.ignore(pos, end);

		if (!s_set.ignore(pos, end, max_parsed_pos, expected))
			return false;

		ws.ignore(pos, end);

		global = s_global.ignore(pos, end, max_parsed_pos, expected);
	}

	ASTSetQuery::Changes changes;

	while (true)
	{
		ws.ignore(pos, end);

		if (!changes.empty() && !s_comma.ignore(pos, end))
			break;

		ws.ignore(pos, end);

		changes.push_back(ASTSetQuery::Change());

		if (!parseNameValuePair(changes.back(), pos, end, max_parsed_pos, expected))
			return false;
	}

	auto query = std::make_shared<ASTSetQuery>(StringRange(begin, pos));
	node = query;

	query->changes = changes;
	query->global = global;

	return true;
}


}
