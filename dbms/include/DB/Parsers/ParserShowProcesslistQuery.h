#pragma once

#include <DB/Parsers/IParserBase.h>
#include <DB/Parsers/CommonParsers.h>
#include <DB/Parsers/ExpressionElementParsers.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTShowProcesslistQuery.h>


namespace DB
{

/** Запрос SHOW PROCESSLIST
  */
class ParserShowProcesslistQuery : public IParserBase
{
protected:
	String getName() { return "SHOW PROCESSLIST query"; }

	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
	{
		Pos begin = pos;

		ParserWhiteSpaceOrComments ws;
		ParserString s_show("SHOW", true, true);
		ParserString s_processlist("PROCESSLIST", true, true);
		ParserString s_format("FORMAT", true, true);

		ASTPtr format;

		ws.ignore(pos, end);

		if (!s_show.ignore(pos, end, expected))
			return false;

		ws.ignore(pos, end);

		if (!s_processlist.ignore(pos, end, expected))
			return false;

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

		ASTShowProcesslistQuery * query = new ASTShowProcesslistQuery(StringRange(begin, pos));
		query->format = format;
		node = query;

		return true;
	}
};

}
