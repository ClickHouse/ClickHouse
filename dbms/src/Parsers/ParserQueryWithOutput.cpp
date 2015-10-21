#include <DB/Parsers/ParserQueryWithOutput.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ExpressionElementParsers.h>

namespace DB
{

bool ParserQueryWithOutput::parseFormat(ASTQueryWithOutput & query, Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
	ParserString s_format("FORMAT", true, true);

	if (s_format.ignore(pos, end, max_parsed_pos, expected))
	{
		ws.ignore(pos, end);

		ParserIdentifier format_p;

		if (!format_p.parse(pos, end, query.format, max_parsed_pos, expected))
			return false;
		typeid_cast<ASTIdentifier &>(*(query.format)).kind = ASTIdentifier::Format;

		ws.ignore(pos, end);
	}

	return true;
}

}
