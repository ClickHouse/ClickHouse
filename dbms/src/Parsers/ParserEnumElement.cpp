#include <DB/Parsers/ParserEnumElement.h>

#include <DB/Parsers/ASTEnumElement.h>
#include <DB/Parsers/CommonParsers.h>


namespace DB
{
bool ParserEnumElement::parseImpl(IParser::Pos & pos, IParser::Pos end, ASTPtr & node, IParser::Pos & max_parsed_pos, Expected & expected)
{
	ParserString equality_sign_parser("=");
	ParserWhiteSpace ws;
	const auto begin = pos;

	ASTPtr name;
	if (!name_parser.parse(pos, end, name, max_parsed_pos, expected))
		return false;

	ws.ignore(pos, end, max_parsed_pos, expected);

	if (!equality_sign_parser.ignore(pos, end, max_parsed_pos, expected))
		return false;

	ws.ignore(pos, end, max_parsed_pos, expected);

	ASTPtr value;
	if (!value_parser.parse(pos, end, value, max_parsed_pos, expected))
		return false;

	node = std::make_shared<ASTEnumElement>(
		StringRange{begin, pos}, static_cast<const ASTLiteral &>(*name).value.get<String>(), static_cast<const ASTLiteral &>(*value).value);

	return true;
}
}
