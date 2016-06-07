#pragma once

#include <DB/Parsers/IParserBase.h>
#include <DB/Parsers/ExpressionElementParsers.h>


namespace DB
{

/** Запрос типа такого:
  * SET [GLOBAL] name1 = value1, name2 = value2, ...
  */
class ParserSetQuery : public IParserBase
{
public:
	ParserSetQuery(bool parse_only_internals_ = false) : parse_only_internals(parse_only_internals_) {}

protected:
	const char * getName() const { return "SET query"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);

	/// Парсить список name = value пар, без SET [GLOBAL].
	bool parse_only_internals;
};

}
