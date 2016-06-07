#pragma once

#include <DB/Parsers/ParserQueryWithOutput.h>

namespace DB
{
/** Запрос вида
 * CHECK [TABLE] [database.]table
 */
class ParserCheckQuery : public ParserQueryWithOutput
{
protected:
	const char * getName() const { return "ALTER query"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);
};

}
