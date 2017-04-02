#pragma once

#include <Parsers/ParserQueryWithOutput.h>

namespace DB
{
/** Запрос вида
 * CHECK [TABLE] [database.]table
 */
class ParserCheckQuery : public IParserBase
{
protected:
    const char * getName() const { return "ALTER query"; }
    bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);
};

}
