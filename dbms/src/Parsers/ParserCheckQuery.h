#pragma once

#include <Parsers/ParserQueryWithOutput.h>

namespace DB
{
/** Query of form
 * CHECK [TABLE] [database.]table
 */
class ParserCheckQuery : public IParserBase
{
protected:
    const char * getName() const { return "ALTER query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};

}
