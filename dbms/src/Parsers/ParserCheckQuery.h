#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{
/** Query of form
 * CHECK [TABLE] [database.]table
 */
class ParserCheckQuery : public IParserBase
{
protected:
    const char * getName() const  override{ return "ALTER query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
