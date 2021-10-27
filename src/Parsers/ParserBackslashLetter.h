#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

/** Query like this:
  * \l or \d
  * or \c db_name
  */
class ParserBackslashLetter final : public IParserBase
{
protected:
    const char * getName() const override { return "\\l|d|c 'name'"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
