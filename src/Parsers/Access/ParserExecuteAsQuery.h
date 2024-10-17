#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses queries like :
  * EXECUTE AS <user>
  */
class ParserExecuteAsQuery : public IParserBase
{
protected:
    const char * getName() const override { return "EXECUTE AS query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
