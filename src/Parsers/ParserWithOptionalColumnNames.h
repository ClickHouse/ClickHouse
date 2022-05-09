#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** [(identifier [, identifier] ...)]
  */
class ParserWithOptionalColumnNames : public IParserBase
{
protected:
    const char * getName() const override { return "WITH column names"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
