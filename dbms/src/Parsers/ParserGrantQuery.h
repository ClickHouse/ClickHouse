#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
class ParserGrantQuery : public IParserBase
{
protected:
    const char * getName() const override { return "GRANT or REVOKE query "; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
