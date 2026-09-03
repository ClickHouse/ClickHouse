#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

class ParserPublicSSHKey : public IParserBase
{
protected:
    const char * getName() const override { return "PublicSSHKey"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
