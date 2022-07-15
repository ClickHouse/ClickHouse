#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{
class ParserKQLBase : public IParserBase
{
public:
    virtual bool parsePrepare(Pos & pos);
    virtual String getExprFromToken(Pos &pos);
    std::vector<Pos> op_pos;

};

class ParserKQLQuery : public IParserBase
{
protected:
    const char * getName() const override { return "KQL query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
