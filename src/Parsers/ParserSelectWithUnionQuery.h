#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{


class ParserSelectWithUnionQuery : public IParserBase
{
public:
    std::map<String, Documentation> getDocumentation() const override;

protected:
    const char * getName() const override { return "SELECT query, possibly with UNION"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
