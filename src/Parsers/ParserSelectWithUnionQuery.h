#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{


class ParserSelectWithUnionQuery : public IParserBase
{
public:
    bool allow_query_parameters = false;

protected:
    const char * getName() const override { return "SELECT query, possibly with UNION"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
