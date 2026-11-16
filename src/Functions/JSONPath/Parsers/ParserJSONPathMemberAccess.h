#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{
class ParserJSONPathMemberAccess : public IParserBase
{
    const char * getName() const override { return "ParserJSONPathMemberAccess"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
