#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>

namespace DB
{
class ParserDeleteQuery : public IParserBase
{
protected:
    constexpr const char * getName() const final { return "DELETE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) final;
};
}
