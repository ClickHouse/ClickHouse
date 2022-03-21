#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
class ParserJSONPathQuery : public IParserBase
{
protected:
    const char * getName() const override { return "ParserJSONPathQuery"; }
    bool parseImpl(Pos & pos, ASTPtr & query, Expected & expected) override;
};
}
