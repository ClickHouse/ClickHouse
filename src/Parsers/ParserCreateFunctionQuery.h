#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/// CREATE FUNCTION test AS x -> x || '1'
class ParserCreateFunctionQuery : public IParserBase
{
public:
    std::map<String, Documentation> getDocumentation() const override;

protected:
    const char * getName() const override { return "CREATE FUNCTION query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
