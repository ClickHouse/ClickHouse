#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

class ParserAlterNamedCollectionQuery : public IParserBase
{
public:
    std::map<String, Documentation> getDocumentation() const override;

protected:
    const char * getName() const override { return "Alter NAMED COLLECTION query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
