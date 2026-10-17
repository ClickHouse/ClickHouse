#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

/// Dummy parser, exists only to provide documentation.
class ParserOnCluster : public IParserBase
{
public:
    [[nodiscard]] std::map<String, Documentation> getDocumentation() const override;

protected:
    [[nodiscard]] const char * getName() const override { return "ON CLUSTER clause"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
