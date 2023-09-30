#pragma once
#include <Parsers/IParserBase.h>
// cases
// - [ident]
// - ['ident']
// - ["ident"]
namespace DB
{
class ParserJSONPathMemberSquareBracketAccess : public IParserBase
{
private:
    const char * getName() const override { return "ParserJSONPathMemberSquareBracketAccess"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    explicit ParserJSONPathMemberSquareBracketAccess() = default;
};
}
