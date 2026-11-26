#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
class ParserJSONPathStar : public IParserBase
{
private:
    [[nodiscard]] const char * getName() const override { return "ParserJSONPathStar"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    explicit ParserJSONPathStar() = default;
};

}
