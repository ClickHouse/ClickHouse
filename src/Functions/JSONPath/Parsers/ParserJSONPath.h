#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/**
 * Entry parser for JSONPath
 */
class ParserJSONPath : public IParserBase
{
private:
    [[nodiscard]] const char * getName() const override { return "ParserJSONPath"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    explicit ParserJSONPath() = default;
};

}
