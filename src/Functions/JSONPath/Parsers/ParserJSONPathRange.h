#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
class ParserJSONPathRange : public IParserBase
{
private:
    const char * getName() const override { return "ParserJSONPathRange"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    explicit ParserJSONPathRange() = default;
};

}
