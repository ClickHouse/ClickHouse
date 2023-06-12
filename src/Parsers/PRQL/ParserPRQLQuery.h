#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{
class ParserPRQLQuery : public IParserBase
{
private:
    const char * end;
    const char * getName() const override { return "PRQL Statement"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    explicit ParserPRQLQuery(const char * end_) : end{end_} { }
};
}
