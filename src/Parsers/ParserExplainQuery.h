#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{


class ParserExplainQuery : public IParserBase
{
protected:
    const char * end;

    const char * getName() const override { return "EXPLAIN"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    explicit ParserExplainQuery(const char* end_) : end(end_) {}
};

}
