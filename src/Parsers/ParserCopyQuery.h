#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{


class ParserCopyQuery : public IParserBase
{
protected:
    const char * getName() const override { return "COPY query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    ParserCopyQuery() = default;
};

}
