#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

class ParserReadFromProjectionSettings : public IParserBase
{
public:
    const char * getName() const override { return "PROJECTION settings"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
