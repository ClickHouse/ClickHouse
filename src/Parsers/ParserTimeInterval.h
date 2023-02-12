#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/// Parser for ASTTimePeriod
class ParserTimePeriod : public IParserBase
{
protected:
    const char * getName() const override { return "time period"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/// Parser for ASTTimeInterval
class ParserTimeInterval : public IParserBase
{
protected:
    const char * getName() const override { return "time interval"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
