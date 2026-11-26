#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/// Parser for ASTTimeInterval
class ParserTimeInterval : public IParserBase
{
public:
    struct Options
    {
        bool allow_mixing_calendar_and_clock_units = true;
        bool allow_zero = false;
    };

    ParserTimeInterval();
    explicit ParserTimeInterval(Options opt);

protected:
    Options options;

    const char * getName() const override { return "time interval"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
