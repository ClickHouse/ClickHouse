#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLQuery.h>

namespace DB
{

class ParserKQLDateTypeTimespan : public ParserKQLBase
{
public:
    enum class KQLTimespanUint : uint8_t
    {
        day,
        hour,
        minute,
        second,
        millisec,
        microsec,
        nanosec,
        tick
    };
    bool parseConstKQLTimespan(const String & text);
    double toSeconds();

protected:
    const char * getName() const override { return "KQLDateTypeTimespan"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    double time_span;
    KQLTimespanUint time_span_unit;
};

}
