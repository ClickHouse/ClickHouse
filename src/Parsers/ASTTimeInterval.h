#pragma once

#include <Parsers/IAST.h>

#include <Common/CalendarTimeInterval.h>

#include <map>

namespace DB
{

/// Compound time interval like 1 YEAR 3 DAY 15 MINUTE
class ASTTimeInterval : public IAST
{
public:
    CalendarTimeInterval interval;

    String getID(char) const override { return "TimeInterval"; }

    ASTPtr clone() const override;
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
