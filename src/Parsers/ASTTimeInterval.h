#pragma once

#include <Parsers/IAST.h>

#include <Common/IntervalKind.h>

#include <map>

namespace DB
{

/// Simple periodic time interval like 10 SECOND
class ASTTimePeriod : public IAST
{
public:
    UInt64 value{0};
    IntervalKind kind{IntervalKind::Second};

    String getID(char) const override { return "TimePeriod"; }

    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

/// Compound time interval like 1 YEAR 3 DAY 15 MINUTE
class ASTTimeInterval : public IAST
{
public:
    std::map<IntervalKind, UInt64> kinds;

    String getID(char) const override { return "TimeInterval"; }

    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
