#pragma once

#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTTimeInterval.h>

namespace DB
{

/// Strategy for MATERIALIZED VIEW ... REFRESH ..
class ASTRefreshStrategy : public IAST
{
public:
    enum class ScheduleKind : UInt8
    {
        UNKNOWN = 0,
        AFTER,
        EVERY
    };

    ASTSetQuery * settings = nullptr;
    ASTExpressionList * dependencies = nullptr;
    ASTTimeInterval * interval = nullptr;
    ASTTimePeriod * period = nullptr;
    ASTTimeInterval * periodic_offset = nullptr;
    ASTTimePeriod * spread = nullptr;
    ScheduleKind schedule_kind{ScheduleKind::UNKNOWN};

    String getID(char) const override { return "Refresh strategy definition"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
