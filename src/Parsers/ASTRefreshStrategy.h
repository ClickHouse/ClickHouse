#pragma once

#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTTimeInterval.h>

namespace DB
{

enum class RefreshScheduleKind : UInt8
{
    UNKNOWN = 0,
    AFTER,
    EVERY
};

/// Strategy for MATERIALIZED VIEW ... REFRESH ..
class ASTRefreshStrategy : public IAST
{
public:
    ASTSetQuery * settings = nullptr;
    ASTExpressionList * dependencies = nullptr;
    ASTTimeInterval * period = nullptr;
    ASTTimeInterval * offset = nullptr;
    ASTTimeInterval * spread = nullptr;
    RefreshScheduleKind schedule_kind{RefreshScheduleKind::UNKNOWN};
    bool append = false;

    String getID(char) const override { return "Refresh strategy definition"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
