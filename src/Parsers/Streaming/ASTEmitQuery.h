#pragma once

#include <Parsers/IAST.h>

namespace DB
{
struct ASTEmitQuery : public IAST
{
public:
    bool streaming = false;
    /// [AFTER WATERMARK]
    bool after_watermark = false;

    /// proc time or event time processing. This is for (last-x for now)
    /// LAST 1h ON PROCTIME
    bool proc_time = false;

    /// [PERIODIC INTERVAL 1 SECOND]
    ASTPtr periodic_interval;
    /// [DELAY INTERVAL 1 SECOND]
    ASTPtr delay_interval;
    /// [LAST <last-x>]
    ASTPtr last_interval;
    /// [TIMEOUT INTERVAL 5 SECOND]
    ASTPtr timeout_interval;

    String getID(char) const override { return "Emit"; }

    ASTPtr clone() const override { return std::make_shared<ASTEmitQuery>(*this); }

    void formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const override;

    void updateTreeHashImpl(SipHash & hash_state) const override;
};
}
