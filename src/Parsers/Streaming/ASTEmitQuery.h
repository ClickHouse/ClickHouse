#pragma once

#include <Parsers/IAST.h>

namespace DB
{
struct ASTEmitQuery : public IAST
{
public:
    bool streaming = false;

    /// [PERIODIC INTERVAL 1 SECOND]
    ASTPtr periodic_interval;

    String getID(char) const override { return "Emit"; }

    ASTPtr clone() const override { return std::make_shared<ASTEmitQuery>(*this); }

    void formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const override;

    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;
};
}
