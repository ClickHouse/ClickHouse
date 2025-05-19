#pragma once
#include <Parsers/IAST.h>

namespace DB
{
struct ASTStartCollectingWorkloadQuery : public IAST
{
    String getID(char) const override { return "StartCollectingWorkload"; }
    ASTPtr clone() const override { return std::make_shared<ASTStartCollectingWorkloadQuery>(*this); }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
