#include <Parsers/IAST.h>

namespace DB
{
struct ASTFinishCollectingWorkloadQuery : public IAST
{
    String getID(char) const override { return "FinishCollectingWorkload"; }
    ASTPtr clone() const override { return std::make_shared<ASTFinishCollectingWorkloadQuery>(*this); }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;

};

}
