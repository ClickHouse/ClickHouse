#include <Parsers/IAST.h>

namespace DB
{
class ASTDefaultedColumn : public IAST
{
public:
    ASTPtr expression;
    ASTPtr name;

    String getID(char) const override { return "DefaultedColumn"; }

    ASTPtr clone() const override;

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};
}
