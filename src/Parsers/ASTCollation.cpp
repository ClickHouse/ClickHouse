#include <Parsers/ASTCollation.h>

namespace DB
{
    ASTPtr ASTCollation::clone() const
    {
        auto res = std::make_shared<ASTCollation>(*this);
        res->collation = collation->clone();
        return res;
    }

    void ASTCollation::formatImpl(WriteBuffer & ostr, const FormatSettings &s, FormatState &state, FormatStateStacked frame) const
    {
        if (collation)
            collation->formatImpl(ostr, s, state, frame);
    }

}
