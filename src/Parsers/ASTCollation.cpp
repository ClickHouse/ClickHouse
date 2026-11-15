#include <Parsers/ASTCollation.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{
    ASTPtr ASTCollation::clone() const
    {
        auto res = make_intrusive<ASTCollation>(*this);
        res->collation = collation->clone();
        return res;
    }

    void ASTCollation::formatImpl(WriteBuffer & ostr, const FormatSettings &s, FormatState &state, FormatStateStacked frame) const
    {
        if (collation)
            collation->format(ostr, s, state, frame);
    }

}
