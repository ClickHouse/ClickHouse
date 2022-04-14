#include <Parsers/ASTCollation.h>

namespace DB
{
    ASTPtr ASTCollation::clone() const
    {
        auto res = std::make_shared<ASTCollation>(*this);
        res->null_modifier = null_modifier;
        res->collation = collation;
        return res;
    }

    void ASTCollation::formatImpl(const FormatSettings &s, FormatState &state, FormatStateStacked frame) const
    {
        if (collation)
        {
            collation->formatImpl(s, state, frame);
        }

        if (!null_modifier)
        {
            s.ostr << " NOT NULL ";
        }

    }

}
