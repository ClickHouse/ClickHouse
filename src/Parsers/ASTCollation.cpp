#include <Parsers/ASTCollation.h>

namespace DB
{
    ASTPtr ASTCollation::clone() const
    {
        auto res = std::make_shared<ASTCollation>(*this);
        res->collation = collation->clone();
        return res;
    }

    void ASTCollation::formatImpl(const FormattingBuffer &out) const
    {
        if (collation)
        {
            collation->formatImpl(out);
        }

    }

}
