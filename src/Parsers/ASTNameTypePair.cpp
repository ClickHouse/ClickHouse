#include <Parsers/ASTNameTypePair.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

ASTPtr ASTNameTypePair::clone() const
{
    auto res = std::make_shared<ASTNameTypePair>(*this);
    res->children.clear();

    if (type)
    {
        res->type = type->clone();
        res->children.push_back(res->type);
    }

    return res;
}


void ASTNameTypePair::formatImpl(FormattingBuffer out) const
{
    out.writeIndent();
    out.ostr << backQuoteIfNeed(name) << ' ';
    type->formatImpl(out);
}

}


