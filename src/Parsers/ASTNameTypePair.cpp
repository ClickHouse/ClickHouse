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


void ASTNameTypePair::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << backQuoteIfNeed(name) << ' ';
    type->formatImpl(settings, state, frame);
}

}
