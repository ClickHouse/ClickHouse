#include <Parsers/ASTWithElement.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTWithElement::clone() const
{
    const auto res = std::make_shared<ASTWithElement>(*this);
    res->children.clear();
    res->name = name;
    res->subquery = subquery->clone();
    res->children.emplace_back(res->subquery);
    return res;
}

void ASTWithElement::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.writeIdentifier(name);
    settings.ostr << (settings.hilite ? hilite_keyword : "") << " AS " << (settings.hilite ? hilite_none : "");
    subquery->formatImpl(settings, state, frame);
}
}
