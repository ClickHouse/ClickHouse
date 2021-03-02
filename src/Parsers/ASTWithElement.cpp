#include <Parsers/ASTWithElement.h>
#include <Parsers/ASTWithAlias.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTWithElement::clone() const
{
    const auto res = std::make_shared<ASTWithElement>(*this);
    res->children.clear();
    res->subquery = subquery->clone();
    res->children.emplace_back(res->subquery);
    return res;
}

void ASTWithElement::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.writeIdentifier(name);
    settings.ostr << (settings.hilite ? hilite_keyword : "") << " AS " << (settings.hilite ? hilite_none : "");
    dynamic_cast<const ASTWithAlias &>(*subquery).formatImplWithoutAlias(settings, state, frame);
}

}
