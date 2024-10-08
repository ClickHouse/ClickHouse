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
    std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');

    settings.ostr << (settings.hilite ? hilite_alias : "");
    settings.writeIdentifier(name, /*ambiguous=*/false);
    settings.ostr << (settings.hilite ? hilite_none : "");
    settings.ostr << (settings.hilite ? hilite_keyword : "") << " AS" << (settings.hilite ? hilite_none : "");
    settings.ostr << settings.nl_or_ws << indent_str;
    dynamic_cast<const ASTWithAlias &>(*subquery).formatImplWithoutAlias(settings, state, frame);
}

}
