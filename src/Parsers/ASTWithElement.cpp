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

void ASTWithElement::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');

    ostr << (settings.hilite ? hilite_alias : "");
    settings.writeIdentifier(ostr, name, /*ambiguous=*/false);
    ostr << (settings.hilite ? hilite_none : "");
    ostr << (settings.hilite ? hilite_keyword : "") << " AS" << (settings.hilite ? hilite_none : "");
    ostr << settings.nl_or_ws << indent_str;
    dynamic_cast<const ASTWithAlias &>(*subquery).formatImplWithoutAlias(ostr, settings, state, frame);
}

}
