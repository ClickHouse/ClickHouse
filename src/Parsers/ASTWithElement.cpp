#include <Parsers/ASTIdentifier.h>
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
    if (aliases)
        res->aliases = aliases->clone();
    res->children.emplace_back(res->subquery);
    return res;
}

void ASTWithElement::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');

    settings.writeIdentifier(ostr, name, /*ambiguous=*/false);
    if (aliases)
    {
        const bool prep_whitespace = frame.expression_list_prepend_whitespace;
        frame.expression_list_prepend_whitespace = false;

        ostr << " (";
        aliases->format(ostr, settings, state, frame);
        ostr << ")";

        frame.expression_list_prepend_whitespace = prep_whitespace;
    }
    ostr << " AS";
    ostr << settings.nl_or_ws << indent_str;
    dynamic_cast<const ASTWithAlias &>(*subquery).formatImplWithoutAlias(ostr, settings, state, frame);
}

}
