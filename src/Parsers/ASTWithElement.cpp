#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/ASTWithAlias.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTWithElement::clone() const
{
    const auto res = make_intrusive<ASTWithElement>(*this);
    res->children.clear();
    res->subquery = subquery->clone();
    if (aliases)
        res->aliases = aliases->clone();
    res->children.emplace_back(res->subquery);
    return res;
}

void ASTWithElement::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    /// The name selects which CTE a reference resolves to, and `aliases` renames the subquery
    /// columns, but neither is a child, so without this both are absent from the hash.
    static_assert(sizeof(*this) == 80, "If members were added to ASTWithElement, hash them here unless they are purely cosmetic.");
    /// Length-prefixed, otherwise the name runs into whatever `getID` writes next.
    hash_state.update(name.size());
    hash_state.update(name);
    hash_state.update(is_materialized);
    hash_state.update(aliases != nullptr);
    if (aliases)
        aliases->updateTreeHash(hash_state, ignore_aliases);
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}

void ASTWithElement::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');

    settings.writeIdentifier(ostr, name, /*ambiguous=*/false);
    if (aliases)
    {
        const bool prep_whitespace = frame.expression_list_prepend_whitespace;
        frame.expression_list_prepend_whitespace = false;

        ostr << "(";
        aliases->format(ostr, settings, state, frame);
        ostr << ")";

        frame.expression_list_prepend_whitespace = prep_whitespace;
    }
    ostr << " AS" << (is_materialized ? " MATERIALIZED" : "");
    ostr << settings.nl_or_ws << indent_str;
    dynamic_cast<const ASTWithAlias &>(*subquery).formatImplWithoutAlias(ostr, settings, state, frame);
}

}
