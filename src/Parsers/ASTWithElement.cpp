#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/ASTWithAlias.h>
#include <IO/Operators.h>
#include <IO/WriteHelpers.h>
#include <Common/SipHash.h>

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

void ASTWithElement::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');

    /// Preserve original double-quoting so format/reparse keeps the CTE name case-sensitive in `standard` mode
    if (name_is_double_quoted)
        writeDoubleQuotedString(name, ostr);
    else
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

void ASTWithElement::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    /// CTE name and its quote style are semantic in `standard` mode; otherwise two CTEs that
    /// differ only by `"X"` vs `X` would hash the same and a QueryResultCache could share entries.
    hash_state.update(name.size());
    hash_state.update(name);
    hash_state.update(is_materialized);
    if (name_is_double_quoted)
        hash_state.update(true);
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}

}
