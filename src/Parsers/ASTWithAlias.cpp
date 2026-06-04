#include <Common/SipHash.h>
#include <Parsers/ASTWithAlias.h>
#include <Parsers/ASTQueryParameter.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <iostream>

namespace DB
{

ASTWithAlias::ASTWithAlias() = default;
ASTWithAlias::~ASTWithAlias() = default;
ASTWithAlias::ASTWithAlias(const ASTWithAlias &) = default;
ASTWithAlias & ASTWithAlias::operator=(const ASTWithAlias &) = default;

static void writeAlias(const String & name, WriteBuffer & ostr, const ASTWithAlias::FormatSettings & settings)
{
    ostr << " AS ";
    settings.writeIdentifier(ostr, name, /*ambiguous=*/false);
}


void ASTWithAlias::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    /// This is needed for distributed queries with the old analyzer. Remove it after removing the old analyzer.
    /// If we have previously output this node elsewhere in the query, now it is enough to output only the alias.
    if (settings.collapse_identical_nodes_to_aliases && !alias.empty() && !state.printed_asts_with_alias.emplace(frame.current_select, alias, getTreeHash(/*ignore_aliases=*/ true)).second)
    {
        settings.writeIdentifier(ostr, alias, /*ambiguous=*/false);
    }
    else if (frame.parenthesize_alias_inner_only && !alias.empty())
    {
        /// `IAST::format` deferred parens emission to us so we can produce `(expr) AS alias`
        /// instead of `(expr AS alias)`. At the top level of an expression / SELECT element /
        /// WHERE clause the latter re-formats to the former because the outer parens are not
        /// needed. Inside an operator chain `IAST::format` keeps the parens itself, producing
        /// `(expr AS alias)` so the alias does not terminate the SELECT element parser early.
        ostr.write('(');
        FormatStateStacked inner = frame;
        inner.parenthesize_alias_inner_only = false;
        inner.need_parens = false;
        formatImplWithoutAlias(ostr, settings, state, inner);
        ostr.write(')');
        writeAlias(alias, ostr, settings);
    }
    else
    {
        /// When the parent operator requires parentheses around this expression and the
        /// expression has an alias, wrap the entire `expr AS alias` in parentheses.
        /// Without the wrap, `a AND b AS x AND c` would re-parse with the alias attached
        /// to `b` only instead of to `(a AND b)`. After re-parsing, the parser sets
        /// `parenthesized=true` on the aliased node; the next format goes through the
        /// `parenthesize_alias_inner_only` branch above.
        const bool wrap_around_alias = frame.need_parens && !alias.empty();
        if (wrap_around_alias)
        {
            ostr.write('(');
            frame.need_parens = false;
        }
        formatImplWithoutAlias(ostr, settings, state, frame);
        if (!alias.empty())
            writeAlias(alias, ostr, settings);
        if (wrap_around_alias)
            ostr.write(')');
    }
}

void ASTWithAlias::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    if (!alias.empty() && !ignore_aliases)
        hash_state.update(alias);
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}

void ASTWithAlias::appendColumnName(WriteBuffer & ostr) const
{
    if (preferAliasToColumnName() && !alias.empty())
        writeString(alias, ostr);
    else
        appendColumnNameImpl(ostr);
}

void ASTWithAlias::appendColumnNameWithoutAlias(WriteBuffer & ostr) const
{
    appendColumnNameImpl(ostr);
}

}
