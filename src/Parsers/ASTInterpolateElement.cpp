#include <Parsers/ASTInterpolateElement.h>
#include <IO/Operators.h>
#include <IO/WriteHelpers.h>
#include <Common/SipHash.h>


namespace DB
{

ASTPtr ASTInterpolateElement::clone() const
{
    auto clone = make_intrusive<ASTInterpolateElement>(*this);
    clone->expr = clone->expr->clone();
    clone->children.clear();
    clone->children.push_back(clone->expr);
    return clone;
}

void ASTInterpolateElement::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    /// The target column name and its double-quote flag are semantic in `standard` mode, so they
    /// must be visible to `QueryResultCache::Key`. Without this override the wrapper hashes only
    /// children, so `INTERPOLATE ("MyCol" AS ...)` and `INTERPOLATE (MyCol AS ...)` could share a
    /// cache entry even though their targets bind with different case-sensitivity.
    hash_state.update(column.size());
    hash_state.update(column);
    if (column_is_double_quoted)
        hash_state.update(true);
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}


void ASTInterpolateElement::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    /// Preserve double-quoting so the format/reparse round trip keeps the interpolation target
    /// case-sensitive in `standard` mode.
    if (column_is_double_quoted)
        writeDoubleQuotedString(column, ostr);
    else
        settings.writeIdentifier(ostr, column, /*ambiguous=*/true);
    ostr << " AS ";

    /// If the expression has an alias, we need to wrap it in parentheses
    /// to avoid ambiguity with double AS: `col AS expr AS alias` is not parseable,
    /// but `col AS (expr AS alias)` is.
    bool need_parens = !expr->tryGetAlias().empty();
    if (need_parens)
    {
        ostr << '(';
        /// We have just emitted `(` around the expression, so suppress the
        /// child's own `parenthesized` parens (which would otherwise duplicate ours).
        FormatStateStacked inner_frame = frame;
        inner_frame.wrapped_in_parens = true;
        expr->format(ostr, settings, state, inner_frame);
        ostr << ')';
    }
    else
    {
        expr->format(ostr, settings, state, frame);
    }
}

}
