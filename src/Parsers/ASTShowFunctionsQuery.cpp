#include <Parsers/ASTShowFunctionsQuery.h>
#include <Common/SipHash.h>
#include <Common/quoteString.h>


namespace DB
{

ASTPtr ASTShowFunctionsQuery::clone() const
{
    auto res = make_intrusive<ASTShowFunctionsQuery>(*this);
    res->children.clear();
    cloneOutputOptions(*res);
    return res;
}

void ASTShowFunctionsQuery::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    ASTQueryWithOutput::updateTreeHashImpl(hash_state, ignore_aliases);
    /// Fold in the semantic fields that are not part of `children` (the base implementation only
    /// hashes `getID`) so two `SHOW FUNCTIONS` queries that differ only in these fields do not share
    /// a tree hash — see the header comment.
    hash_state.update(has_like);
    hash_state.update(case_insensitive_like);
    hash_state.update(like);
}

void ASTShowFunctionsQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings &, FormatState &, FormatStateStacked) const
{
    ostr << "SHOW FUNCTIONS";
    /// Emit the clause whenever a `LIKE` was present, even with an empty pattern (see the
    /// `has_like` comment in the header): dropping it would lose the clause and its
    /// `case_insensitive_like` modifier on a format -> parse round-trip (which the rewrite-rule
    /// matcher's tree-hash consistency check relies on).
    if (has_like)
        ostr << (case_insensitive_like ? " ILIKE " : " LIKE ") << quoteString(like);
}

}
