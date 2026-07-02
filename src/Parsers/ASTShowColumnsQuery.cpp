#include <Parsers/ASTShowColumnsQuery.h>
#include <Parsers/ASTLiteral.h>

#include <iomanip>
#include <Common/SipHash.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTShowColumnsQuery::clone() const
{
    auto res = make_intrusive<ASTShowColumnsQuery>(*this);
    res->children.clear();
    cloneOutputOptions(*res);
    return res;
}

void ASTShowColumnsQuery::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
    /// Fold in the semantic fields that are not part of `children` (the base implementation only
    /// hashes `getID`) so two `SHOW COLUMNS` queries that differ only in these fields do not share
    /// a tree hash — see the header comment.
    hash_state.update(extended);
    hash_state.update(full);
    hash_state.update(not_like);
    hash_state.update(case_insensitive_like);
    hash_state.update(database);
    hash_state.update(table);
    hash_state.update(like);
    hash_state.update(where_expression != nullptr);
    if (where_expression)
        where_expression->updateTreeHash(hash_state, ignore_aliases);
    hash_state.update(limit_length != nullptr);
    if (limit_length)
        limit_length->updateTreeHash(hash_state, ignore_aliases);
}

void ASTShowColumnsQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr
                  << "SHOW "
                  << (extended ? "EXTENDED " : "")
                  << (full ? "FULL " : "")
                  << "COLUMNS"
                 ;

    ostr << " FROM " << backQuoteIfNeed(table);
    if (!database.empty())
        ostr << " FROM " << backQuoteIfNeed(database);


    /// Emit the clause whenever a `LIKE` was present, even with an empty pattern: `NOT LIKE ''` /
    /// `ILIKE ''` set `not_like` / `case_insensitive_like` while leaving `like` empty, and dropping
    /// the clause would lose those flags on a format -> parse round-trip (which the rewrite-rule
    /// matcher's tree-hash consistency check relies on).
    if (!like.empty() || not_like || case_insensitive_like)
    {
        ostr

            << (not_like ? " NOT" : "")
            << (case_insensitive_like ? " ILIKE " : " LIKE ")
            << quoteString(like);
    }

    if (where_expression)
    {
        ostr << " WHERE ";
        where_expression->format(ostr, settings, state, frame);
    }

    if (limit_length)
    {
        ostr << " LIMIT ";
        limit_length->format(ostr, settings, state, frame);
    }
}

}
