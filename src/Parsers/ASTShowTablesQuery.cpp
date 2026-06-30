#include <iomanip>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTShowTablesQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Common/SipHash.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTShowTablesQuery::clone() const
{
    auto res = make_intrusive<ASTShowTablesQuery>(*this);
    res->children.clear();
    if (from)
        res->set(res->from, from->clone());

    cloneOutputOptions(*res);
    return res;
}

void ASTShowTablesQuery::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
    /// Fold in the semantic fields that are not part of `children` (the base implementation only
    /// hashes `getID`, and `from` is hashed through `children`). Two `SHOW` queries that differ
    /// only in these fields must not share a tree hash — see the header comment.
    hash_state.update(databases);
    hash_state.update(clusters);
    hash_state.update(cluster);
    hash_state.update(dictionaries);
    hash_state.update(m_settings);
    hash_state.update(merges);
    hash_state.update(changed);
    hash_state.update(temporary);
    hash_state.update(caches);
    hash_state.update(full);
    hash_state.update(cluster_str);
    hash_state.update(like);
    hash_state.update(not_like);
    hash_state.update(case_insensitive_like);
    hash_state.update(where_expression != nullptr);
    if (where_expression)
        where_expression->updateTreeHash(hash_state, ignore_aliases);
    hash_state.update(limit_length != nullptr);
    if (limit_length)
        limit_length->updateTreeHash(hash_state, ignore_aliases);
}

String ASTShowTablesQuery::getFrom() const
{
    String name;
    tryGetIdentifierNameInto(from, name);
    return name;
}

void ASTShowTablesQuery::formatLike(WriteBuffer & ostr, const FormatSettings &) const
{
    if (!like.empty())
    {
        ostr << (not_like ? " NOT" : "")
            << (case_insensitive_like ? " ILIKE " : " LIKE ")
            << quoteString(like);
    }
}

void ASTShowTablesQuery::formatLimit(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (limit_length)
    {
        ostr << " LIMIT ";
        limit_length->format(ostr, settings, state, frame);
    }
}

void ASTShowTablesQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (databases)
    {
        ostr << "SHOW DATABASES";
        formatLike(ostr, settings);
        formatLimit(ostr, settings, state, frame);

    }
    else if (clusters)
    {
        ostr << "SHOW CLUSTERS";
        formatLike(ostr, settings);
        formatLimit(ostr, settings, state, frame);

    }
    else if (cluster)
    {
        ostr << "SHOW CLUSTER";
        ostr << " " << backQuoteIfNeed(cluster_str);
    }
    else if (caches)
    {
        ostr << "SHOW FILESYSTEM CACHES";
        formatLike(ostr, settings);
        formatLimit(ostr, settings, state, frame);
    }
    else if (m_settings)
    {
        ostr << "SHOW " << (changed ? "CHANGED " : "") << "SETTINGS";
        formatLike(ostr, settings);
    }
    else if (merges)
    {
        ostr << "SHOW MERGES";
        formatLike(ostr, settings);
        formatLimit(ostr, settings, state, frame);
    }
    else
    {
        ostr << "SHOW " << (temporary ? "TEMPORARY " : "") <<
             (dictionaries ? "DICTIONARIES" : "TABLES");

        if (from)
        {
            ostr << " FROM ";
            from->format(ostr, settings, state, frame);
        }

        formatLike(ostr, settings);

        if (where_expression)
        {
            ostr << " WHERE ";
            where_expression->format(ostr, settings, state, frame);
        }

        formatLimit(ostr, settings, state, frame);
    }
}

}
