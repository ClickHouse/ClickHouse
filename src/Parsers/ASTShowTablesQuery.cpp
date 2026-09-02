#include <iomanip>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTShowTablesQuery.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <Parsers/ASTLiteral.h>
#include <Common/SipHash.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

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
    ASTQueryWithOutput::updateTreeHashImpl(hash_state, ignore_aliases);
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
    hash_state.update(has_like);
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
    /// Emit the clause whenever a `LIKE` was present, even with an empty pattern: `SHOW TABLES
    /// LIKE ''` differs from plain `SHOW TABLES` (see the `has_like` comment in the header), and
    /// dropping the clause would lose it and the `not_like` / `case_insensitive_like` modifiers
    /// on a format -> parse round-trip.
    if (has_like)
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
    /// The `FULL` modifier is parsed for every `SHOW` variant (before the selector keyword), so it
    /// must be emitted here for every variant. Otherwise `SHOW FULL TABLES` formats as `SHOW TABLES`
    /// and re-parses with `full = false`, which both loses semantics (`full` changes the result
    /// columns) and breaks the format -> parse tree-hash round-trip the rewrite-rule matcher relies on.
    ostr << "SHOW " << (full ? "FULL " : "");

    if (databases)
    {
        ostr << "DATABASES";
        formatLike(ostr, settings);
        formatLimit(ostr, settings, state, frame);
    }
    else if (clusters)
    {
        ostr << "CLUSTERS";
        formatLike(ostr, settings);
        formatLimit(ostr, settings, state, frame);
    }
    else if (cluster)
    {
        ostr << "CLUSTER";
        ostr << " " << backQuoteIfNeed(cluster_str);
    }
    else if (caches)
    {
        ostr << "FILESYSTEM CACHES";
        formatLike(ostr, settings);
        formatLimit(ostr, settings, state, frame);
    }
    else if (m_settings)
    {
        ostr << (changed ? "CHANGED " : "") << "SETTINGS";
        formatLike(ostr, settings);
    }
    else if (merges)
    {
        ostr << "MERGES";
        formatLike(ostr, settings);
        formatLimit(ostr, settings, state, frame);
    }
    else
    {
        ostr << (temporary ? "TEMPORARY " : "") <<
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

void ASTShowTablesQuery::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "ShowTablesQuery");
    if (databases)
        w.writeBool("databases", true);
    if (clusters)
        w.writeBool("clusters", true);
    if (cluster)
        w.writeBool("cluster", true);
    if (dictionaries)
        w.writeBool("dictionaries", true);
    if (m_settings)
        w.writeBool("settings", true);
    if (merges)
        w.writeBool("merges", true);
    if (changed)
        w.writeBool("changed", true);
    if (temporary)
        w.writeBool("temporary", true);
    if (caches)
        w.writeBool("caches", true);
    if (full)
        w.writeBool("full", true);
    if (!cluster_str.empty())
        w.writeString("cluster_str", cluster_str);
    if (has_like)
        w.writeBool("has_like", true);
    if (!like.empty())
        w.writeString("like", like);
    if (not_like)
        w.writeBool("not_like", true);
    if (case_insensitive_like)
        w.writeBool("case_insensitive_like", true);
    w.writeChild("from", from);
    w.writeChild("where_expression", where_expression);
    w.writeChild("limit_length", limit_length);

    /// Output options inherited from `ASTQueryWithOutput`. `ASTShowTablesQuery` is a
    /// `ASTQueryWithOutput`, so the `INTO OUTFILE`, `FORMAT`, and `SETTINGS` suffixes are
    /// semantics-bearing children/flags formatted by the base class. Without persisting them,
    /// e.g. `SHOW TABLES FORMAT JSON` or `SHOW TABLES SETTINGS x=1` would round-trip as plain
    /// `SHOW TABLES`.
    w.writeChild("out_file", out_file);
    w.writeChild("format_ast", format_ast);
    w.writeChild("settings_ast", settings_ast);
    w.writeChild("compression", compression);
    w.writeChild("compression_level", compression_level);

    /// Output-option flags from `ASTQueryWithOutput`: without these, `INTO OUTFILE ... APPEND`,
    /// `INTO OUTFILE ... TRUNCATE`, and `INTO OUTFILE ... AND STDOUT` would be silently lost on round-trip.
    w.writeBool("is_outfile_append", isOutfileAppend());
    w.writeBool("is_outfile_truncate", isOutfileTruncate());
    w.writeBool("is_into_outfile_with_stdout", isIntoOutfileWithStdout());
}

void ASTShowTablesQuery::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    databases = r.getBool("databases");
    clusters = r.getBool("clusters");
    cluster = r.getBool("cluster");
    dictionaries = r.getBool("dictionaries");
    m_settings = r.getBool("settings");
    merges = r.getBool("merges");
    changed = r.getBool("changed");
    temporary = r.getBool("temporary");
    caches = r.getBool("caches");
    full = r.getBool("full");
    cluster_str = r.getString("cluster_str");
    has_like = r.getBool("has_like");
    like = r.getString("like");
    not_like = r.getBool("not_like");
    case_insensitive_like = r.getBool("case_insensitive_like");
    /// `from` is parser-produced as an `ASTIdentifier` (`ParserShowTablesQuery` uses
    /// `ParserIdentifier`). `getFrom` only extracts a name through `tryGetIdentifierNameInto`,
    /// so a non-identifier node from malformed `clickhouse_json` would format as
    /// `SHOW TABLES FROM <expr>` while execution resolves an empty/current database. Reject it.
    auto from_child = r.readChildOfType<ASTIdentifier>("from");
    if (from_child)
        set(from, from_child);
    where_expression = r.readChild("where_expression");
    if (where_expression)
        children.push_back(where_expression);
    limit_length = r.readChild("limit_length");
    if (limit_length)
        children.push_back(limit_length);

    /// Restore output options inherited from `ASTQueryWithOutput` through the shared helper so
    /// the validation of their interdependencies stays in one place instead of diverging from
    /// `ASTQueryWithOutput::readOutputOptionsJSON`.
    readOutputOptionsJSON(r);

    /// `ASTShowTablesQuery` represents several mutually exclusive query forms (`SHOW DATABASES`,
    /// `SHOW CLUSTERS`, `SHOW CLUSTER`, `SHOW FILESYSTEM CACHES`, `SHOW SETTINGS`, `SHOW MERGES`,
    /// or the default `SHOW [TEMPORARY] TABLES`/`DICTIONARIES` form), selected by `formatQueryImpl`
    /// via an `if`/`else if` chain. Each form accepts only a specific subset of modifiers. Since the
    /// flags are read independently above, a malformed `clickhouse_json` could set an inconsistent
    /// combination (e.g. `{"databases":true,"dictionaries":true}`) that would silently format as a
    /// different, valid-looking query instead of being rejected. Validate that the deserialized AST
    /// corresponds to exactly one form the parser could have produced.
    const size_t mode_count = static_cast<size_t>(databases) + clusters + cluster + caches + m_settings + merges;
    if (mode_count > 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "`ShowTablesQuery` has mutually exclusive modes, but more than one of "
            "'databases', 'clusters', 'cluster', 'caches', 'settings', 'merges' is set "
            "during AST JSON deserialization");

    /// `mode_count == 0` is the default table/dictionary form.
    const bool table_form = mode_count == 0;

    if (cluster && cluster_str.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "`SHOW CLUSTER` requires a non-empty 'cluster_str' during AST JSON deserialization");
    if (!cluster && !cluster_str.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "'cluster_str' is only valid for `SHOW CLUSTER` during AST JSON deserialization");

    if (changed && !m_settings)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "'changed' is only valid for `SHOW SETTINGS` during AST JSON deserialization");

    if ((temporary || dictionaries) && !table_form)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "'temporary' and 'dictionaries' are only valid for the `SHOW TABLES`/`SHOW DICTIONARIES` "
            "form during AST JSON deserialization");

    if (from && !table_form)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "'from' is only valid for the `SHOW TABLES`/`SHOW DICTIONARIES` form during AST JSON deserialization");

    if (where_expression && !table_form)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "'where_expression' is only valid for the `SHOW TABLES`/`SHOW DICTIONARIES` form "
            "during AST JSON deserialization");

    /// In every form, the parser sets the pattern and the `NOT` / `ILIKE` modifiers only as part
    /// of a LIKE clause, so they cannot exist without 'has_like'; `formatQueryImpl` silently drops
    /// them when 'has_like' is not set.
    if (!has_like && (!like.empty() || not_like || case_insensitive_like))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "'like', 'not_like' and 'case_insensitive_like' require 'has_like' during AST JSON deserialization");

    /// In the table/dictionary form, the parser accepts either a LIKE clause or a WHERE clause,
    /// never both, and `InterpreterShowTablesQuery` ignores 'where_expression' whenever 'like' is
    /// set, so the formatted SQL and the executed query would diverge.
    if (where_expression && has_like)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "'like' and 'where_expression' are mutually exclusive in `ShowTablesQuery` "
            "during AST JSON deserialization");

    /// `SHOW CLUSTER` and `SHOW FILESYSTEM CACHES` accept neither a LIKE pattern nor a LIMIT.
    if ((cluster || caches) && has_like)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "LIKE is not valid for `SHOW CLUSTER`/`SHOW FILESYSTEM CACHES` during AST JSON deserialization");

    /// `SHOW SETTINGS` accepts a LIKE pattern but no LIMIT (see `formatQueryImpl`).
    if ((cluster || caches || m_settings) && limit_length)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "LIMIT is not valid for `SHOW CLUSTER`/`SHOW FILESYSTEM CACHES`/`SHOW SETTINGS` "
            "during AST JSON deserialization");
}

}
