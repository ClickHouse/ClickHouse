#include <iomanip>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTShowTablesQuery.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <Parsers/ASTLiteral.h>
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
    like = r.getString("like");
    not_like = r.getBool("not_like");
    case_insensitive_like = r.getBool("case_insensitive_like");
    auto from_child = r.readChild("from");
    if (from_child)
        set(from, from_child);
    where_expression = r.readChild("where_expression");
    if (where_expression)
        children.push_back(where_expression);
    limit_length = r.readChild("limit_length");
    if (limit_length)
        children.push_back(limit_length);

    /// Restore output options inherited from `ASTQueryWithOutput` (see `writeJSON`).
    out_file = r.readChild("out_file");
    if (out_file)
        children.push_back(out_file);

    format_ast = r.readChild("format_ast");
    if (format_ast)
        children.push_back(format_ast);

    settings_ast = r.readChild("settings_ast");
    if (settings_ast)
        children.push_back(settings_ast);

    compression = r.readChild("compression");
    if (compression)
        children.push_back(compression);

    compression_level = r.readChild("compression_level");
    if (compression_level)
        children.push_back(compression_level);

    /// Restore output-option flags from `ASTQueryWithOutput` (see `writeJSON`).
    setIsOutfileAppend(r.getBool("is_outfile_append"));
    setIsOutfileTruncate(r.getBool("is_outfile_truncate"));
    setIsIntoOutfileWithStdout(r.getBool("is_into_outfile_with_stdout"));

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

    /// `SHOW CLUSTER` and `SHOW FILESYSTEM CACHES` accept neither a LIKE pattern nor a LIMIT.
    if ((cluster || caches) && (!like.empty() || not_like || case_insensitive_like))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "LIKE is not valid for `SHOW CLUSTER`/`SHOW FILESYSTEM CACHES` during AST JSON deserialization");

    /// `SHOW SETTINGS` accepts a LIKE pattern but no LIMIT (see `formatQueryImpl`).
    if ((cluster || caches || m_settings) && limit_length)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "LIMIT is not valid for `SHOW CLUSTER`/`SHOW FILESYSTEM CACHES`/`SHOW SETTINGS` "
            "during AST JSON deserialization");
}

}
