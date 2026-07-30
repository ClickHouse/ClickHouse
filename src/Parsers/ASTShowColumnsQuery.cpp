#include <Parsers/ASTShowColumnsQuery.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <Parsers/ASTLiteral.h>

#include <iomanip>
#include <Common/SipHash.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

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

void ASTShowColumnsQuery::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "ShowColumnsQuery");
    if (extended)
        w.writeBool("extended", true);
    if (full)
        w.writeBool("full", true);
    if (!database.empty())
        w.writeString("database", database);
    w.writeString("table", table);
    if (!like.empty())
        w.writeString("like", like);
    if (not_like)
        w.writeBool("not_like", true);
    if (case_insensitive_like)
        w.writeBool("case_insensitive_like", true);
    w.writeChild("where_expression", where_expression);
    w.writeChild("limit_length", limit_length);
    writeOutputOptionsJSON(w);
}

void ASTShowColumnsQuery::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    extended = r.getBool("extended");
    full = r.getBool("full");
    database = r.getString("database");
    table = r.getString("table");
    if (table.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "SHOW COLUMNS requires a non-empty 'table' field during AST JSON deserialization");
    like = r.getString("like");
    not_like = r.getBool("not_like");
    case_insensitive_like = r.getBool("case_insensitive_like");
    where_expression = r.readChild("where_expression");
    if (where_expression)
        children.push_back(where_expression);
    limit_length = r.readChild("limit_length");
    if (limit_length)
        children.push_back(limit_length);

    /// `ParserShowColumnsQuery` consumes `NOT` and `ILIKE` only as part of a LIKE clause, so these
    /// flags cannot exist without a pattern; `formatQueryImpl` silently drops them when 'like' is empty.
    if (like.empty() && (not_like || case_insensitive_like))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "'not_like' and 'case_insensitive_like' require a non-empty 'like' during AST JSON deserialization");

    /// The parser accepts either a LIKE clause or a WHERE clause, never both, and
    /// `InterpreterShowColumnsQuery` ignores 'where_expression' whenever 'like' is set, so the
    /// formatted SQL and the executed query would diverge.
    if (where_expression && !like.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "'like' and 'where_expression' are mutually exclusive in `ShowColumnsQuery` "
            "during AST JSON deserialization");

    readOutputOptionsJSON(r);
}

}
