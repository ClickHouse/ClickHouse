#include <Parsers/ASTShowTableSettingsQuery.h>

#include <IO/Operators.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <Common/SipHash.h>
#include <Common/quoteString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

ASTPtr ASTShowTableSettingsQuery::clone() const
{
    auto res = make_intrusive<ASTShowTableSettingsQuery>(*this);
    res->children.clear();
    cloneOutputOptions(*res);
    return res;
}

void ASTShowTableSettingsQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings &, FormatState &, FormatStateStacked) const
{
    ostr << "SHOW ";
    if (changed)
        ostr << "CHANGED ";
    ostr << "TABLE SETTINGS FROM ";
    /// Only when the query named one: printing an empty database would render `FROM ``.`t``, which
    /// does not parse back - and a debug build checks exactly that round trip.
    if (!database.empty())
        ostr << backQuoteIfNeed(database) << ".";
    ostr << backQuoteIfNeed(table);

    if (has_like)
    {
        ostr << (not_like ? " NOT" : "") << (case_insensitive_like ? " ILIKE " : " LIKE ")
             << quoteString(like);
    }
}

void ASTShowTableSettingsQuery::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    /// Every field is a member rather than a child, so the base implementation - which hashes only
    /// `getID()` - would give the same hash to every `SHOW TABLE SETTINGS` query. Nothing keys off
    /// this today (the query result cache takes only `SELECT`), which is exactly why it would be
    /// missed by whoever adds the first consumer.
    hash_state.update(changed);
    hash_state.update(has_like);
    hash_state.update(not_like);
    hash_state.update(case_insensitive_like);

    const auto update_string = [&hash_state](const String & value)
    {
        hash_state.update(value.size());
        hash_state.update(value);
    };

    update_string(database);
    update_string(table);
    update_string(like);
    ASTQueryWithOutput::updateTreeHashImpl(hash_state, ignore_aliases);
}

void ASTShowTableSettingsQuery::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "ShowTableSettingsQuery");
    if (changed)
        w.writeBool("changed", true);
    if (!database.empty())
        w.writeString("database", database);
    w.writeString("table", table);
    if (has_like)
        w.writeString("like", like);
    if (not_like)
        w.writeBool("not_like", true);
    if (case_insensitive_like)
        w.writeBool("case_insensitive_like", true);
    writeOutputOptionsJSON(w);
}

void ASTShowTableSettingsQuery::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    changed = r.getBool("changed");
    database = r.getString("database");
    table = r.getString("table");
    if (table.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "SHOW TABLE SETTINGS requires a non-empty 'table' field during AST JSON deserialization");
    like = r.getString("like");
    has_like = r.has("like");
    not_like = r.getBool("not_like");
    case_insensitive_like = r.getBool("case_insensitive_like");

    /// `ParserShowTableSettingsQuery` reads `NOT` and `ILIKE` only as part of a LIKE clause, so
    /// neither flag can stand without a pattern - and `formatQueryImpl` would silently drop them.
    if (!has_like && (not_like || case_insensitive_like))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "'not_like' and 'case_insensitive_like' require a non-empty 'like' during AST JSON deserialization");

    readOutputOptionsJSON(r);
}

}
