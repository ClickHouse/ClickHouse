#include <Parsers/ASTShowColumnsQuery.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <Parsers/ASTLiteral.h>

#include <iomanip>
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


    if (!like.empty())
    {
        ostr

            << (not_like ? " NOT" : "")
            << (case_insensitive_like ? " ILIKE " : " LIKE")
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
}

void ASTShowColumnsQuery::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    extended = r.getBool("extended");
    full = r.getBool("full");
    database = r.getString("database");
    table = r.getString("table");
    like = r.getString("like");
    not_like = r.getBool("not_like");
    case_insensitive_like = r.getBool("case_insensitive_like");
    where_expression = r.readChild("where_expression");
    if (where_expression)
        children.push_back(where_expression);
    limit_length = r.readChild("limit_length");
    if (limit_length)
        children.push_back(limit_length);
}

}
