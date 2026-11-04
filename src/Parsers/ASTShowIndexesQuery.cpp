#include <Parsers/ASTShowIndexesQuery.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>

#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

ASTPtr ASTShowIndexesQuery::clone() const
{
    auto res = make_intrusive<ASTShowIndexesQuery>(*this);
    res->children.clear();
    /// `where_expression` is not a child: the parser puts it into the member only. Do not leave it
    /// shared with the source.
    if (where_expression)
        res->where_expression = where_expression->clone();
    cloneOutputOptions(*res);
    return res;
}

void ASTShowIndexesQuery::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "ShowIndexesQuery");
    w.writeBool("extended", extended);
    w.writeString("database", database);
    w.writeString("table", table);
    w.writeChild("where_expression", where_expression);
    writeOutputOptionsJSON(w);
}

void ASTShowIndexesQuery::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    extended = r.getBool("extended");
    database = r.getString("database");
    table = r.getString("table");
    if (table.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "SHOW INDEXES requires a non-empty 'table' field during AST JSON deserialization");
    where_expression = r.readChild("where_expression");
    readOutputOptionsJSON(r);
}

void ASTShowIndexesQuery::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    /// `where_expression` is member-only in the parser and must be included explicitly.
    hash_state.update(extended);
    const auto update_string = [&hash_state](const String & value)
    {
        hash_state.update(value.size());
        hash_state.update(value);
    };

    update_string(database);
    update_string(table);
    hash_state.update(where_expression != nullptr);
    if (where_expression)
        where_expression->updateTreeHash(hash_state, ignore_aliases);
    ASTQueryWithOutput::updateTreeHashImpl(hash_state, ignore_aliases);
}

void ASTShowIndexesQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr
                  << "SHOW "
                  << (extended ? "EXTENDED " : "")
                  << "INDEXES"
                 ;

    ostr << " FROM " << backQuoteIfNeed(table);
    if (!database.empty())
        ostr << " FROM " << backQuoteIfNeed(database);

    if (where_expression)
    {
        ostr << " WHERE ";
        where_expression->format(ostr, settings, state, frame);
    }
}

}
