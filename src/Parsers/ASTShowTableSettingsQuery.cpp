#include <Parsers/ASTShowTableSettingsQuery.h>

#include <IO/Operators.h>
#include <Common/quoteString.h>

namespace DB
{

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

}
