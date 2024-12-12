#include <Parsers/ASTShowColumnsQuery.h>

#include <iomanip>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTShowColumnsQuery::clone() const
{
    auto res = std::make_shared<ASTShowColumnsQuery>(*this);
    res->children.clear();
    cloneOutputOptions(*res);
    return res;
}

void ASTShowColumnsQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << (settings.hilite ? hilite_keyword : "")
                  << "SHOW "
                  << (extended ? "EXTENDED " : "")
                  << (full ? "FULL " : "")
                  << "COLUMNS"
                  << (settings.hilite ? hilite_none : "");

    ostr << (settings.hilite ? hilite_keyword : "") << " FROM " << (settings.hilite ? hilite_none : "") << backQuoteIfNeed(table);
    if (!database.empty())
        ostr << (settings.hilite ? hilite_keyword : "") << " FROM " << (settings.hilite ? hilite_none : "") << backQuoteIfNeed(database);


    if (!like.empty())
        ostr << (settings.hilite ? hilite_keyword : "")
                      << (not_like ? " NOT" : "")
                      << (case_insensitive_like ? " ILIKE " : " LIKE")
                      << (settings.hilite ? hilite_none : "")
                      << DB::quote << like;

    if (where_expression)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << " WHERE " << (settings.hilite ? hilite_none : "");
        where_expression->formatImpl(ostr, settings, state, frame);
    }

    if (limit_length)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << " LIMIT " << (settings.hilite ? hilite_none : "");
        limit_length->formatImpl(ostr, settings, state, frame);
    }
}

}
