#include <Parsers/ASTShowIndexesQuery.h>

#include <iomanip>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTShowIndexesQuery::clone() const
{
    auto res = std::make_shared<ASTShowIndexesQuery>(*this);
    res->children.clear();
    cloneOutputOptions(*res);
    return res;
}

void ASTShowIndexesQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << (settings.hilite ? hilite_keyword : "")
                  << "SHOW "
                  << (extended ? "EXTENDED " : "")
                  << "INDEXES"
                  << (settings.hilite ? hilite_none : "");

    ostr << (settings.hilite ? hilite_keyword : "") << " FROM " << (settings.hilite ? hilite_none : "") << backQuoteIfNeed(table);
    if (!database.empty())
        ostr << (settings.hilite ? hilite_keyword : "") << " FROM " << (settings.hilite ? hilite_none : "") << backQuoteIfNeed(database);

    if (where_expression)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << " WHERE " << (settings.hilite ? hilite_none : "");
        where_expression->formatImpl(ostr, settings, state, frame);
    }
}

}

