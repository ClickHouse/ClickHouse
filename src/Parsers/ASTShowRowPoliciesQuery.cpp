#include <Parsers/ASTShowRowPoliciesQuery.h>
#include <Common/quoteString.h>


namespace DB
{
void ASTShowRowPoliciesQuery::formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "SHOW POLICIES" << (settings.hilite ? hilite_none : "");

    if (current)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " CURRENT" << (settings.hilite ? hilite_none : "");

    if (!table_name.empty())
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " ON " << (settings.hilite ? hilite_none : "");
        if (!database.empty())
            settings.ostr << backQuoteIfNeed(database) << ".";
        settings.ostr << backQuoteIfNeed(table_name);
    }
}
}
