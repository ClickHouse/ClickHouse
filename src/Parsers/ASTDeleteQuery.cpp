#include <Parsers/ASTDeleteQuery.h>
#include <Common/quoteString.h>

namespace DB
{
String ASTDeleteQuery::getID(char delim) const
{
    return "DeleteQuery" + (delim + database) + delim + table;
}

ASTPtr ASTDeleteQuery::clone() const
{
    return std::make_shared<ASTDeleteQuery>(*this);
}

void ASTDeleteQuery::formatQueryImpl(const FormatSettings & settings, FormatState & fs, FormatStateStacked fss) const
{
    settings.ostr
        << (settings.hilite ? hilite_keyword : "")
        << "DELETE FROM "
        << (settings.hilite ? hilite_none : "")
        << (!database.empty() ? backQuoteIfNeed(database) + "." : "") << backQuoteIfNeed(table)
        << (settings.hilite ? hilite_keyword : "")
        << "WHERE ";

    predicate->formatImpl(settings, fs, fss);
}
}
