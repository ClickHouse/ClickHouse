#include <Parsers/ASTUndropQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

String ASTUndropQuery::getID(char delim) const
{
    return "UndropQuery" + (delim + getDatabase()) + delim + getTable();
}

ASTPtr ASTUndropQuery::clone() const
{
    auto res = std::make_shared<ASTUndropQuery>(*this);
    cloneOutputOptions(*res);
    cloneTableOptions(*res);
    return res;
}

void ASTUndropQuery::formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "");
    settings.ostr << "UNDROP ";
    settings.ostr << "TABLE ";
    settings.ostr << (settings.hilite ? hilite_none : "");

    assert (table);
    if (!database)
        settings.ostr << backQuoteIfNeed(getTable());
    else
        settings.ostr << backQuoteIfNeed(getDatabase()) + "." << backQuoteIfNeed(getTable());

    if (uuid != UUIDHelpers::Nil)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " UUID " << (settings.hilite ? hilite_none : "")
            << quoteString(toString(uuid));

    formatOnCluster(settings);
}

}
