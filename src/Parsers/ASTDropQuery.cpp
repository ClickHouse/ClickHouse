#include <Parsers/ASTDropQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}


String ASTDropQuery::getID(char delim) const
{
    if (kind == ASTDropQuery::Kind::Drop)
        return "DropQuery" + (delim + getDatabase()) + delim + getTable();
    else if (kind == ASTDropQuery::Kind::Detach)
        return "DetachQuery" + (delim + getDatabase()) + delim + getTable();
    else if (kind == ASTDropQuery::Kind::Truncate)
        return "TruncateQuery" + (delim + getDatabase()) + delim + getTable();
    else
        throw Exception("Not supported kind of drop query.", ErrorCodes::SYNTAX_ERROR);
}

ASTPtr ASTDropQuery::clone() const
{
    auto res = std::make_shared<ASTDropQuery>(*this);
    cloneOutputOptions(*res);
    cloneTableOptions(*res);
    return res;
}

void ASTDropQuery::formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "");
    if (kind == ASTDropQuery::Kind::Drop)
        settings.ostr << "DROP ";
    else if (kind == ASTDropQuery::Kind::Detach)
        settings.ostr << "DETACH ";
    else if (kind == ASTDropQuery::Kind::Truncate)
        settings.ostr << "TRUNCATE ";
    else
        throw Exception("Not supported kind of drop query.", ErrorCodes::SYNTAX_ERROR);

    if (temporary)
        settings.ostr << "TEMPORARY ";


    if (!table && database)
        settings.ostr << "DATABASE ";
    else if (is_dictionary)
        settings.ostr << "DICTIONARY ";
    else if (is_view)
        settings.ostr << "VIEW ";
    else
        settings.ostr << "TABLE ";

    if (if_exists)
        settings.ostr << "IF EXISTS ";

    settings.ostr << (settings.hilite ? hilite_none : "");

    if (!table && database)
        settings.ostr << backQuoteIfNeed(getDatabase());
    else
        settings.ostr << (database ? backQuoteIfNeed(getDatabase()) + "." : "") << backQuoteIfNeed(getTable());

    formatOnCluster(settings);

    if (permanently)
        settings.ostr << " PERMANENTLY";

    if (no_delay)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " NO DELAY" << (settings.hilite ? hilite_none : "");
}

}
