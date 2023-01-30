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
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Not supported kind of drop query.");
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
    if (kind == ASTDropQuery::Kind::Drop)
        settings.writeKeyword("DROP ");
    else if (kind == ASTDropQuery::Kind::Detach)
        settings.writeKeyword("DETACH ");
    else if (kind == ASTDropQuery::Kind::Truncate)
        settings.writeKeyword("TRUNCATE ");
    else
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Not supported kind of drop query.");

    if (temporary)
        settings.writeKeyword("TEMPORARY ");


    if (!table && database)
        settings.writeKeyword("DATABASE ");
    else if (is_dictionary)
        settings.writeKeyword("DICTIONARY ");
    else if (is_view)
        settings.writeKeyword("VIEW ");
    else
        settings.writeKeyword("TABLE ");

    if (if_exists)
        settings.writeKeyword("IF EXISTS ");

    if (!table && database)
        settings.ostr << backQuoteIfNeed(getDatabase());
    else
        settings.ostr << (database ? backQuoteIfNeed(getDatabase()) + "." : "") << backQuoteIfNeed(getTable());

    formatOnCluster(settings);

    if (permanently)
        settings.ostr << " PERMANENTLY";

    if (sync)
        settings.writeKeyword(" SYNC");
}

}
