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

void ASTDropQuery::formatQueryImpl(const FormattingBuffer & out) const
{
    if (kind == ASTDropQuery::Kind::Drop)
        out.writeKeyword("DROP ");
    else if (kind == ASTDropQuery::Kind::Detach)
        out.writeKeyword("DETACH ");
    else if (kind == ASTDropQuery::Kind::Truncate)
        out.writeKeyword("TRUNCATE ");
    else
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Not supported kind of drop query.");

    if (temporary)
        out.writeKeyword("TEMPORARY ");


    if (!table && database)
        out.writeKeyword("DATABASE ");
    else if (is_dictionary)
        out.writeKeyword("DICTIONARY ");
    else if (is_view)
        out.writeKeyword("VIEW ");
    else
        out.writeKeyword("TABLE ");

    if (if_exists)
        out.writeKeyword("IF EXISTS ");

    if (!table && database)
        out.ostr << backQuoteIfNeed(getDatabase());
    else
        out.ostr << (database ? backQuoteIfNeed(getDatabase()) + "." : "") << backQuoteIfNeed(getTable());

    formatOnCluster(out);

    if (permanently)
        out.ostr << " PERMANENTLY";

    if (sync)
        out.writeKeyword(" SYNC");
}

}
