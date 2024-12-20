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

void ASTDropQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "");
    if (kind == ASTDropQuery::Kind::Drop)
        settings.ostr << "DROP ";
    else if (kind == ASTDropQuery::Kind::Detach)
        settings.ostr << "DETACH ";
    else if (kind == ASTDropQuery::Kind::Truncate)
        settings.ostr << "TRUNCATE ";
    else
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Not supported kind of drop query.");

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

    if (if_empty)
        settings.ostr << "IF EMPTY ";

    settings.ostr << (settings.hilite ? hilite_none : "");

    if (!table && database)
    {
        database->formatImpl(settings, state, frame);
    }
    else
    {
        if (database)
        {
            database->formatImpl(settings, state, frame);
            settings.ostr << '.';
        }

        chassert(table);
        table->formatImpl(settings, state, frame);
    }

    formatOnCluster(settings);

    if (permanently)
        settings.ostr << " PERMANENTLY";

    if (sync)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " SYNC" << (settings.hilite ? hilite_none : "");
}

}
