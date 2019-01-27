#include <Parsers/ASTDropQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}


String ASTDropQuery::getID(char delim) const
{
    String suffix = delim + database;
    if (!table.empty())
        suffix += delim + table;

    if (!dictionary.empty())
        suffix = delim + dictionary;

    if (kind == ASTDropQuery::Kind::Drop)
        return "DropQuery" + suffix;
    else if (kind == ASTDropQuery::Kind::Detach)
        return "DetachQuery" + suffix;
    else if (kind == ASTDropQuery::Kind::Truncate)
        return "TruncateQuery" + suffix;
    else
        throw Exception("Not supported kind of drop query.", ErrorCodes::SYNTAX_ERROR);
}

ASTPtr ASTDropQuery::clone() const
{
    auto res = std::make_shared<ASTDropQuery>(*this);
    cloneOutputOptions(*res);
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

    if (!table.empty() && !dictionary.empty())
        throw Exception("Can't format table and dictionary together", ErrorCodes::LOGICAL_ERROR);

    if (!database.empty() && table.empty() && dictionary.empty())
        settings.ostr << "DATABASE ";
    else if (!table.empty())
        settings.ostr << "TABLE ";
    else
        settings.ostr << "DICTIONARY ";

    if (if_exists)
        settings.ostr << "IF EXISTS ";

    settings.ostr << (settings.hilite ? hilite_none : "");

    if (dictionary.empty() && table.empty() && !database.empty())
        settings.ostr << backQuoteIfNeed(database);
    else if (!table.empty())
        settings.ostr << (!database.empty() ? backQuoteIfNeed(database) + "." : "") << backQuoteIfNeed(table);
    else
        settings.ostr << (!database.empty() ? backQuoteIfNeed(database) + "." : "") << backQuoteIfNeed(dictionary);

    formatOnCluster(settings);
}

}

