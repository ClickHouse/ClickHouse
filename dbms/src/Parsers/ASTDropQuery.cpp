#include <Parsers/ASTDropQuery.h>
#include <Common/quoteString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}


String ASTDropQuery::getID(char delim) const
{
    if (kind == ASTDropQuery::Kind::Drop)
        return "DropQuery" + (delim + getTableAndDatabaseID(delim));
    else if (kind == ASTDropQuery::Kind::Detach)
        return "DetachQuery" + (delim + getTableAndDatabaseID(delim));
    else if (kind == ASTDropQuery::Kind::Truncate)
        return "TruncateQuery" + (delim + getTableAndDatabaseID(delim));
    else
        throw Exception("Not supported kind of drop query.", ErrorCodes::SYNTAX_ERROR);
}

ASTPtr ASTDropQuery::clone() const
{
    auto res = std::make_shared<ASTDropQuery>(*this);
    cloneOutputOptions(*res);
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
        throw Exception("Not supported kind of drop query.", ErrorCodes::SYNTAX_ERROR);

    if (temporary)
        settings.ostr << "TEMPORARY ";

    if (onlyDatabase())
        settings.ostr << "DATABASE ";
    else if (!is_dictionary)
        settings.ostr << "TABLE ";
    else
        settings.ostr << "DICTIONARY ";

    if (if_exists)
        settings.ostr << "IF EXISTS ";

    settings.ostr << (settings.hilite ? hilite_none : "");

    formatTableAndDatabase(settings, state, frame);
    formatOnCluster(settings);
}

}
