#include <Parsers/ASTDropQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}


String ASTDropQuery::getID() const
{
    if (kind == ASTDropQuery::Kind::Drop)
        return "DropQuery_" + database + "_" + table;
    else if (kind == ASTDropQuery::Kind::Detach)
        return "DetachQuery_" + database + "_" + table;
    else if (kind == ASTDropQuery::Kind::Truncate)
        return "TruncateQuery_" + database + "_" + table;
    else
        throw Exception("Not supported kind of drop query.", ErrorCodes::SYNTAX_ERROR);
}

ASTPtr ASTDropQuery::clone() const
{
    auto res = std::make_shared<ASTDropQuery>(*this);
    cloneOutputOptions(*res);
    return res;
}

ASTPtr ASTDropQuery::getRewrittenASTWithoutOnCluster(const std::string & new_database) const
{
    auto query_ptr = clone();
    auto & query = static_cast<ASTDropQuery &>(*query_ptr);

    query.cluster.clear();
    if (query.database.empty())
        query.database = new_database;

    return query_ptr;
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

    settings.ostr << ((table.empty() && !database.empty()) ? "DATABASE " : "TABLE ");

    if (if_exists)
        settings.ostr << "IF EXISTS ";

    settings.ostr << (settings.hilite ? hilite_none : "");

    if (table.empty() && !database.empty())
        settings.ostr << backQuoteIfNeed(database);
    else
        settings.ostr << (!database.empty() ? backQuoteIfNeed(database) + "." : "") << backQuoteIfNeed(table);

    formatOnCluster(settings);
}

}

