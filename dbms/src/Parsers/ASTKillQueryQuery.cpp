#include <Parsers/ASTKillQueryQuery.h>

namespace DB
{

String ASTKillQueryQuery::getID() const
{
    return "KillQueryQuery_" + (where_expression ? where_expression->getID() : "") + "_" + String(sync ? "SYNC" : "ASYNC");
}

ASTPtr ASTKillQueryQuery::getRewrittenASTWithoutOnCluster(const std::string & /*new_database*/) const
{
    auto query_ptr = clone();
    ASTKillQueryQuery & query = static_cast<ASTKillQueryQuery &>(*query_ptr);

    query.cluster.clear();

    return query_ptr;
}

void ASTKillQueryQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "KILL QUERY ";

    formatOnCluster(settings);
    settings.ostr << " WHERE " << (settings.hilite ? hilite_none : "");

    if (where_expression)
        where_expression->formatImpl(settings, state, frame);

    settings.ostr << " " << (settings.hilite ? hilite_keyword : "") << (test ? "TEST" : (sync ? "SYNC" : "ASYNC")) << (settings.hilite ? hilite_none : "");
}

}
