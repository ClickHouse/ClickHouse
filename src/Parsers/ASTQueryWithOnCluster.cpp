#include <Parsers/ASTQueryWithOnCluster.h>

#include <Core/Settings.h>
#include <Databases/DatabaseReplicated.h>
#include <IO/Operators.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace Setting
{
    extern const SettingsBool ignore_on_cluster_for_replicated_database_queries;
}


std::string ASTQueryWithOnCluster::getRewrittenQueryWithoutOnCluster(const WithoutOnClusterASTRewriteParams & params) const
{
    return getRewrittenASTWithoutOnCluster(params)->formatWithSecretsOneLine();
}


bool ASTQueryWithOnCluster::parse(Pos & pos, std::string & cluster_str, Expected & expected)
{
    if (!ParserKeyword(Keyword::CLUSTER).ignore(pos, expected))
        return false;

    return parseIdentifierOrStringLiteral(pos, expected, cluster_str);
}


void ASTQueryWithOnCluster::formatOnCluster(WriteBuffer & ostr, const IAST::FormatSettings &) const
{
    if (!cluster.empty())
    {
        ostr << " ON CLUSTER " << backQuoteIfNeed(cluster);
    }
}

bool ASTQueryWithOnCluster::isIgnoreOnCluster(const ASTPtr & query, const ContextPtr & context) const
{
    // String database_name = query->getDatabase();
    // if (database_name.empty())
    //     database_name = context->getCurrentDatabase();
    const auto database = DatabaseCatalog::instance().getDatabase(context->getCurrentDatabase());
    chassert(database);
    const auto * replicated = dynamic_cast<const DatabaseReplicated *>(database.get());
    
    bool setting = context->getSettingsRef()[Setting::ignore_on_cluster_for_replicated_database_queries];
    LOG_DEBUG(getLogger("testLogger"), "isIgnoreOnCluster ignore_on_cluster_for_replicated_database_queries={}", setting);

    if (replicated 
        && context->getSettingsRef()[Setting::ignore_on_cluster_for_replicated_database_queries])
    {
        LOG_DEBUG(
            getLogger("IgnoreOnClusterClauseReplicatedDatabase"),
            "ON CLUSTER clause was ignored for query {} because database {} is Replicated and setting "
            "`ignore_on_cluster_for_replicated_database_queries` is on.",
            query->getID(),
            replicated->getDatabaseName());
        return true;
    }

    return false;
}


}
