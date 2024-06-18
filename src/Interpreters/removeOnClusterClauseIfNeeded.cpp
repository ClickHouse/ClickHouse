#include <Interpreters/removeOnClusterClauseIfNeeded.h>

#include <Access/AccessControl.h>
#include <Access/ReplicatedAccessStorage.h>
#include <Common/logger_useful.h>
#include <Functions/UserDefined/IUserDefinedSQLObjectsStorage.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateFunctionQuery.h>
#include <Parsers/ASTDropFunctionQuery.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/Access/ASTCreateQuotaQuery.h>
#include <Parsers/Access/ASTCreateRoleQuery.h>
#include <Parsers/Access/ASTCreateRowPolicyQuery.h>
#include <Parsers/Access/ASTCreateSettingsProfileQuery.h>
#include <Parsers/Access/ASTCreateUserQuery.h>
#include <Parsers/Access/ASTDropAccessEntityQuery.h>
#include <Parsers/Access/ASTGrantQuery.h>


namespace DB
{


static bool isUserDefinedFunctionQuery(const ASTPtr & query)
{
    return query->as<ASTCreateFunctionQuery>()
        || query->as<ASTDropFunctionQuery>();
}

static bool isAccessControlQuery(const ASTPtr & query)
{
    return query->as<ASTCreateUserQuery>()
        || query->as<ASTCreateQuotaQuery>()
        || query->as<ASTCreateRoleQuery>()
        || query->as<ASTCreateRowPolicyQuery>()
        || query->as<ASTCreateSettingsProfileQuery>()
        || query->as<ASTDropAccessEntityQuery>()
        || query->as<ASTGrantQuery>();
}

ASTPtr removeOnClusterClauseIfNeeded(const ASTPtr & query, ContextPtr context, const WithoutOnClusterASTRewriteParams & params)
{
    auto * query_on_cluster = dynamic_cast<ASTQueryWithOnCluster *>(query.get());

    if (!query_on_cluster || query_on_cluster->cluster.empty())
        return query;

    if ((isUserDefinedFunctionQuery(query)
         && context->getSettings().ignore_on_cluster_for_replicated_udf_queries
         && context->getUserDefinedSQLObjectsStorage().isReplicated())
        || (isAccessControlQuery(query)
            && context->getSettings().ignore_on_cluster_for_replicated_access_entities_queries
            && context->getAccessControl().containsStorage(ReplicatedAccessStorage::STORAGE_TYPE)))
    {
        LOG_DEBUG(getLogger("removeOnClusterClauseIfNeeded"), "ON CLUSTER clause was ignored for query {}", query->getID());
        return query_on_cluster->getRewrittenASTWithoutOnCluster(params);
    }

    return query;
}
}
