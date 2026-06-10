#include <Interpreters/removeOnClusterClauseIfNeeded.h>

#include <Access/AccessControl.h>
#include <Access/ReplicatedAccessStorage.h>
#include <Common/logger_useful.h>
#include <Core/Settings.h>
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
#include <Parsers/ASTCreateNamedCollectionQuery.h>
#include <Parsers/ASTAlterNamedCollectionQuery.h>
#include <Parsers/ASTDropNamedCollectionQuery.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool ignore_on_cluster_for_replicated_named_collections_queries;
    extern const SettingsBool ignore_on_cluster_for_replicated_access_entities_queries;
    extern const SettingsBool ignore_on_cluster_for_replicated_udf_queries;
}

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

static bool isNamedCollectionQuery(const ASTPtr & query)
{
    return query->as<ASTCreateNamedCollectionQuery>()
        || query->as<ASTDropNamedCollectionQuery>()
        || query->as<ASTAlterNamedCollectionQuery>();
}

ASTPtr removeOnClusterClauseIfNeeded(const ASTPtr & query, ContextPtr context, const WithoutOnClusterASTRewriteParams & params)
{
    auto * query_on_cluster = dynamic_cast<ASTQueryWithOnCluster *>(query.get());

    if (!query_on_cluster || query_on_cluster->cluster.empty())
        return query;

    if ((isUserDefinedFunctionQuery(query)
         && context->getSettingsRef()[Setting::ignore_on_cluster_for_replicated_udf_queries]
         && context->getUserDefinedSQLObjectsStorage().isReplicated())
        || (isAccessControlQuery(query)
            && context->getSettingsRef()[Setting::ignore_on_cluster_for_replicated_access_entities_queries]
            && context->getAccessControl().containsStorage(ReplicatedAccessStorage::STORAGE_TYPE))
        || (isNamedCollectionQuery(query)
            && context->getSettingsRef()[Setting::ignore_on_cluster_for_replicated_named_collections_queries]
            && NamedCollectionFactory::instance().usesReplicatedStorage()))
    {
        LOG_DEBUG(getLogger("removeOnClusterClauseIfNeeded"), "ON CLUSTER clause was ignored for query {}", query->getID());
        return query_on_cluster->getRewrittenASTWithoutOnCluster(params);
    }

    return query;
}
}
