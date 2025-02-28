#include <filesystem>
#include <Access/Common/AccessRightsElement.h>
#include <Access/ContextAccess.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/DatabaseReplicated.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/DDLOnClusterQueryStatusSource.h>
#include <Interpreters/DDLTask.h>
#include <Interpreters/DDLWorker.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTSystemQuery.h>
#include <Parsers/queryToString.h>
#include <Processors/Sinks/EmptySink.h>
#include <QueryPipeline/Pipe.h>
#include <base/sort.h>
#include <Common/Macros.h>
#include <Common/ZooKeeper/ZooKeeper.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_distributed_ddl;
    extern const SettingsBool database_replicated_enforce_synchronous_settings;
    extern const SettingsDistributedDDLOutputMode distributed_ddl_output_mode;
    extern const SettingsInt64 distributed_ddl_task_timeout;
    extern const SettingsBool throw_on_unsupported_query_inside_transaction;
}

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int QUERY_IS_PROHIBITED;
extern const int LOGICAL_ERROR;
}


bool isSupportedAlterTypeForOnClusterDDLQuery(int type)
{
    assert(type != ASTAlterCommand::NO_TYPE);
    static const std::unordered_set<int> unsupported_alter_types{
        /// It's dangerous, because it may duplicate data if executed on multiple replicas. We can allow it after #18978
        ASTAlterCommand::ATTACH_PARTITION,
        /// Usually followed by ATTACH PARTITION
        ASTAlterCommand::FETCH_PARTITION,
        /// Logical error
        ASTAlterCommand::NO_TYPE,
    };

    return !unsupported_alter_types.contains(type);
}


BlockIO executeDDLQueryOnCluster(const ASTPtr & query_ptr_, ContextPtr context, const DDLQueryOnClusterParams & params)
{
    OpenTelemetry::SpanHolder span(__FUNCTION__, OpenTelemetry::SpanKind::PRODUCER);

    if (context->getCurrentTransaction() && context->getSettingsRef()[Setting::throw_on_unsupported_query_inside_transaction])
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ON CLUSTER queries inside transactions are not supported");

    /// Remove FORMAT <fmt> and INTO OUTFILE <file> if exists
    ASTPtr query_ptr = query_ptr_->clone();
    ASTQueryWithOutput::resetOutputASTIfExist(*query_ptr);

    // XXX: serious design flaw since `ASTQueryWithOnCluster` is not inherited from `IAST`!
    auto * query = dynamic_cast<ASTQueryWithOnCluster *>(query_ptr.get());
    if (!query)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Distributed execution is not supported for such DDL queries");
    }

    if (!context->getSettingsRef()[Setting::allow_distributed_ddl])
        throw Exception(ErrorCodes::QUERY_IS_PROHIBITED, "Distributed DDL queries are prohibited for the user");

    bool is_system_query = dynamic_cast<ASTSystemQuery *>(query_ptr.get()) != nullptr;
    bool replicated_ddl_queries_enabled = DatabaseCatalog::instance().canPerformReplicatedDDLQueries();

    if (!is_system_query && !replicated_ddl_queries_enabled)
        throw Exception(ErrorCodes::QUERY_IS_PROHIBITED, "Replicated DDL queries are disabled");

    if (const auto * query_alter = query_ptr->as<ASTAlterQuery>())
    {
        for (const auto & command : query_alter->command_list->children)
        {
            if (!isSupportedAlterTypeForOnClusterDDLQuery(command->as<ASTAlterCommand&>().type))
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported type of ALTER query");
        }
    }

    ClusterPtr cluster = params.cluster;
    if (!cluster)
    {
        query->cluster = context->getMacros()->expand(query->cluster);
        cluster = context->getCluster(query->cluster);
    }

    span.addAttribute("clickhouse.cluster", query->cluster);

    if (!cluster->areDistributedDDLQueriesAllowed())
        throw Exception(ErrorCodes::QUERY_IS_PROHIBITED, "Distributed DDL queries are prohibited for the cluster");

    /// TODO: support per-cluster grant
    context->checkAccess(AccessType::CLUSTER);

    /// NOTE: if `async_load_databases = true`, then it block until ddl_worker is started, which includes startup of all related tables.
    DDLWorker & ddl_worker = context->getDDLWorker();

    /// Enumerate hosts which will be used to send query.
    auto addresses = cluster->filterAddressesByShardOrReplica(params.only_shard_num, params.only_replica_num);
    if (addresses.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No hosts defined to execute distributed DDL query");

    std::vector<HostID> hosts;
    hosts.reserve(addresses.size());
    for (const auto * address : addresses)
        hosts.emplace_back(*address);

    /// The current database in a distributed query need to be replaced with either
    /// the local current database or a shard's default database.
    AccessRightsElements access_to_check = params.access_to_check;
    bool need_replace_current_database = std::any_of(
        access_to_check.begin(),
        access_to_check.end(),
        [](const AccessRightsElement & elem) { return elem.isEmptyDatabase(); });

    bool use_local_default_database = false;
    const String & current_database = context->getCurrentDatabase();

    if (need_replace_current_database)
    {
        Strings host_default_databases;
        for (const auto * address : addresses)
        {
            if (!address->default_database.empty())
                host_default_databases.push_back(address->default_database);
            else
                use_local_default_database = true;
        }
        ::sort(host_default_databases.begin(), host_default_databases.end());
        host_default_databases.erase(std::unique(host_default_databases.begin(), host_default_databases.end()), host_default_databases.end());
        assert(use_local_default_database || !host_default_databases.empty());

        if (use_local_default_database && !host_default_databases.empty())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Mixed local default DB and shard default DB in DDL query");

        if (use_local_default_database)
        {
            access_to_check.replaceEmptyDatabase(current_database);
        }
        else
        {
            for (size_t i = 0; i != access_to_check.size();)
            {
                auto & element = access_to_check[i];
                if (element.isEmptyDatabase())
                {
                    access_to_check.insert(access_to_check.begin() + i + 1, host_default_databases.size() - 1, element);
                    for (size_t j = 0; j != host_default_databases.size(); ++j)
                        access_to_check[i + j].replaceEmptyDatabase(host_default_databases[j]);
                    i += host_default_databases.size();
                }
                else
                    ++i;
            }
        }
    }

    AddDefaultDatabaseVisitor visitor(context, current_database, !use_local_default_database);
    visitor.visitDDL(query_ptr);

    /// Check access rights, assume that all servers have the same users config
    context->checkAccess(access_to_check);

    DDLLogEntry entry;
    entry.hosts = std::move(hosts);
    entry.query = queryToString(query_ptr);
    entry.initiator = ddl_worker.getCommonHostID();
    entry.setSettingsIfRequired(context);
    entry.tracing_context = OpenTelemetry::CurrentContext();
    entry.initial_query_id = context->getClientInfo().initial_query_id;
    String node_path = ddl_worker.enqueueQuery(entry, params.retries_info, context->getProcessListElement());

    return getDDLOnClusterStatus(node_path, ddl_worker.getReplicasDir(), entry, context);
}

BlockIO getDDLOnClusterStatus(const String & node_path, const String & replicas_path, const DDLLogEntry & entry, ContextPtr context)
{
    BlockIO io;
    if (context->getSettingsRef()[Setting::distributed_ddl_task_timeout] == 0)
        return io;
    Strings hosts_to_wait;
    for (const HostID & host : entry.hosts)
        hosts_to_wait.push_back(host.toString());

    auto source = std::make_shared<DDLOnClusterQueryStatusSource>(node_path, replicas_path, context, hosts_to_wait);
    io.pipeline = QueryPipeline(std::move(source));

    if (context->getSettingsRef()[Setting::distributed_ddl_output_mode] == DistributedDDLOutputMode::NONE
        || context->getSettingsRef()[Setting::distributed_ddl_output_mode] == DistributedDDLOutputMode::NONE_ONLY_ACTIVE)
        io.pipeline.complete(std::make_shared<EmptySink>(io.pipeline.getHeader()));

    return io;
}

bool maybeRemoveOnCluster(const ASTPtr & query_ptr, ContextPtr context)
{
    const auto * query = dynamic_cast<const ASTQueryWithTableAndOutput *>(query_ptr.get());
    if (!query || !query->table)
        return false;

    String database_name = query->getDatabase();
    if (database_name.empty())
        database_name = context->getCurrentDatabase();

    auto * query_on_cluster = dynamic_cast<ASTQueryWithOnCluster *>(query_ptr.get());
    if (database_name != query_on_cluster->cluster)
        return false;

    auto database = DatabaseCatalog::instance().tryGetDatabase(database_name);
    if (database && database->shouldReplicateQuery(context, query_ptr))
    {
        /// It's Replicated database and query is replicated on database level,
        /// so ON CLUSTER clause is redundant.
        query_on_cluster->cluster.clear();
        return true;
    }

    return false;
}

}
