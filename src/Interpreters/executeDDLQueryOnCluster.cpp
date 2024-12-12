#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/DDLWorker.h>
#include <Interpreters/DDLTask.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/queryToString.h>
#include <Access/Common/AccessRightsElement.h>
#include <Access/ContextAccess.h>
#include <Core/Settings.h>
#include <Common/Macros.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Databases/DatabaseReplicated.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeEnum.h>
#include <Processors/Sinks/EmptySink.h>
#include <QueryPipeline/Pipe.h>
#include <filesystem>
#include <base/sort.h>


namespace fs = std::filesystem;

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
    extern const int TIMEOUT_EXCEEDED;
    extern const int UNFINISHED;
    extern const int QUERY_IS_PROHIBITED;
    extern const int LOGICAL_ERROR;
}

static ZooKeeperRetriesInfo getRetriesInfo()
{
    const auto & config_ref = Context::getGlobalContextInstance()->getConfigRef();
    return ZooKeeperRetriesInfo(
        config_ref.getInt("distributed_ddl_keeper_max_retries", 5),
        config_ref.getInt("distributed_ddl_keeper_initial_backoff_ms", 100),
        config_ref.getInt("distributed_ddl_keeper_max_backoff_ms", 5000));
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
    String node_path = ddl_worker.enqueueQuery(entry);

    return getDistributedDDLStatus(node_path, entry, context, /* hosts_to_wait */ nullptr);
}


class DDLQueryStatusSource final : public ISource
{
public:
    DDLQueryStatusSource(
        const String & zk_node_path, const DDLLogEntry & entry, ContextPtr context_, const Strings * hosts_to_wait);

    String getName() const override { return "DDLQueryStatus"; }
    Chunk generate() override;
    Status prepare() override;

private:
    static Block getSampleBlock(ContextPtr context_, bool hosts_to_wait);

    Strings getNewAndUpdate(const Strings & current_list_of_finished_hosts);

    std::pair<String, UInt16> parseHostAndPort(const String & host_id) const;

    Chunk generateChunkWithUnfinishedHosts() const;

    enum ReplicatedDatabaseQueryStatus
    {
        /// Query is (successfully) finished
        OK = 0,
        /// Query is not finished yet, but replica is currently executing it
        IN_PROGRESS = 1,
        /// Replica is not available or busy with previous queries. It will process query asynchronously
        QUEUED = 2,
    };

    String node_path;
    ContextPtr context;
    Stopwatch watch;
    LoggerPtr log;

    NameSet waiting_hosts;  /// hosts from task host list
    NameSet finished_hosts; /// finished hosts from host list
    NameSet ignoring_hosts; /// appeared hosts that are not in hosts list
    Strings current_active_hosts; /// Hosts that are currently executing the task
    NameSet offline_hosts;  /// Hosts that are not currently running
    size_t num_hosts_finished = 0;

    /// Save the first detected error and throw it at the end of execution
    std::unique_ptr<Exception> first_exception;

    Int64 timeout_seconds = 120;
    bool is_replicated_database = false;
    bool throw_on_timeout = true;
    bool throw_on_timeout_only_active = false;
    bool only_running_hosts = false;

    bool timeout_exceeded = false;
    bool stop_waiting_offline_hosts = false;
};


BlockIO getDistributedDDLStatus(const String & node_path, const DDLLogEntry & entry, ContextPtr context, const Strings * hosts_to_wait)
{
    BlockIO io;
    if (context->getSettingsRef()[Setting::distributed_ddl_task_timeout] == 0)
        return io;

    auto source = std::make_shared<DDLQueryStatusSource>(node_path, entry, context, hosts_to_wait);
    io.pipeline = QueryPipeline(std::move(source));

    if (context->getSettingsRef()[Setting::distributed_ddl_output_mode] == DistributedDDLOutputMode::NONE
        || context->getSettingsRef()[Setting::distributed_ddl_output_mode] == DistributedDDLOutputMode::NONE_ONLY_ACTIVE)
        io.pipeline.complete(std::make_shared<EmptySink>(io.pipeline.getHeader()));

    return io;
}

Block DDLQueryStatusSource::getSampleBlock(ContextPtr context_, bool hosts_to_wait)
{
    auto output_mode = context_->getSettingsRef()[Setting::distributed_ddl_output_mode];

    auto maybe_make_nullable = [&](const DataTypePtr & type) -> DataTypePtr
    {
        if (output_mode == DistributedDDLOutputMode::THROW ||
            output_mode == DistributedDDLOutputMode::NONE ||
            output_mode == DistributedDDLOutputMode::NONE_ONLY_ACTIVE)
            return type;
        return std::make_shared<DataTypeNullable>(type);
    };

    auto get_status_enum = []()
    {
        return std::make_shared<DataTypeEnum8>(
            DataTypeEnum8::Values
            {
                {"OK",              static_cast<Int8>(OK)},
                {"IN_PROGRESS",     static_cast<Int8>(IN_PROGRESS)},
                {"QUEUED",          static_cast<Int8>(QUEUED)},
            });
    };

    if (hosts_to_wait)
    {
        return Block{
            {std::make_shared<DataTypeString>(), "shard"},
            {std::make_shared<DataTypeString>(), "replica"},
            {get_status_enum(), "status"},
            {std::make_shared<DataTypeUInt64>(), "num_hosts_remaining"},
            {std::make_shared<DataTypeUInt64>(), "num_hosts_active"},
        };
    }
    else
    {
        return Block{
            {std::make_shared<DataTypeString>(), "host"},
            {std::make_shared<DataTypeUInt16>(), "port"},
            {maybe_make_nullable(std::make_shared<DataTypeInt64>()), "status"},
            {maybe_make_nullable(std::make_shared<DataTypeString>()), "error"},
            {std::make_shared<DataTypeUInt64>(), "num_hosts_remaining"},
            {std::make_shared<DataTypeUInt64>(), "num_hosts_active"},
        };
    }
}

DDLQueryStatusSource::DDLQueryStatusSource(
    const String & zk_node_path, const DDLLogEntry & entry, ContextPtr context_, const Strings * hosts_to_wait)
    : ISource(getSampleBlock(context_, static_cast<bool>(hosts_to_wait)))
    , node_path(zk_node_path)
    , context(context_)
    , watch(CLOCK_MONOTONIC_COARSE)
    , log(getLogger("DDLQueryStatusSource"))
{
    auto output_mode = context->getSettingsRef()[Setting::distributed_ddl_output_mode];
    throw_on_timeout = output_mode == DistributedDDLOutputMode::THROW || output_mode == DistributedDDLOutputMode::NONE;
    throw_on_timeout_only_active = output_mode == DistributedDDLOutputMode::THROW_ONLY_ACTIVE || output_mode == DistributedDDLOutputMode::NONE_ONLY_ACTIVE;

    if (hosts_to_wait)
    {
        waiting_hosts = NameSet(hosts_to_wait->begin(), hosts_to_wait->end());
        is_replicated_database = true;
        only_running_hosts = output_mode == DistributedDDLOutputMode::THROW_ONLY_ACTIVE ||
                             output_mode == DistributedDDLOutputMode::NULL_STATUS_ON_TIMEOUT_ONLY_ACTIVE ||
                             output_mode == DistributedDDLOutputMode::NONE_ONLY_ACTIVE;
    }
    else
    {
        for (const HostID & host : entry.hosts)
            waiting_hosts.emplace(host.toString());
    }

    addTotalRowsApprox(waiting_hosts.size());
    timeout_seconds = context->getSettingsRef()[Setting::distributed_ddl_task_timeout];
}

std::pair<String, UInt16> DDLQueryStatusSource::parseHostAndPort(const String & host_id) const
{
    String host = host_id;
    UInt16 port = 0;
    if (!is_replicated_database)
    {
        auto host_and_port = Cluster::Address::fromString(host_id);
        host = host_and_port.first;
        port = host_and_port.second;
    }
    return {host, port};
}

Chunk DDLQueryStatusSource::generateChunkWithUnfinishedHosts() const
{
    NameSet unfinished_hosts = waiting_hosts;
    for (const auto & host_id : finished_hosts)
        unfinished_hosts.erase(host_id);

    NameSet active_hosts_set = NameSet{current_active_hosts.begin(), current_active_hosts.end()};

    /// Query is not finished on the rest hosts, so fill the corresponding rows with NULLs.
    MutableColumns columns = output.getHeader().cloneEmptyColumns();
    for (const String & host_id : unfinished_hosts)
    {
        size_t num = 0;
        if (is_replicated_database)
        {
            auto [shard, replica] = DatabaseReplicated::parseFullReplicaName(host_id);
            columns[num++]->insert(shard);
            columns[num++]->insert(replica);
            if (active_hosts_set.contains(host_id))
                columns[num++]->insert(IN_PROGRESS);
            else
                columns[num++]->insert(QUEUED);
        }
        else
        {
            auto [host, port] = parseHostAndPort(host_id);
            columns[num++]->insert(host);
            columns[num++]->insert(port);
            columns[num++]->insert(Field{});
            columns[num++]->insert(Field{});
        }
        columns[num++]->insert(unfinished_hosts.size());
        columns[num++]->insert(current_active_hosts.size());
    }
    return Chunk(std::move(columns), unfinished_hosts.size());
}

static NameSet getOfflineHosts(const String & node_path, const NameSet & hosts_to_wait, const ZooKeeperPtr & zookeeper, LoggerPtr log)
{
    fs::path replicas_path;
    if (node_path.ends_with('/'))
        replicas_path = fs::path(node_path).parent_path().parent_path().parent_path() / "replicas";
    else
        replicas_path = fs::path(node_path).parent_path().parent_path() / "replicas";

    Strings paths;
    Strings hosts_array;
    for (const auto & host : hosts_to_wait)
    {
        hosts_array.push_back(host);
        paths.push_back(replicas_path / host / "active");
    }

    NameSet offline;
    auto res = zookeeper->tryGet(paths);
    for (size_t i = 0; i < res.size(); ++i)
        if (res[i].error == Coordination::Error::ZNONODE)
            offline.insert(hosts_array[i]);

    if (offline.size() == hosts_to_wait.size())
    {
        /// Avoid reporting that all hosts are offline
        LOG_WARNING(log, "Did not find active hosts, will wait for all {} hosts. This should not happen often", offline.size());
        return {};
    }

    return offline;
}

Chunk DDLQueryStatusSource::generate()
{
    bool all_hosts_finished = num_hosts_finished >= waiting_hosts.size();

    /// Seems like num_hosts_finished cannot be strictly greater than waiting_hosts.size()
    assert(num_hosts_finished <= waiting_hosts.size());

    if (all_hosts_finished || timeout_exceeded)
        return {};

    String node_to_wait = "finished";
    if (is_replicated_database && context->getSettingsRef()[Setting::database_replicated_enforce_synchronous_settings])
        node_to_wait = "synced";

    size_t try_number = 0;

    while (true)
    {
        if (isCancelled())
            return {};

        if (stop_waiting_offline_hosts || (timeout_seconds >= 0 && watch.elapsedSeconds() > timeout_seconds))
        {
            timeout_exceeded = true;

            size_t num_unfinished_hosts = waiting_hosts.size() - num_hosts_finished;
            size_t num_active_hosts = current_active_hosts.size();

            constexpr auto msg_format = "Distributed DDL task {} is not finished on {} of {} hosts "
                                        "({} of them are currently executing the task, {} are inactive). "
                                        "They are going to execute the query in background. Was waiting for {} seconds{}";

            if (throw_on_timeout || (throw_on_timeout_only_active && !stop_waiting_offline_hosts))
            {
                if (!first_exception)
                    first_exception = std::make_unique<Exception>(Exception(ErrorCodes::TIMEOUT_EXCEEDED,
                        msg_format, node_path, num_unfinished_hosts, waiting_hosts.size(), num_active_hosts, offline_hosts.size(),
                        watch.elapsedSeconds(), stop_waiting_offline_hosts ? "" : ", which is longer than distributed_ddl_task_timeout"));

                /// For Replicated database print a list of unfinished hosts as well. Will return empty block on next iteration.
                if (is_replicated_database)
                    return generateChunkWithUnfinishedHosts();
                return {};
            }

            LOG_INFO(log, msg_format, node_path, num_unfinished_hosts, waiting_hosts.size(), num_active_hosts, offline_hosts.size(),
                     watch.elapsedSeconds(), stop_waiting_offline_hosts ? "" : "which is longer than distributed_ddl_task_timeout");

            return generateChunkWithUnfinishedHosts();
        }

        sleepForMilliseconds(std::min<size_t>(1000, 50 * try_number));

        bool node_exists = false;
        Strings tmp_hosts;
        Strings tmp_active_hosts;

        {
            auto retries_ctl = ZooKeeperRetriesControl(
                "executeDDLQueryOnCluster", getLogger("DDLQueryStatusSource"), getRetriesInfo(), context->getProcessListElement());
            retries_ctl.retryLoop([&]()
            {
                auto zookeeper = context->getZooKeeper();
                Strings paths = {String(fs::path(node_path) / node_to_wait), String(fs::path(node_path) / "active")};
                auto res = zookeeper->tryGetChildren(paths);
                for (size_t i = 0; i < res.size(); ++i)
                    if (res[i].error != Coordination::Error::ZOK && res[i].error != Coordination::Error::ZNONODE)
                        throw Coordination::Exception::fromPath(res[i].error, paths[i]);

                if (res[0].error == Coordination::Error::ZNONODE)
                    node_exists = zookeeper->exists(node_path);
                else
                    node_exists = true;
                tmp_hosts = res[0].names;
                tmp_active_hosts = res[1].names;

                if (only_running_hosts)
                    offline_hosts = getOfflineHosts(node_path, waiting_hosts, zookeeper, log);
            });
        }

        if (!node_exists)
        {
            /// Paradoxically, this exception will be throw even in case of "never_throw" mode.

            if (!first_exception)
                first_exception = std::make_unique<Exception>(Exception(ErrorCodes::UNFINISHED,
                        "Cannot provide query execution status. The query's node {} has been deleted by the cleaner"
                        " since it was finished (or its lifetime is expired)",
                        node_path));
            return {};
        }

        Strings new_hosts = getNewAndUpdate(tmp_hosts);
        ++try_number;

        if (only_running_hosts)
        {
            size_t num_finished_or_offline = 0;
            for (const auto & host : waiting_hosts)
                num_finished_or_offline += finished_hosts.contains(host) || offline_hosts.contains(host);

            if (num_finished_or_offline == waiting_hosts.size())
                stop_waiting_offline_hosts = true;
        }

        if (new_hosts.empty())
            continue;

        current_active_hosts = std::move(tmp_active_hosts);

        MutableColumns columns = output.getHeader().cloneEmptyColumns();
        for (const String & host_id : new_hosts)
        {
            ExecutionStatus status(-1, "Cannot obtain error message");

            /// Replicated database retries in case of error, it should not write error status.
#ifdef DEBUG_OR_SANITIZER_BUILD
            bool need_check_status = true;
#else
            bool need_check_status = !is_replicated_database;
#endif
            if (need_check_status)
            {
                String status_data;
                bool finished_exists = false;

                auto retries_ctl = ZooKeeperRetriesControl(
                    "executeDDLQueryOnCluster",
                    getLogger("DDLQueryStatusSource"),
                    getRetriesInfo(),
                    context->getProcessListElement());
                retries_ctl.retryLoop([&]()
                {
                    finished_exists = context->getZooKeeper()->tryGet(fs::path(node_path) / "finished" / host_id, status_data);
                });
                if (finished_exists)
                    status.tryDeserializeText(status_data);
            }
            else
            {
                status = ExecutionStatus{0};
            }


            if (status.code != 0 && !first_exception
                && context->getSettingsRef()[Setting::distributed_ddl_output_mode] != DistributedDDLOutputMode::NEVER_THROW)
            {
                if (is_replicated_database)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "There was an error on {}: {} (probably it's a bug)", host_id, status.message);

                auto [host, port] = parseHostAndPort(host_id);
                first_exception = std::make_unique<Exception>(Exception(status.code,
                    "There was an error on [{}:{}]: {}", host, port, status.message));
            }

            ++num_hosts_finished;

            size_t num = 0;
            if (is_replicated_database)
            {
                if (status.code != 0)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "There was an error on {}: {} (probably it's a bug)", host_id, status.message);
                auto [shard, replica] = DatabaseReplicated::parseFullReplicaName(host_id);
                columns[num++]->insert(shard);
                columns[num++]->insert(replica);
                columns[num++]->insert(OK);
            }
            else
            {
                auto [host, port] = parseHostAndPort(host_id);
                columns[num++]->insert(host);
                columns[num++]->insert(port);
                columns[num++]->insert(status.code);
                columns[num++]->insert(status.message);
            }
            columns[num++]->insert(waiting_hosts.size() - num_hosts_finished);
            columns[num++]->insert(current_active_hosts.size());
        }

        return Chunk(std::move(columns), new_hosts.size());
    }
}

IProcessor::Status DDLQueryStatusSource::prepare()
{
    /// This method is overloaded to throw exception after all data is read.
    /// Exception is pushed into pipe (instead of simply being thrown) to ensure the order of data processing and exception.

    if (finished)
    {
        if (first_exception)
        {
            if (!output.canPush())
                return Status::PortFull;

            output.pushException(std::make_exception_ptr(*first_exception));
        }

        output.finish();
        return Status::Finished;
    }
    else
        return ISource::prepare();
}

Strings DDLQueryStatusSource::getNewAndUpdate(const Strings & current_list_of_finished_hosts)
{
    Strings diff;
    for (const String & host : current_list_of_finished_hosts)
    {
        if (!waiting_hosts.contains(host))
        {
            if (!ignoring_hosts.contains(host))
            {
                ignoring_hosts.emplace(host);
                LOG_INFO(log, "Unexpected host {} appeared in task {}", host, node_path);
            }
            continue;
        }

        if (!finished_hosts.contains(host))
        {
            diff.emplace_back(host);
            finished_hosts.emplace(host);
        }
    }

    return diff;
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
