#include <Interpreters/executeDDLQueryOnCluster.h>
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
#include <Common/Macros.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <Processors/Sinks/EmptySink.h>
#include <QueryPipeline/Pipe.h>
#include <filesystem>
#include <base/sort.h>


namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int TIMEOUT_EXCEEDED;
    extern const int UNFINISHED;
    extern const int QUERY_IS_PROHIBITED;
    extern const int INVALID_SHARD_ID;
    extern const int NO_SUCH_REPLICA;
    extern const int LOGICAL_ERROR;
}

bool isSupportedAlterType(int type)
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
    if (context->getCurrentTransaction() && context->getSettingsRef().throw_on_unsupported_query_inside_transaction)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ON CLUSTER queries inside transactions are not supported");

    /// Remove FORMAT <fmt> and INTO OUTFILE <file> if exists
    ASTPtr query_ptr = query_ptr_->clone();
    ASTQueryWithOutput::resetOutputASTIfExist(*query_ptr);

    // XXX: serious design flaw since `ASTQueryWithOnCluster` is not inherited from `IAST`!
    auto * query = dynamic_cast<ASTQueryWithOnCluster *>(query_ptr.get());
    if (!query)
    {
        throw Exception("Distributed execution is not supported for such DDL queries", ErrorCodes::NOT_IMPLEMENTED);
    }

    if (!context->getSettingsRef().allow_distributed_ddl)
        throw Exception("Distributed DDL queries are prohibited for the user", ErrorCodes::QUERY_IS_PROHIBITED);

    if (const auto * query_alter = query_ptr->as<ASTAlterQuery>())
    {
        for (const auto & command : query_alter->command_list->children)
        {
            if (!isSupportedAlterType(command->as<ASTAlterCommand&>().type))
                throw Exception("Unsupported type of ALTER query", ErrorCodes::NOT_IMPLEMENTED);
        }
    }

    query->cluster = context->getMacros()->expand(query->cluster);

    /// TODO: support per-cluster grant
    context->checkAccess(AccessType::CLUSTER);

    ClusterPtr cluster = params.cluster ? params.cluster : context->getCluster(query->cluster);
    DDLWorker & ddl_worker = context->getDDLWorker();

    /// Enumerate hosts which will be used to send query.
    std::vector<HostID> hosts;
    Cluster::AddressesWithFailover shards = cluster->getShardsAddresses();

    auto collect_hosts_from_replicas = [&](size_t shard_index)
    {
        if (shard_index > shards.size())
            throw Exception(ErrorCodes::INVALID_SHARD_ID, "Cluster {} doesn't have shard #{}", query->cluster, shard_index);
        const auto & replicas = shards[shard_index - 1];
        if (params.only_replica_num)
        {
            if (params.only_replica_num > replicas.size())
                throw Exception(ErrorCodes::NO_SUCH_REPLICA, "Cluster {} doesn't have replica #{} in shard #{}", query->cluster, params.only_replica_num, shard_index);
            hosts.emplace_back(replicas[params.only_replica_num - 1]);
        }
        else
        {
            for (const auto & addr : replicas)
                hosts.emplace_back(addr);
        }
    };

    if (params.only_shard_num)
    {
        collect_hosts_from_replicas(params.only_shard_num);
    }
    else
    {
        for (size_t shard_index = 1; shard_index <= shards.size(); ++shard_index)
            collect_hosts_from_replicas(shard_index);
    }

    if (hosts.empty())
        throw Exception("No hosts defined to execute distributed DDL query", ErrorCodes::LOGICAL_ERROR);

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
        Strings shard_default_databases;
        for (const auto & shard : shards)
        {
            for (const auto & addr : shard)
            {
                if (!addr.default_database.empty())
                    shard_default_databases.push_back(addr.default_database);
                else
                    use_local_default_database = true;
            }
        }
        ::sort(shard_default_databases.begin(), shard_default_databases.end());
        shard_default_databases.erase(std::unique(shard_default_databases.begin(), shard_default_databases.end()), shard_default_databases.end());
        assert(use_local_default_database || !shard_default_databases.empty());

        if (use_local_default_database && !shard_default_databases.empty())
            throw Exception("Mixed local default DB and shard default DB in DDL query", ErrorCodes::NOT_IMPLEMENTED);

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
                    access_to_check.insert(access_to_check.begin() + i + 1, shard_default_databases.size() - 1, element);
                    for (size_t j = 0; j != shard_default_databases.size(); ++j)
                        access_to_check[i + j].replaceEmptyDatabase(shard_default_databases[j]);
                    i += shard_default_databases.size();
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
    String node_path = ddl_worker.enqueueQuery(entry);

    return getDistributedDDLStatus(node_path, entry, context);
}


class DDLQueryStatusSource final : public ISource
{
public:
    DDLQueryStatusSource(
        const String & zk_node_path, const DDLLogEntry & entry, ContextPtr context_, const std::optional<Strings> & hosts_to_wait = {});

    String getName() const override { return "DDLQueryStatus"; }
    Chunk generate() override;
    Status prepare() override;

private:
    static Strings getChildrenAllowNoNode(const std::shared_ptr<zkutil::ZooKeeper> & zookeeper, const String & node_path);

    Strings getNewAndUpdate(const Strings & current_list_of_finished_hosts);

    std::pair<String, UInt16> parseHostAndPort(const String & host_id) const;

    String node_path;
    ContextPtr context;
    Stopwatch watch;
    Poco::Logger * log;

    NameSet waiting_hosts;  /// hosts from task host list
    NameSet finished_hosts; /// finished hosts from host list
    NameSet ignoring_hosts; /// appeared hosts that are not in hosts list
    Strings current_active_hosts; /// Hosts that were in active state at the last check
    size_t num_hosts_finished = 0;

    /// Save the first detected error and throw it at the end of execution
    std::unique_ptr<Exception> first_exception;

    Int64 timeout_seconds = 120;
    bool by_hostname = true;
    bool throw_on_timeout = true;
    bool timeout_exceeded = false;
};


BlockIO getDistributedDDLStatus(const String & node_path, const DDLLogEntry & entry, ContextPtr context, const std::optional<Strings> & hosts_to_wait)
{
    BlockIO io;
    if (context->getSettingsRef().distributed_ddl_task_timeout == 0)
        return io;

    auto source = std::make_shared<DDLQueryStatusSource>(node_path, entry, context, hosts_to_wait);
    io.pipeline = QueryPipeline(std::move(source));

    if (context->getSettingsRef().distributed_ddl_output_mode == DistributedDDLOutputMode::NONE)
        io.pipeline.complete(std::make_shared<EmptySink>(io.pipeline.getHeader()));

    return io;
}

static Block getSampleBlock(ContextPtr context_, bool hosts_to_wait)
{
    auto output_mode = context_->getSettingsRef().distributed_ddl_output_mode;

    auto maybe_make_nullable = [&](const DataTypePtr & type) -> DataTypePtr
    {
        if (output_mode == DistributedDDLOutputMode::THROW || output_mode == DistributedDDLOutputMode::NONE)
            return type;
        return std::make_shared<DataTypeNullable>(type);
    };

    Block res = Block{
        {std::make_shared<DataTypeString>(),                         "host"},
        {std::make_shared<DataTypeUInt16>(),                         "port"},
        {maybe_make_nullable(std::make_shared<DataTypeInt64>()),     "status"},
        {maybe_make_nullable(std::make_shared<DataTypeString>()),    "error"},
        {std::make_shared<DataTypeUInt64>(),                         "num_hosts_remaining"},
        {std::make_shared<DataTypeUInt64>(),                         "num_hosts_active"},
    };

    if (hosts_to_wait)
        res.erase("port");

    return res;
}

DDLQueryStatusSource::DDLQueryStatusSource(
    const String & zk_node_path, const DDLLogEntry & entry, ContextPtr context_, const std::optional<Strings> & hosts_to_wait)
    : ISource(getSampleBlock(context_, hosts_to_wait.has_value()))
    , node_path(zk_node_path)
    , context(context_)
    , watch(CLOCK_MONOTONIC_COARSE)
    , log(&Poco::Logger::get("DDLQueryStatusSource"))
{
    auto output_mode = context->getSettingsRef().distributed_ddl_output_mode;
    throw_on_timeout = output_mode == DistributedDDLOutputMode::THROW || output_mode == DistributedDDLOutputMode::NONE;

    if (hosts_to_wait)
    {
        waiting_hosts = NameSet(hosts_to_wait->begin(), hosts_to_wait->end());
        by_hostname = false;
    }
    else
    {
        for (const HostID & host : entry.hosts)
            waiting_hosts.emplace(host.toString());
    }

    addTotalRowsApprox(waiting_hosts.size());
    timeout_seconds = context->getSettingsRef().distributed_ddl_task_timeout;
}

std::pair<String, UInt16> DDLQueryStatusSource::parseHostAndPort(const String & host_id) const
{
    String host = host_id;
    UInt16 port = 0;
    if (by_hostname)
    {
        auto host_and_port = Cluster::Address::fromString(host_id);
        host = host_and_port.first;
        port = host_and_port.second;
    }
    return {host, port};
}

Chunk DDLQueryStatusSource::generate()
{
    bool all_hosts_finished = num_hosts_finished >= waiting_hosts.size();

    /// Seems like num_hosts_finished cannot be strictly greater than waiting_hosts.size()
    assert(num_hosts_finished <= waiting_hosts.size());

    if (all_hosts_finished || timeout_exceeded)
        return {};

    auto zookeeper = context->getZooKeeper();
    size_t try_number = 0;

    while (true)
    {
        if (isCancelled())
            return {};

        if (timeout_seconds >= 0 && watch.elapsedSeconds() > timeout_seconds)
        {
            timeout_exceeded = true;

            size_t num_unfinished_hosts = waiting_hosts.size() - num_hosts_finished;
            size_t num_active_hosts = current_active_hosts.size();

            constexpr const char * msg_format = "Watching task {} is executing longer than distributed_ddl_task_timeout (={}) seconds. "
                                                "There are {} unfinished hosts ({} of them are currently active), "
                                                "they are going to execute the query in background";
            if (throw_on_timeout)
            {
                if (!first_exception)
                    first_exception = std::make_unique<Exception>(
                        fmt::format(msg_format, node_path, timeout_seconds, num_unfinished_hosts, num_active_hosts),
                        ErrorCodes::TIMEOUT_EXCEEDED);
                return {};
            }

            LOG_INFO(log, msg_format, node_path, timeout_seconds, num_unfinished_hosts, num_active_hosts);

            NameSet unfinished_hosts = waiting_hosts;
            for (const auto & host_id : finished_hosts)
                unfinished_hosts.erase(host_id);

            /// Query is not finished on the rest hosts, so fill the corresponding rows with NULLs.
            MutableColumns columns = output.getHeader().cloneEmptyColumns();
            for (const String & host_id : unfinished_hosts)
            {
                auto [host, port] = parseHostAndPort(host_id);
                size_t num = 0;
                columns[num++]->insert(host);
                if (by_hostname)
                    columns[num++]->insert(port);
                columns[num++]->insert(Field{});
                columns[num++]->insert(Field{});
                columns[num++]->insert(num_unfinished_hosts);
                columns[num++]->insert(num_active_hosts);
            }
            return Chunk(std::move(columns), unfinished_hosts.size());
        }

        if (num_hosts_finished != 0 || try_number != 0)
        {
            sleepForMilliseconds(std::min<size_t>(1000, 50 * (try_number + 1)));
        }

        if (!zookeeper->exists(node_path))
        {
            /// Paradoxically, this exception will be throw even in case of "never_throw" mode.

            if (!first_exception)
                first_exception = std::make_unique<Exception>(
                    fmt::format(
                        "Cannot provide query execution status. The query's node {} has been deleted by the cleaner"
                        " since it was finished (or its lifetime is expired)",
                        node_path),
                    ErrorCodes::UNFINISHED);
            return {};
        }

        Strings new_hosts = getNewAndUpdate(getChildrenAllowNoNode(zookeeper, fs::path(node_path) / "finished"));
        ++try_number;
        if (new_hosts.empty())
            continue;

        current_active_hosts = getChildrenAllowNoNode(zookeeper, fs::path(node_path) / "active");

        MutableColumns columns = output.getHeader().cloneEmptyColumns();
        for (const String & host_id : new_hosts)
        {
            ExecutionStatus status(-1, "Cannot obtain error message");
            {
                String status_data;
                if (zookeeper->tryGet(fs::path(node_path) / "finished" / host_id, status_data))
                    status.tryDeserializeText(status_data);
            }

            auto [host, port] = parseHostAndPort(host_id);

            if (status.code != 0 && !first_exception
                && context->getSettingsRef().distributed_ddl_output_mode != DistributedDDLOutputMode::NEVER_THROW)
            {
                first_exception = std::make_unique<Exception>(
                    fmt::format("There was an error on [{}:{}]: {}", host, port, status.message), status.code);
            }

            ++num_hosts_finished;

            size_t num = 0;
            columns[num++]->insert(host);
            if (by_hostname)
                columns[num++]->insert(port);
            columns[num++]->insert(status.code);
            columns[num++]->insert(status.message);
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

Strings DDLQueryStatusSource::getChildrenAllowNoNode(const std::shared_ptr<zkutil::ZooKeeper> & zookeeper, const String & node_path)
{
    Strings res;
    Coordination::Error code = zookeeper->tryGetChildren(node_path, res);
    if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNONODE)
        throw Coordination::Exception(code, node_path);
    return res;
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


}
