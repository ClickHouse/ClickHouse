#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/DDLWorker.h>
#include <Interpreters/DDLTask.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/queryToString.h>
#include <Access/AccessRightsElement.h>
#include <Access/ContextAccess.h>
#include <Common/Macros.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int TIMEOUT_EXCEEDED;
    extern const int UNFINISHED;
    extern const int QUERY_IS_PROHIBITED;
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

    return unsupported_alter_types.count(type) == 0;
}


BlockIO executeDDLQueryOnCluster(const ASTPtr & query_ptr_, const Context & context)
{
    return executeDDLQueryOnCluster(query_ptr_, context, {});
}

BlockIO executeDDLQueryOnCluster(const ASTPtr & query_ptr, const Context & context, const AccessRightsElements & query_requires_access, bool query_requires_grant_option)
{
    return executeDDLQueryOnCluster(query_ptr, context, AccessRightsElements{query_requires_access}, query_requires_grant_option);
}

BlockIO executeDDLQueryOnCluster(const ASTPtr & query_ptr_, const Context & context, AccessRightsElements && query_requires_access, bool query_requires_grant_option)
{
    /// Remove FORMAT <fmt> and INTO OUTFILE <file> if exists
    ASTPtr query_ptr = query_ptr_->clone();
    ASTQueryWithOutput::resetOutputASTIfExist(*query_ptr);

    // XXX: serious design flaw since `ASTQueryWithOnCluster` is not inherited from `IAST`!
    auto * query = dynamic_cast<ASTQueryWithOnCluster *>(query_ptr.get());
    if (!query)
    {
        throw Exception("Distributed execution is not supported for such DDL queries", ErrorCodes::NOT_IMPLEMENTED);
    }

    if (!context.getSettingsRef().allow_distributed_ddl)
        throw Exception("Distributed DDL queries are prohibited for the user", ErrorCodes::QUERY_IS_PROHIBITED);

    if (const auto * query_alter = query_ptr->as<ASTAlterQuery>())
    {
        for (const auto & command : query_alter->command_list->children)
        {
            if (!isSupportedAlterType(command->as<ASTAlterCommand&>().type))
                throw Exception("Unsupported type of ALTER query", ErrorCodes::NOT_IMPLEMENTED);
        }
    }

    query->cluster = context.getMacros()->expand(query->cluster);
    ClusterPtr cluster = context.getCluster(query->cluster);
    DDLWorker & ddl_worker = context.getDDLWorker();

    /// Enumerate hosts which will be used to send query.
    Cluster::AddressesWithFailover shards = cluster->getShardsAddresses();
    std::vector<HostID> hosts;
    for (const auto & shard : shards)
    {
        for (const auto & addr : shard)
            hosts.emplace_back(addr);
    }

    if (hosts.empty())
        throw Exception("No hosts defined to execute distributed DDL query", ErrorCodes::LOGICAL_ERROR);

    /// The current database in a distributed query need to be replaced with either
    /// the local current database or a shard's default database.
    bool need_replace_current_database
        = (std::find_if(
            query_requires_access.begin(),
            query_requires_access.end(),
            [](const AccessRightsElement & elem) { return elem.isEmptyDatabase(); })
           != query_requires_access.end());

    bool use_local_default_database = false;
    const String & current_database = context.getCurrentDatabase();

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
        std::sort(shard_default_databases.begin(), shard_default_databases.end());
        shard_default_databases.erase(std::unique(shard_default_databases.begin(), shard_default_databases.end()), shard_default_databases.end());
        assert(use_local_default_database || !shard_default_databases.empty());

        if (use_local_default_database && !shard_default_databases.empty())
            throw Exception("Mixed local default DB and shard default DB in DDL query", ErrorCodes::NOT_IMPLEMENTED);

        if (use_local_default_database)
        {
            query_requires_access.replaceEmptyDatabase(current_database);
        }
        else
        {
            for (size_t i = 0; i != query_requires_access.size();)
            {
                auto & element = query_requires_access[i];
                if (element.isEmptyDatabase())
                {
                    query_requires_access.insert(query_requires_access.begin() + i + 1, shard_default_databases.size() - 1, element);
                    for (size_t j = 0; j != shard_default_databases.size(); ++j)
                        query_requires_access[i + j].replaceEmptyDatabase(shard_default_databases[j]);
                    i += shard_default_databases.size();
                }
                else
                    ++i;
            }
        }
    }

    AddDefaultDatabaseVisitor visitor(current_database, !use_local_default_database);
    visitor.visitDDL(query_ptr);

    /// Check access rights, assume that all servers have the same users config
    if (query_requires_grant_option)
        context.getAccess()->checkGrantOption(query_requires_access);
    else
        context.checkAccess(query_requires_access);

    DDLLogEntry entry;
    entry.hosts = std::move(hosts);
    entry.query = queryToString(query_ptr);
    entry.initiator = ddl_worker.getCommonHostID();
    String node_path = ddl_worker.enqueueQuery(entry);

    BlockIO io;
    if (context.getSettingsRef().distributed_ddl_task_timeout == 0)
        return io;

    auto stream = std::make_shared<DDLQueryStatusInputStream>(node_path, entry, context);
    io.in = std::move(stream);
    return io;
}


DDLQueryStatusInputStream::DDLQueryStatusInputStream(const String & zk_node_path, const DDLLogEntry & entry, const Context & context_,
                                                     const std::optional<Strings> & hosts_to_wait)
    : node_path(zk_node_path)
    , context(context_)
    , watch(CLOCK_MONOTONIC_COARSE)
    , log(&Poco::Logger::get("DDLQueryStatusInputStream"))
{
    sample = Block{
        {std::make_shared<DataTypeString>(),    "host"},
        {std::make_shared<DataTypeUInt16>(),    "port"},
        {std::make_shared<DataTypeInt64>(),     "status"},
        {std::make_shared<DataTypeString>(),    "error"},
        {std::make_shared<DataTypeUInt64>(),    "num_hosts_remaining"},
        {std::make_shared<DataTypeUInt64>(),    "num_hosts_active"},
    };

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

    timeout_seconds = context.getSettingsRef().distributed_ddl_task_timeout;
}

Block DDLQueryStatusInputStream::readImpl()
{
    Block res;
    if (num_hosts_finished >= waiting_hosts.size())
    {
        if (first_exception)
            throw Exception(*first_exception);

        return res;
    }

    auto zookeeper = context.getZooKeeper();
    size_t try_number = 0;

    while (res.rows() == 0)
    {
        if (isCancelled())
        {
            if (first_exception)
                throw Exception(*first_exception);

            return res;
        }

        if (timeout_seconds >= 0 && watch.elapsedSeconds() > timeout_seconds)
        {
            size_t num_unfinished_hosts = waiting_hosts.size() - num_hosts_finished;
            size_t num_active_hosts = current_active_hosts.size();


            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED,
                            "Watching task {} is executing longer than distributed_ddl_task_timeout (={}) seconds. "
                            "There are {} unfinished hosts ({} of them are currently active), they are going to execute the query in background",
                            node_path, timeout_seconds, num_unfinished_hosts, num_active_hosts);
        }

        if (num_hosts_finished != 0 || try_number != 0)
        {
            sleepForMilliseconds(std::min<size_t>(1000, 50 * (try_number + 1)));
        }

        if (!zookeeper->exists(node_path))
        {
            throw Exception(ErrorCodes::UNFINISHED,
                            "Cannot provide query execution status. The query's node {} has been deleted by the cleaner since it was finished (or its lifetime is expired)",
                            node_path);
        }

        Strings new_hosts = getNewAndUpdate(getChildrenAllowNoNode(zookeeper, fs::path(node_path) / "finished"));
        ++try_number;
        if (new_hosts.empty())
            continue;

        current_active_hosts = getChildrenAllowNoNode(zookeeper, fs::path(node_path) / "active");

        MutableColumns columns = sample.cloneEmptyColumns();
        for (const String & host_id : new_hosts)
        {
            ExecutionStatus status(-1, "Cannot obtain error message");
            {
                String status_data;
                if (zookeeper->tryGet(fs::path(node_path) / "finished" / host_id, status_data))
                    status.tryDeserializeText(status_data);
            }

            String host = host_id;
            UInt16 port = 0;
            if (by_hostname)
            {
                auto host_and_port = Cluster::Address::fromString(host_id);
                host = host_and_port.first;
                port = host_and_port.second;
            }

            if (status.code != 0 && first_exception == nullptr)
                first_exception = std::make_unique<Exception>(status.code, "There was an error on [{}:{}]: {}", host, port, status.message);

            ++num_hosts_finished;

            columns[0]->insert(host);
            columns[1]->insert(port);
            columns[2]->insert(status.code);
            columns[3]->insert(status.message);
            columns[4]->insert(waiting_hosts.size() - num_hosts_finished);
            columns[5]->insert(current_active_hosts.size());
        }
        res = sample.cloneWithColumns(std::move(columns));
    }

    return res;
}

Strings DDLQueryStatusInputStream::getChildrenAllowNoNode(const std::shared_ptr<zkutil::ZooKeeper> & zookeeper, const String & node_path)
{
    Strings res;
    Coordination::Error code = zookeeper->tryGetChildren(node_path, res);
    if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNONODE)
        throw Coordination::Exception(code, node_path);
    return res;
}

Strings DDLQueryStatusInputStream::getNewAndUpdate(const Strings & current_list_of_finished_hosts)
{
    Strings diff;
    for (const String & host : current_list_of_finished_hosts)
    {
        if (!waiting_hosts.count(host))
        {
            if (!ignoring_hosts.count(host))
            {
                ignoring_hosts.emplace(host);
                LOG_INFO(log, "Unexpected host {} appeared  in task {}", host, node_path);
            }
            continue;
        }

        if (!finished_hosts.count(host))
        {
            diff.emplace_back(host);
            finished_hosts.emplace(host);
        }
    }

    return diff;
}


}
