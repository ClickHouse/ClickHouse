#include <random>
#include <Access/AccessRightsElement.h>
#include <Access/ContextAccess.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/ClusterWorker.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTClusterQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTOptimizeQuery.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Storages/IStorage.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <pcg_random.hpp>
#include <Poco/Net/NetException.h>
#include <Poco/Timestamp.h>
#include <Common/DNSResolver.h>
#include <Common/Macros.h>
#include <Common/Stopwatch.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/isLocalAddress.h>
#include <Common/quoteString.h>
#include <Common/randomSeed.h>
#include <Common/setThreadName.h>
#include <common/getFQDNOrHostName.h>
#include <common/sleep.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_FORMAT_VERSION;
    extern const int TIMEOUT_EXCEEDED;
    extern const int UNKNOWN_TYPE_OF_QUERY;
    extern const int UNFINISHED;
}

class ZooKeeperLock
{
public:
    /// lock_prefix - path where the ephemeral lock node will be created
    /// lock_name - the name of the ephemeral lock node
    ZooKeeperLock(
        const zkutil::ZooKeeperPtr & zookeeper_,
        const std::string & lock_prefix_,
        const std::string & lock_name_,
        const std::string & lock_message_ = "")
        : zookeeper(zookeeper_)
        , lock_path(lock_prefix_ + "/" + lock_name_)
        , lock_message(lock_message_)
        , log(&Poco::Logger::get("zkutil::Lock"))
    {
        zookeeper->createIfNotExists(lock_prefix_, "");
    }

    ~ZooKeeperLock()
    {
        try
        {
            unlock();
        }
        catch (...)
        {
            DB::tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    void unlock()
    {
        Coordination::Stat stat;
        std::string dummy;
        bool result = zookeeper->tryGet(lock_path, dummy, &stat);

        if (result && stat.ephemeralOwner == zookeeper->getClientID())
            zookeeper->remove(lock_path, -1);
        else
            LOG_WARNING(log, "Lock is lost. It is normal if session was expired. Path: {}/{}", lock_path, lock_message);
    }

    bool tryLock()
    {
        std::string dummy;
        Coordination::Error code = zookeeper->tryCreate(lock_path, lock_message, zkutil::CreateMode::Ephemeral, dummy);

        if (code == Coordination::Error::ZNODEEXISTS)
        {
            return false;
        }
        else if (code == Coordination::Error::ZOK)
        {
            return true;
        }
        else
        {
            throw Coordination::Exception(code);
        }
    }

private:
    zkutil::ZooKeeperPtr zookeeper;

    std::string lock_path;
    std::string lock_message;
    Poco::Logger * log;
};

/// default cluster zk path : /clickhouse/cluster/node/
/// node directory = ip:port
struct NodeInfo
{
    enum Type
    {
        ACTIVATED = 1,
        PAUSED = 2,
        DECOMMISSION = 3,
        DROPPED = 4
    };

    Type status;
    HostID host_id;
    std::set<String> clusters;
    std::vector<Cluster::Address *> address_vec;

    static constexpr int CURRENT_VERSION = 1;

    String toString()
    {
        WriteBufferFromOwnString wb;
        auto version = CURRENT_VERSION;
        Strings cluster_strings(clusters.size());
        std::transform(clusters.begin(), clusters.end(), cluster_strings.begin(), [](String s) { return s; });

        wb << "version: " << version << "\n";
        wb << "status: " << static_cast<UInt16>(status) << "\n";
        wb << "host: " << host_id.readableString() << "\n";
        wb << "clusters: " << cluster_strings << "\n";

        return wb.str();
    }

    void parse(const String & data)
    {
        ReadBufferFromString rb(data);
        UInt16 st_int;
        String host_str;
        Strings cluster_strings;

        int version;
        rb >> "version: " >> version >> "\n";

        if (version != CURRENT_VERSION)
            throw Exception("Unknown nodeinfo format version: " + DB::toString(version), ErrorCodes::UNKNOWN_FORMAT_VERSION);

        rb >> "status: " >> st_int >> "\n";
        rb >> "host: " >> host_str >> "\n";
        rb >> "clusters: " >> cluster_strings >> "\n";

        status = Type(st_int);
        host_id = HostID::fromString(host_str);

        for (auto & s : cluster_strings)
        {
            clusters.insert(s);
        }
    }
};


struct ClusterEntry
{
    String query;
    ASTClusterQuery * query_ptr;
    HostVec opt_node_vec;
    HostMap all_host_map;
    Clusters::Impl clusters;
    String initiator; //optional
    static constexpr int CURRENT_VERSION = 1;

    String toString() const
    {
        WriteBufferFromOwnString wb;
        auto version = CURRENT_VERSION;
        wb << "version: " << version << "\n";
        wb << "query: " << escape << query << "\n";
        return wb.str();
    }

    void parse(const String & data)
    {
        ReadBufferFromString rb(data);
        int version;
        rb >> "version: " >> version >> "\n";

        if (version != CURRENT_VERSION)
            throw Exception("Unknown nodeinfo format version: " + DB::toString(version), ErrorCodes::UNKNOWN_FORMAT_VERSION);
        rb >> "query: " >> escape >> query >> "\n";
    }
};

static void filterAndSortQueueNodes(Strings & all_nodes)
{
    all_nodes.erase(
        std::remove_if(all_nodes.begin(), all_nodes.end(), [](const String & s) { return !startsWith(s, "query-"); }), all_nodes.end());
    std::sort(all_nodes.begin(), all_nodes.end());
}


class ClusterQueryStatusInputStream : public IBlockInputStream
{
public:
    ClusterQueryStatusInputStream(const String & zk_node_path, ClusterEntry & entry, ContextPtr context_)
        : query_path(zk_node_path)
        , context(context_)
        , watch(CLOCK_MONOTONIC_COARSE)
        , log(&Poco::Logger::get("ClusterQueryStatusInputStream"))
    {
        sample = Block{
            {std::make_shared<DataTypeString>(), "host"},
            {std::make_shared<DataTypeUInt16>(), "port"},
            {std::make_shared<DataTypeInt64>(), "status"},
            {std::make_shared<DataTypeString>(), "error"},
            {std::make_shared<DataTypeUInt64>(), "num_hosts_remaining"},
            {std::make_shared<DataTypeUInt64>(), "num_hosts_active"},
        };

        for (auto it = entry.all_host_map.begin(); it != entry.all_host_map.end(); ++it)
        {
            auto node_info = it->second;
            bool opted = false;
            for (auto & opt_node : entry.opt_node_vec)
            {
                //Skip the operated node
                if (node_info->host_id.toString() == opt_node->host_id.toString())
                {
                    LOG_DEBUG(log, "Skip operated node {}", opt_node->host_id.toString());
                    opted = true;
                    ignoring_hosts.emplace(node_info->host_id.toString());
                    break;
                }
            }
            if (!opted)
            {
                waiting_hosts.emplace(node_info->host_id.toString());
                LOG_DEBUG(log, "Waiting host {}", node_info->host_id.toString());
            }
        }

        addTotalRowsApprox(waiting_hosts.size());

        LOG_DEBUG(
            log,
            "All host size {}, waiting hosts {}, ignoring hosts {}",
            entry.all_host_map.size(),
            waiting_hosts.size(),
            ignoring_hosts.size());

        timeout_seconds = context->getSettingsRef().distributed_ddl_task_timeout;
    }

    String getName() const override { return "ClusterQueryStatusInputStream"; }

    Block getHeader() const override { return sample; }

    Block readImpl() override
    {
        Block res;
        if (num_hosts_finished >= waiting_hosts.size())
        {
            if (first_exception)
                throw Exception(*first_exception);

            return res;
        }

        auto zookeeper = context->getZooKeeper();
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

                constexpr const char * msg_format = "Watching task {} is executing longer than distributed_ddl_task_timeout (={}) seconds. "
                                                    "There are {} unfinished hosts ({} of them are currently active), "
                                                    "they are going to execute the query in background";
                throw Exception(
                    ErrorCodes::TIMEOUT_EXCEEDED, msg_format, query_path, timeout_seconds, num_unfinished_hosts, num_active_hosts);
            }

            if (num_hosts_finished != 0 || try_number != 0)
            {
                auto current_sleep_for = std::chrono::milliseconds(std::min(static_cast<size_t>(1000), 50 * (try_number + 1)));
                std::this_thread::sleep_for(current_sleep_for);
            }

            if (!zookeeper->exists(query_path))
            {
                throw Exception(
                    "Cannot provide query execution status. The query's node " + query_path
                        + " has been deleted by the cleaner since it was finished (or its lifetime is expired)",
                    ErrorCodes::UNFINISHED);
            }

            Strings new_hosts = getNewAndUpdate(getChildrenAllowNoNode(zookeeper, query_path + "/finished"));

            ++try_number;
            if (new_hosts.empty())
                continue;

            current_active_hosts = getChildrenAllowNoNode(zookeeper, query_path + "/active");

            MutableColumns columns = sample.cloneEmptyColumns();
            for (const String & host_id : new_hosts)
            {
                ExecutionStatus status(-1, "Cannot obtain error message");
                {
                    String status_data;
                    if (zookeeper->tryGet(query_path + "/finished/" + host_id, status_data))
                    {
                        status.tryDeserializeText(status_data);
                    }
                }

                auto [host, port] = Cluster::Address::fromString(host_id);

                if (status.code != 0 && first_exception == nullptr)
                    first_exception = std::make_unique<Exception>(
                        "There was an error on [" + host + ":" + toString(port) + "]: " + status.message, status.code);

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

    Block getSampleBlock() const { return sample.cloneEmpty(); }

    ~ClusterQueryStatusInputStream() override = default;

private:
    static Strings getChildrenAllowNoNode(const std::shared_ptr<zkutil::ZooKeeper> & zookeeper, const String & node_path)
    {
        Strings res;
        Coordination::Error code = zookeeper->tryGetChildren(node_path, res);
        if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNONODE)
            throw Coordination::Exception(code, node_path);
        return res;
    }

    Strings getNewAndUpdate(const Strings & current_list_of_finished_hosts)
    {
        Strings diff;
        for (const String & host : current_list_of_finished_hosts)
        {
            if (!waiting_hosts.count(host))
            {
                if (!ignoring_hosts.count(host))
                {
                    ignoring_hosts.emplace(host);
                    LOG_INFO(log, "Unexpected host {} appeared  in task {}", host, query_path);
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

    String query_path;
    ContextPtr context;
    Stopwatch watch;
    Poco::Logger * log;

    Block sample;

    NameSet waiting_hosts; /// hosts from task host list
    NameSet finished_hosts; /// finished hosts from host list
    NameSet ignoring_hosts; /// appeared hosts that are not in hosts list
    Strings current_active_hosts; /// Hosts that were in active state at the last check
    size_t num_hosts_finished = 0;

    /// Save the first detected error and throw it at the end of execution
    std::unique_ptr<Exception> first_exception;

    Int64 timeout_seconds = 120;
};

std::unique_ptr<ZooKeeperLock> createSimpleZooKeeperLock(
    const zkutil::ZooKeeperPtr & zookeeper, const String & lock_prefix, const String & lock_name, const String & lock_message)
{
    return std::make_unique<ZooKeeperLock>(zookeeper, lock_prefix, lock_name, lock_message);
}

/// Try to create nonexisting "status" dirs for a node
void createStatusDirs(const std::string & query_path, const ZooKeeperPtr & zookeeper)
{
    Coordination::Requests ops;
    {
        Coordination::CreateRequest request;
        request.path = query_path + "/active";
        ops.emplace_back(std::make_shared<Coordination::CreateRequest>(std::move(request)));
    }
    {
        Coordination::CreateRequest request;
        request.path = query_path + "/finished";
        ops.emplace_back(std::make_shared<Coordination::CreateRequest>(std::move(request)));
    }
    Coordination::Responses responses;
    Coordination::Error code = zookeeper->tryMulti(ops, responses);
    if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNODEEXISTS)
        throw Coordination::Exception(code);
}


static bool isSupportedClusterType(int type)
{
    static const std::unordered_set<int> supported_system_types{
        //ASTClusterQuery::ADD_NODE,
        ASTClusterQuery::PAUSE_NODE,
        ASTClusterQuery::START_NODE,
        //ASTClusterQuery::DROP_NODE,
        //ASTClusterQuery::REPLACE_NODE
    };

    return supported_system_types.count(type) == 1;
}


ClusterWorker::ClusterWorker(
    const std::string & zk_root_dir, ContextPtr context_, const Poco::Util::AbstractConfiguration * config, const String & prefix)
    : context(context_), log(&Poco::Logger::get("ClusterWorker"))
{
    cluster_dir = zk_root_dir;
    if (cluster_dir.back() == '/')
        cluster_dir.resize(cluster_dir.size() - 1);

    if (config)
    {
        if (config->has(prefix + ".profile"))
            context->setSetting("profile", config->getString(prefix + ".profile"));
    }

    if (context->getSettingsRef().readonly)
    {
        LOG_WARNING(
            log,
            "Cluster worker is run with readonly settings, it will not be able to execute CLUSTER queries Set appropriate "
            "system_profile or distributed_ddl.profile to fix this.");
    }

    auto host_port = context->getInterserverIOAddress();
    local_host_id.host_name = host_port.first;
    local_host_id.port = context->getTCPPort();

    node_dir = cluster_dir + "/node";
    queue_dir = cluster_dir + "/queue";

    main_thread = ThreadFromGlobalPool(&ClusterWorker::runMainThread, this);
}


ClusterWorker::~ClusterWorker()
{
    stop_flag = true;
    cluster_updated_event->set();
    main_thread.join();
}


/// Parses query and resolves cluster and host in cluster
void ClusterWorker::parseQuery(ClusterEntry & entry, NodeInfo & node)
{
    ASTClusterQuery * query_ptr = entry.query_ptr;
    //query_ptr->
    node.host_id.host_name = query_ptr->server;
    node.host_id.port = query_ptr->port;
    switch (query_ptr->type)
    {
        case ASTClusterQuery::PAUSE_NODE:
            node.status = NodeInfo::PAUSED;
            break;
        case ASTClusterQuery::START_NODE:
            node.status = NodeInfo::ACTIVATED;
            break;
        default:
            node.status = NodeInfo::ACTIVATED;
    }
}


void ClusterWorker::attachToThreadGroup()
{
    if (thread_group)
    {
        /// Put all threads to one thread pool
        CurrentThread::attachToIfDetached(thread_group);
    }
    else
    {
        CurrentThread::initializeQuery();
        thread_group = CurrentThread::getGroup();
    }
}


ZooKeeperPtr ClusterWorker::tryGetZooKeeper() const
{
    std::lock_guard lock(zookeeper_mutex);
    return current_zookeeper;
}

ZooKeeperPtr ClusterWorker::getAndSetZooKeeper()
{
    std::lock_guard lock(zookeeper_mutex);

    if (!current_zookeeper || current_zookeeper->expired())
        current_zookeeper = context->getZooKeeper();

    return current_zookeeper;
}


void ClusterWorker::getHostsFromClusters(HostMap & host_map, String cluster)
{
    Clusters::Impl cluster_map = context->getClusters().getContainer();

    for (auto & it : cluster_map)
    {
        String cluster_name = it.first;
        //LOG_DEBUG(log, "cluster name {}, current cluster {}", cluster_name, cluster);
        if (!cluster.empty() && cluster_name != cluster)
        {
            LOG_DEBUG(log, "Skip cluster {}", cluster_name);
            continue;
        }
        Cluster::AddressesWithFailover * shards = it.second->getClusterShardsAddresses();
        for (auto & address_vec : *shards)
        {
            for (auto & address : address_vec)
            {
                HostIDPtr host = std::make_shared<HostID>();
                host->host_name = address.host_name;
                host->port = address.port;
                String readable_str = host->readableString();
                if (host_map.find(readable_str) == host_map.end())
                {
                    NodeInfoPtr node_info = std::make_shared<NodeInfo>();
                    node_info->status = static_cast<NodeInfo::Type>(address.status);
                    node_info->host_id.host_name = address.host_name;
                    node_info->host_id.port = address.port;
                    node_info->clusters.insert(cluster_name);
                    node_info->address_vec.push_back(&address);
                    //LOG_DEBUG(log, "Get node {} status {} from context", node_info->host_id.toString(), node_info->status);
                    host_map[readable_str] = std::move(node_info);
                }
                else
                {
                    NodeInfoPtr node_info = host_map.find(readable_str)->second;
                    node_info->address_vec.push_back(&address);
                    if (node_info->clusters.find(cluster_name) == node_info->clusters.end())
                    {
                        node_info->clusters.insert(cluster_name);
                    }
                }
            }
        }
    }
}


void ClusterWorker::registerNodes(const ZooKeeperPtr & zookeeper)
{
    LOG_DEBUG(log, "Register config nodes to zookeeper");

    HostMap host_map;

    getHostsFromClusters(host_map, "");

    auto lock = createSimpleZooKeeperLock(zookeeper, node_dir, "lock", local_host_id.toString());

    if (!lock->tryLock())
    {
        return;
    }

    String executed_by;
    //for (auto it = host_map.begin(); it != host_map.end(); ++it)
    for (auto & it : host_map)
    {
        auto node_info = it.second;
        String curr_node_dir = node_dir + "/" + node_info->host_id.readableString();
        if (!zookeeper->tryGet(curr_node_dir, executed_by))
        {
            String node_path = zookeeper->create(curr_node_dir, node_info->toString(), zkutil::CreateMode::Persistent);
            LOG_DEBUG(log, "Register node path {}, node data {}", curr_node_dir, node_info->toString());
        }
    }
    lock->unlock();
    LOG_DEBUG(log, "All nodes is registered to zookeeper.");
}


bool ClusterWorker::updateLocalNodes(Strings & cluster_nodes, const ZooKeeperPtr & zookeeper)
{
    LOG_DEBUG(log, "Update local nodes status from zookeeper");
    HostMap zk_host_map;
    for (const auto & node : cluster_nodes)
    {
        NodeInfoPtr node_info = std::make_shared<NodeInfo>();
        String curr_node_dir = node_dir + "/" + node;
        String node_data;
        if (!zookeeper->tryGet(curr_node_dir, node_data))
        {
            LOG_DEBUG(log, "Can not get cluster node data, node dir {}", curr_node_dir);
            continue;
        }
        node_info->parse(node_data);
        LOG_DEBUG(log, "Zookeeper node data:{}, node info:{}", node_data, node_info->toString());
        zk_host_map[node] = std::move(node_info);
    }

    HostMap local_host_map;
    getHostsFromClusters(local_host_map, "");

    for (auto it = local_host_map.begin(); it != local_host_map.end(); ++it)
    {
        auto zk_host = zk_host_map.find(it->first);
        auto local_node = it->second;
        if (zk_host == zk_host_map.end())
        {
            local_node->status = NodeInfo::PAUSED;
            LOG_DEBUG(
                log,
                "Can't find node {} from zookeeper, the node set to PAUSED, address size {}",
                it->first,
                local_node->address_vec.size());
        }
        else
        {
            local_node->status = zk_host->second->status;
            //LOG_DEBUG(log, "The node {} status set to {}, address size {}", it->first, zk_host->second->status, local_node->address_vec.size());
        }

        for (auto address_it = local_node->address_vec.begin(); address_it != local_node->address_vec.end(); ++address_it)
        {
            Cluster::Address * address = *address_it;
            address->status = local_node->status;
            LOG_DEBUG(log, "The context node {} status set to {}", address->toString(), address->status);
        }
    }

    return true;
}


String ClusterWorker::alterNodes(ClusterEntry & entry, const ZooKeeperPtr & zookeeper)
{
    Strings cluster_nodes = zookeeper->getChildren(node_dir, nullptr, cluster_updated_event);
    if (cluster_nodes.empty())
    {
        registerNodes(zookeeper);
        cluster_nodes = zookeeper->getChildren(node_dir, nullptr, cluster_updated_event);
    }

    auto node = std::make_unique<NodeInfo>();
    parseQuery(entry, *node);

    Strings entry_nodes;
    for (auto & entry_name : cluster_nodes)
    {
        if (node->host_id.port == 0)
        {
            if (entry_name.find(node->host_id.host_name) != String::npos)
            {
                entry_nodes.push_back(entry_name);
            }
        }
        else if (entry_name == node->host_id.readableString())
        {
            entry_nodes.push_back(entry_name);
            break;
        }
    }

    if (entry_nodes.empty() && entry.query_ptr->type != ASTClusterQuery::ADD_NODE)
    {
        throw Exception("No corresponding node was found in the configuration file", ErrorCodes::UNKNOWN_TYPE_OF_QUERY);
    }

    String node_data, curr_node_dir;
    for (auto & entry_name : entry_nodes)
    {
        curr_node_dir = node_dir + "/" + entry_name;

        if (!zookeeper->tryGet(curr_node_dir, node_data))
        {
            /// It is Ok that node could be deleted just now. It means that there are no current host in node's host list.
            LOG_DEBUG(log, "Can not get zookeeper node data {}", curr_node_dir);
            return curr_node_dir;
        }

        NodeInfoPtr node_info = std::make_shared<NodeInfo>();

        try
        {
            node_info->parse(node_data);
            //LOG_DEBUG(log, "load node info path:{}, zk data:{}, data:{}", curr_node_dir, node_data, node_info->toString());
        }
        catch (...)
        {
            tryLogCurrentException(log, "Cannot parse node info " + curr_node_dir + ", will try to send error status");
            return curr_node_dir;
        }

        node_info->status = node->status;
        LOG_DEBUG(log, "Modify node info path:{}, data:{}", curr_node_dir, node_info->toString());

        zookeeper->set(curr_node_dir, node_info->toString());

        entry.opt_node_vec.emplace_back(node_info);
    }

    return curr_node_dir;
}


void ClusterWorker::updateQueueStatus(const ZooKeeperPtr & zookeeper)
{
    LOG_DEBUG(log, "Update zookeeper queue status");
    zkutil::EventPtr event = std::make_shared<Poco::Event>();
    if (!zookeeper->exists(queue_dir, nullptr, event))
    {
        LOG_DEBUG(log, "Not found cluster queue {}.", queue_dir);
        return;
    }

    Strings queue_nodes = zookeeper->getChildren(queue_dir, nullptr, cluster_updated_event);
    if (queue_nodes.empty())
        return;

    bool server_startup = last_processed_task_name.empty();

    auto begin_node
        = server_startup ? queue_nodes.begin() : std::upper_bound(queue_nodes.begin(), queue_nodes.end(), last_processed_task_name);

    ExecutionStatus execution_status = ExecutionStatus(0);
    for (auto it = begin_node; it != queue_nodes.end(); ++it)
    {
        String entry_name = *it;
        String finished_path = queue_dir + "/" + entry_name + "/finished/" + local_host_id.toString();
        LOG_DEBUG(log, "Finish queue {}", finished_path);
        bool already_processed = zookeeper->exists(finished_path);
        if (!already_processed)
        {
            String dummy;
            zookeeper->tryCreate(finished_path, execution_status.serializeText(), zkutil::CreateMode::Ephemeral, dummy);
        }
        if (stop_flag)
            break;
    }
}

void ClusterWorker::cleanupQueue(const ZooKeeperPtr & zookeeper)
{
    LOG_DEBUG(log, "Begin cleaning queue");

    Int64 current_time_seconds = Poco::Timestamp().epochTime();

    Strings query_nodes = zookeeper->getChildren(queue_dir);
    filterAndSortQueueNodes(query_nodes);

    for (auto it = query_nodes.cbegin(); it < query_nodes.cend(); ++it)
    {
        if (stop_flag)
            return;

        String query_name = *it;
        String query_path = queue_dir + "/" + query_name;

        Coordination::Stat stat;
        String dummy;

        try
        {
            /// Already deleted
            if (!zookeeper->exists(query_path, &stat))
                continue;

            /// Delete node if its lifetime is expired (according to task_max_lifetime parameter)
            constexpr UInt64 zookeeper_time_resolution = 1000;
            Int64 zookeeper_time_seconds = stat.ctime / zookeeper_time_resolution;
            bool node_lifetime_is_expired = (zookeeper_time_seconds + task_max_lifetime) < current_time_seconds;

            if (!node_lifetime_is_expired)
                continue;
            else
                LOG_INFO(log, "Lifetime of task {} is expired, deleting it", query_path);

            /// Deleting
            {
                Strings children = zookeeper->getChildren(query_path);
                for (const String & child : children)
                {
                    zookeeper->tryRemoveRecursive(query_path + "/" + child);
                }

                /// Remove the lock node and its parent atomically
                Coordination::Requests ops;
                ops.emplace_back(zkutil::makeRemoveRequest(query_path, -1));
                zookeeper->multi(ops);
            }
        }
        catch (...)
        {
            LOG_INFO(
                log, "An error occurred while checking and cleaning task {} from queue: {}", query_name, getCurrentExceptionMessage(false));
        }
    }
}

void ClusterWorker::processCluster()
{
    std::lock_guard lock(sync_mutex);

    LOG_DEBUG(log, "Processing sync node status of cluster");

    auto zookeeper = tryGetZooKeeper();

    Strings cluster_nodes = zookeeper->getChildren(node_dir, nullptr, cluster_updated_event);
    if (cluster_nodes.empty())
    {
        registerNodes(zookeeper);
    }
    else if (updateLocalNodes(cluster_nodes, zookeeper))
    {
        updateQueueStatus(zookeeper);
        cleanupQueue(zookeeper);
    }
}

String ClusterWorker::enqueueQuery(ClusterEntry & entry)
{
    LOG_DEBUG(log, "Enqueue query {}", entry.query);
    auto zookeeper = getAndSetZooKeeper();

    alterNodes(entry, zookeeper);

    String queue_path_prefix = queue_dir + "/query-";
    zookeeper->createAncestors(queue_path_prefix);

    String query_path = zookeeper->create(queue_path_prefix, entry.toString(), zkutil::CreateMode::PersistentSequential);

    /// Optional step
    try
    {
        createStatusDirs(query_path, zookeeper);
    }
    catch (...)
    {
        LOG_INFO(
            log,
            "An error occurred while creating auxiliary ZooKeeper directories in {} . They will be created later. Error : {}",
            query_path,
            getCurrentExceptionMessage(true));
    }

    return query_path;
}


void ClusterWorker::runMainThread()
{
    setThreadName("ClusterWorker");
    LOG_DEBUG(log, "Started ClusterWorker main thread");

    bool initialized = false;
    do
    {
        try
        {
            auto zookeeper = getAndSetZooKeeper();
            zookeeper->createAncestors(cluster_dir + "/");
            zookeeper->createAncestors(node_dir + "/");
            zookeeper->createAncestors(queue_dir + "/");
            initialized = true;
        }
        catch (const Coordination::Exception & e)
        {
            if (!Coordination::isHardwareError(e.code))
                throw; /// A logical error.

            tryLogCurrentException(__PRETTY_FUNCTION__);

            /// Avoid busy loop when ZooKeeper is not available.
            sleepForSeconds(1);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Terminating. Cannot initialize cluster node.");
            return;
        }
    } while (!initialized && !stop_flag);

    int retry_times = 0;

    while (!stop_flag && retry_times <= 3)
    {
        try
        {
            attachToThreadGroup();

            processCluster();

            retry_times = 0;

            LOG_DEBUG(log, "Waiting a watch");
            cluster_updated_event->wait();
        }
        catch (const Coordination::Exception & e)
        {
            if (Coordination::isHardwareError(e.code))
            {
                LOG_DEBUG(log, "Recovering ZooKeeper session after: {}", getCurrentExceptionMessage(false));

                while (!stop_flag)
                {
                    try
                    {
                        getAndSetZooKeeper();
                        break;
                    }
                    catch (...)
                    {
                        tryLogCurrentException(__PRETTY_FUNCTION__);

                        using namespace std::chrono_literals;
                        std::this_thread::sleep_for(5s);
                    }
                }
            }
            else if (e.code == Coordination::Error::ZNONODE)
            {
                LOG_ERROR(log, "ZooKeeper error: {}", getCurrentExceptionMessage(true));
            }
            else
            {
                LOG_ERROR(log, "Unexpected ZooKeeper error: {}. Terminating.", getCurrentExceptionMessage(true));
                retry_times++;
            }
            sleep(1);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Unexpected error, will terminate:");
            retry_times++;
            sleep(1);
        }
    }
}


BlockIO executeClusterQuery(
    const ASTPtr & query_ptr_, ContextPtr context, AccessRightsElements && query_requires_access, bool query_requires_grant_option)
{
    auto * log = &(Poco::Logger::get("executeClusterQuery"));

    /// Remove FORMAT <fmt> and INTO OUTFILE <file> if exists
    ASTPtr query_ptr = query_ptr_->clone();
    ASTQueryWithOutput::resetOutputASTIfExist(*query_ptr);

    // XXX: serious design flaw since `ASTQueryWithOnCluster` is not inherited from `IAST`!
    auto * query = dynamic_cast<ASTQueryWithOnCluster *>(query_ptr.get());
    if (!query)
    {
        throw Exception("Cluster execution is not supported for such cluster queries", ErrorCodes::NOT_IMPLEMENTED);
    }

    auto * query_cluster = query_ptr->as<ASTClusterQuery>();
    if (query_cluster)
    {
        if (!isSupportedClusterType(query_cluster->type))
            throw Exception("Unsupported type of cluster query", ErrorCodes::NOT_IMPLEMENTED);
    }

    ClusterWorker & cluster_worker = context->getClusterWorker();

    /// Check access rights, assume that all servers have the same users config
    if (query_requires_grant_option)
        context->getAccess()->checkGrantOption(query_requires_access);
    else
        context->checkAccess(query_requires_access);

    ClusterEntry entry;
    entry.query = queryToString(query_ptr);
    entry.query_ptr = std::move(query_cluster);

    entry.clusters = context->getClusters().getContainer();
    cluster_worker.getHostsFromClusters(entry.all_host_map, query->cluster);

    LOG_DEBUG(log, "Query {}, cluster {}, cluster host size {}", entry.query, query->cluster, entry.all_host_map.size());

    entry.initiator = cluster_worker.getCommonHostID();
    String curr_query_dir = cluster_worker.enqueueQuery(entry);

    BlockIO io;
    if (context->getSettingsRef().distributed_ddl_task_timeout == 0)
        return io;

    auto stream = std::make_shared<ClusterQueryStatusInputStream>(curr_query_dir, entry, context);
    io.in = std::move(stream);
    return io;
}

BlockIO executeClusterQuery(
    const ASTPtr & query_ptr, ContextPtr context, const AccessRightsElements & query_requires_access, bool query_requires_grant_option)
{
    return executeClusterQuery(query_ptr, context, AccessRightsElements{query_requires_access}, query_requires_grant_option);
}

BlockIO executeClusterQuery(const ASTPtr & query_ptr_, ContextPtr context)
{
    return executeClusterQuery(query_ptr_, context, {});
}
}
