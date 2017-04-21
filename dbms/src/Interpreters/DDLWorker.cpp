#include <Interpreters/DDLWorker.h>

#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTQueryWithOnCluster.h>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>

#include <Interpreters/executeQuery.h>
#include <Interpreters/Cluster.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Common/getFQDNOrHostName.h>

#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>

#include <zkutil/ZooKeeper.h>
#include <Poco/Timestamp.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int UNKNOWN_FORMAT_VERSION;
    extern const int UNFINISHED;
}


struct DDLLogEntry
{
    String query;
    Strings hosts;
    String initiator;

    static constexpr char CURRENT_VERSION = '1';

    String toString()
    {
        String res;
        {
            WriteBufferFromString wb(res);

            writeChar(CURRENT_VERSION, wb);
            wb << "\n";
            wb << "query: " << query << "\n";
            wb << "hosts: " << hosts << "\n";
            wb << "initiator: " << initiator << "\n";
        }

        return res;
    }

    void parse(const String & data)
    {
        ReadBufferFromString rb(data);

        char version;
        readChar(version, rb);
        if (version != CURRENT_VERSION)
            throw Exception("Unknown DDLLogEntry format version: " + version, ErrorCodes::UNKNOWN_FORMAT_VERSION);

        rb >> "\n";
        rb >> "query: " >> query >> "\n";
        rb >> "hosts: " >> hosts >> "\n";
        rb >> "initiator: " >> initiator >> "\n";

        assertEOF(rb);
    }
};


DDLWorker::DDLWorker(const std::string & zk_root_dir, Context & context_)
    : context(context_), stop_flag(false)
{
    root_dir = zk_root_dir;
    if (root_dir.back() == '/')
        root_dir.resize(root_dir.size() - 1);

    hostname = getFQDNOrHostName() + ':' + DB::toString(context.getTCPPort());

    zookeeper = context.getZooKeeper();
    event_queue_updated = std::make_shared<Poco::Event>();

    thread = std::thread(&DDLWorker::run, this);
}


DDLWorker::~DDLWorker()
{
    stop_flag = true;
    //cond_var.notify_one();
    event_queue_updated->set();
    thread.join();
}


void DDLWorker::processTasks()
{
    LOG_DEBUG(log, "processTasks");

    Strings queue_nodes = zookeeper->getChildren(root_dir, nullptr, event_queue_updated);
    if (queue_nodes.empty())
        return;

    LOG_DEBUG(log, "fetched " << queue_nodes.size() << " nodes");

    bool server_startup = last_processed_node_name.empty();

    std::sort(queue_nodes.begin(), queue_nodes.end());
    auto begin_node = server_startup
        ? queue_nodes.begin()
        : std::upper_bound(queue_nodes.begin(), queue_nodes.end(), last_processed_node_name);

    for (auto it = begin_node; it != queue_nodes.end(); ++it)
    {
        String node_data, node_name = *it, node_path = root_dir + "/" + node_name;
        LOG_DEBUG(log, "Fetching node " << node_path);
        if (!zookeeper->tryGet(node_path, node_data))
        {
            /// It is Ok that node could be deleted just now. It means that there are no current host in node's host list.
            continue;
        }

        LOG_DEBUG(log, "Fetched data for node " << node_name);

        DDLLogEntry node;
        node.parse(node_data);

        bool host_in_hostlist = std::find(node.hosts.cbegin(), node.hosts.cend(), hostname) != node.hosts.cend();

        bool already_processed = zookeeper->exists(node_path + "/failed/" + hostname)
                                 || zookeeper->exists(node_path + "/sucess/" + hostname);

        LOG_DEBUG(log, "node " << node_name << ", " << node.query << " status: " << host_in_hostlist << " " << already_processed);

        if (!server_startup && already_processed)
        {
            throw Exception(
                "Server expects that DDL node " + node_name + " should be processed, but it was already processed according to ZK",
                ErrorCodes::LOGICAL_ERROR);
        }

        if (host_in_hostlist && !already_processed)
        {
            try
            {
                processTask(node, node_name);
            }
            catch (...)
            {
                /// It could be network error, but we mark node as processed anyway.
                last_processed_node_name = node_name;

                tryLogCurrentException(log,
                    "An unexpected error occurred during processing DLL query " + node.query + " (" + node_name + ")");
                throw;
            }
        }

        last_processed_node_name = node_name;
    }
}

void DDLWorker::cleanupQueue(const Strings * node_names_to_check)
{
    /// Both ZK and Poco use Unix epoch
    size_t current_time_seconds = Poco::Timestamp().epochTime();
    constexpr size_t zookeeper_time_resolution = 1000;

    // Too early to check
    if (last_cleanup_time_seconds && current_time_seconds < last_cleanup_time_seconds + cleanup_after_seconds)
        return;

    last_cleanup_time_seconds = current_time_seconds;

    String data;
    zkutil::Stat stat;
    DDLLogEntry node;
    Strings failed_hosts, sucess_hosts;

    Strings node_names_fetched = node_names_to_check ? Strings{} : zookeeper->getChildren(root_dir);
    const Strings & node_names = (node_names_to_check) ? *node_names_to_check : node_names_fetched;

    for (const String & node_name : node_names)
    {
        /// TODO: Add /root/locks/node_name lock to avoid rare race counditions.
        try
        {
            String node_path = root_dir + "/" + node_name;
            if (!zookeeper->tryGet(node_path, data, &stat))
                continue;

            node.parse(data);

            size_t zookeeper_time_seconds = stat.mtime / zookeeper_time_resolution;
            if (zookeeper_time_seconds + node_max_lifetime_seconds < current_time_seconds)
            {
                size_t lifetime_seconds = current_time_seconds - zookeeper_time_seconds;
                LOG_INFO(log, "Lifetime of node " << node_name << " (" << lifetime_seconds << " sec.) is expired, deleting it");
                zookeeper->removeRecursive(node_path);
                continue;
            }

            Strings sucess_nodes = zookeeper->getChildren(node_path + "/sucess");
            Strings failed_nodes = zookeeper->getChildren(node_path + "/failed");

            if (sucess_nodes.size() + failed_nodes.size() >= node.hosts.size())
            {
                LOG_INFO(log, "Node " << node_name << " had been executed by each host, deleting it");
                zookeeper->removeRecursive(node_path);
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, "An error occured while checking and cleaning node " + node_name + " from queue");
        }
    }
}


/// Try to create unexisting "status" dirs for a node
void DDLWorker::createStatusDirs(const std::string & node_path)
{
    auto acl = zookeeper->getDefaultACL();

    zkutil::Ops ops;
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(node_path + "/active", "", acl, zkutil::CreateMode::Persistent));
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(node_path + "/sucess", "", acl, zkutil::CreateMode::Persistent));
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(node_path + "/failed", "", acl, zkutil::CreateMode::Persistent));

    int code = zookeeper->tryMulti(ops);

    if (code != ZOK && code != ZNODEEXISTS)
        throw zkutil::KeeperException(code);
}


bool DDLWorker::processTask(const DDLLogEntry & node, const std::string & node_name)
{
    LOG_DEBUG(log, "Process " << node_name << " node, query " << node.query);

    String node_path = root_dir + "/" + node_name;
    createStatusDirs(node_path);

    LOG_DEBUG(log, "Created status dirs");

    String active_flag_path = node_path + "/active/" + hostname;
    zookeeper->create(active_flag_path, "", zkutil::CreateMode::Ephemeral);

    LOG_DEBUG(log, "Created active flag");

    /// At the end we will delete active flag and ...
    zkutil::Ops ops;
    auto acl = zookeeper->getDefaultACL();
    ops.emplace_back(std::make_unique<zkutil::Op::Remove>(active_flag_path, -1));

    LOG_DEBUG(log, "Executing query: " << node.query);

    try
    {
        executeQuery(node.query, context);
    }
    catch (...)
    {
        /// ... and create fail flag
        String fail_flag_path = node_path + "/failed/" + hostname;
        String exception_msg = getCurrentExceptionMessage(false, true);

        ops.emplace_back(std::make_unique<zkutil::Op::Create>(fail_flag_path, exception_msg, acl, zkutil::CreateMode::Persistent));
        zookeeper->multi(ops);

        tryLogCurrentException(log, "Query " + node.query + " wasn't finished successfully");
        return false;
    }

    LOG_DEBUG(log, "Executed query: " << node.query);

    /// ... and create sucess flag
    String fail_flag_path = node_path + "/sucess/" + hostname;
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(fail_flag_path, "", acl, zkutil::CreateMode::Persistent));
    zookeeper->multi(ops);

    LOG_DEBUG(log, "Removed flags");

    return true;
}


String DDLWorker::enqueueQuery(DDLLogEntry & entry)
{
    if (entry.hosts.empty())
        return {};

    String query_path_prefix = root_dir + "/query-";
    zookeeper->createAncestors(query_path_prefix);

    String node_path = zookeeper->create(query_path_prefix, entry.toString(), zkutil::CreateMode::PersistentSequential);
    createStatusDirs(node_path);

    return node_path;
}


void DDLWorker::run()
{
    using namespace std::chrono_literals;

    zookeeper->createAncestors(root_dir + "/");
    LOG_DEBUG(log, "Started DDLWorker thread");

    while (!stop_flag)
    {
        LOG_DEBUG(log, "Begin tasks processing");

        try
        {
            cleanupQueue();
        }
        catch (...)
        {
            tryLogCurrentException(log);
        }

        try
        {
            processTasks();
        }
        catch (...)
        {
            tryLogCurrentException(log);
        }

        //std::unique_lock<std::mutex> g(lock);
        //cond_var.wait_for(g, 10s);

        LOG_DEBUG(log, "Waiting watch");
        event_queue_updated->wait();
    }
}


class DDLQueryStatusInputSream : public IProfilingBlockInputStream
{
public:

    DDLQueryStatusInputSream(const String & zk_node_path, Context & context, size_t num_hosts)
    : node_path(zk_node_path), context(context)
    {
        sample = Block{
            {std::make_shared<DataTypeString>(),    "host"},
            {std::make_shared<DataTypeUInt64>(),    "status"},
            {std::make_shared<DataTypeString>(),    "error"},
            {std::make_shared<DataTypeUInt64>(),    "num_hosts_remaining"},
            {std::make_shared<DataTypeUInt64>(),    "num_hosts_active"},
        };

        setTotalRowsApprox(num_hosts);
    }

    String getName() const override
    {
        return "DDLQueryStatusInputSream";
    }

    String getID() const override
    {
        return "DDLQueryStatusInputSream(" + node_path + ")";
    }

    Block readImpl() override
    {
        Block res;
        if (num_hosts_finished >= total_rows_approx)
            return res;

        auto zookeeper = context.getZooKeeper();
        size_t try_number = 0;

        while(res.rows() == 0)
        {
            if (is_cancelled)
                return res;

            if (num_hosts_finished != 0 || try_number != 0)
                std::this_thread::sleep_for(std::chrono::milliseconds(50 * std::min(20LU, try_number + 1)));

            /// TODO: add /lock node somewhere
            if (!zookeeper->exists(node_path))
            {
                throw Exception("Cannot provide query execution status. The query's node " + node_path
                                + " had been deleted by cleaner since it was finished (or its lifetime is expired)",
                                ErrorCodes::UNFINISHED);
            }

            Strings new_sucess_hosts = getNewAndUpdate(sucess_hosts_set, getChildrenAllowNoNode(zookeeper, node_path + "/sucess"));
            Strings new_failed_hosts = getNewAndUpdate(failed_hosts_set, getChildrenAllowNoNode(zookeeper, node_path + "/failed"));

            Strings new_hosts = new_sucess_hosts;
            new_hosts.insert(new_hosts.end(), new_failed_hosts.cbegin(), new_failed_hosts.cend());
            ++try_number;

            if (new_hosts.empty())
                continue;

            Strings cur_active_hosts = getChildrenAllowNoNode(zookeeper, node_path + "/active");

            res = sample.cloneEmpty();
            for (size_t i = 0; i < new_hosts.size(); ++i)
            {
                bool fail = i >= new_sucess_hosts.size();
                res.getByName("host").column->insert(new_hosts[i]);
                res.getByName("status").column->insert(static_cast<UInt64>(fail));
                res.getByName("error").column->insert(fail ? zookeeper->get(node_path + "/failed/" + new_hosts[i]) : String{});
                res.getByName("num_hosts_remaining").column->insert(total_rows_approx - (++num_hosts_finished));
                res.getByName("num_hosts_active").column->insert(cur_active_hosts.size());
            }
        }

        return res;
    }

    static Strings getChildrenAllowNoNode(const std::shared_ptr<zkutil::ZooKeeper> & zookeeper, const String & node_path)
    {
        Strings res;
        int code = zookeeper->tryGetChildren(node_path, res);
        if (code != ZOK && code != ZNONODE)
            throw zkutil::KeeperException(code, node_path);
        return res;
    }

    static Strings getNewAndUpdate(NameSet & prev, const Strings & cur_list)
    {
        Strings diff;
        for (const String & elem : cur_list)
        {
            if (!prev.count(elem))
            {
                diff.emplace_back(elem);
                prev.emplace(elem);
            }
        }

        return diff;
    }

    ~DDLQueryStatusInputSream() override = default;

    Block sample;

private:
    String node_path;
    Context & context;

    NameSet sucess_hosts_set;
    NameSet failed_hosts_set;
    size_t num_hosts_finished = 0;
};


BlockIO executeDDLQueryOnCluster(const ASTQueryWithOnCluster & query, Context & context)
{
    ClusterPtr cluster = context.getCluster(query.cluster);
    DDLWorker & ddl_worker = context.getDDLWorker();

    /// Do we really should use that database for each server?
    String query_str = query.getRewrittenQueryWithoutOnCluster(context.getCurrentDatabase());

    DDLLogEntry entry;
    entry.query = query_str;
    entry.initiator = ddl_worker.getHostName();

    Cluster::AddressesWithFailover shards = cluster->getShardsWithFailoverAddresses();
    for (const auto & shard : shards)
        for (const auto & addr : shard)
            entry.hosts.emplace_back(addr.toString());

    String node_path = ddl_worker.enqueueQuery(entry);

    BlockIO io;
    if (node_path.empty())
        return io;

    auto stream = std::make_shared<DDLQueryStatusInputSream>(node_path, context, entry.hosts.size());
    io.in_sample = stream->sample.cloneEmpty();
    io.in = std::move(stream);
    return io;
}


}
