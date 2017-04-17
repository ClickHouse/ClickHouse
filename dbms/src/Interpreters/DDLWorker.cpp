#include <Interpreters/DDLWorker.h>

#include <Parsers/parseQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTAlterQuery.h>

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

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int UNKNOWN_FORMAT_VERSION;
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
            wb << "query: " << double_quote << query << "\n";
            wb << "hosts: " << double_quote << hosts << "\n";
            wb << "initiator: " << double_quote << initiator << "\n";
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
        rb >> "query: " >> double_quote >> query >> "\n";
        rb >> "hosts: " >> double_quote >> hosts >> "\n";
        rb >> "initiator: " >> double_quote >> initiator >> "\n";

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
    master_dir = getMastersDir() + hostname;

    zookeeper = context.getZooKeeper();
    event_queue_updated = std::make_shared<Poco::Event>();

    thread = std::thread(&DDLWorker::run, this);
}


DDLWorker::~DDLWorker()
{
    stop_flag = true;
    cond_var.notify_one();
    thread.join();
}


void DDLWorker::processTasks()
{
    Strings queue_nodes;
    int code = zookeeper->tryGetChildren(root_dir, queue_nodes, nullptr, event_queue_updated);
    if (code != ZNONODE)
        throw zkutil::KeeperException(code);

    /// Threre are no tasks
    if (code == ZNONODE || queue_nodes.empty())
        return;

    bool server_startup = last_processed_node_name.empty();

    std::sort(queue_nodes.begin(), queue_nodes.end());
    auto begin_node = server_startup
        ? queue_nodes.begin()
        : std::upper_bound(queue_nodes.begin(), queue_nodes.end(), last_processed_node_name);

    for (auto it = begin_node; it != queue_nodes.end(); ++it)
    {
        String node_data, node_name = *it, node_path = root_dir + "/" + node_name;
        code = zookeeper->tryGet(node_path, node_data);

        /// It is Ok that node could be deleted just now. It means that there are no current host in node's host list.
        if (code != ZNONODE)
            throw zkutil::KeeperException(code);

        DDLLogEntry node;
        node.parse(node_data);

        bool host_in_hostlist = std::find(node.hosts.cbegin(), node.hosts.cend(), hostname) != node.hosts.cend();

        bool already_processed = !zookeeper->exists(node_path + "/failed/" + hostname)
                                 && !zookeeper->exists(node_path + "/sucess/" + hostname);

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
    String node_path = root_dir + "/" + node_name;
    createStatusDirs(node_path);

    String active_flag_path = node_path + "/active/" + hostname;
    zookeeper->create(active_flag_path, "", zkutil::CreateMode::Ephemeral);

    /// At the end we will delete active flag and ...
    zkutil::Ops ops;
    auto acl = zookeeper->getDefaultACL();
    ops.emplace_back(std::make_unique<zkutil::Op::Remove>(active_flag_path, -1));

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

    /// ... and create sucess flag
    String fail_flag_path = node_path + "/sucess/" + hostname;
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(fail_flag_path, "", acl, zkutil::CreateMode::Persistent));
    zookeeper->multi(ops);

    return true;
}


void DDLWorker::enqueueQuery(DDLLogEntry & entry)
{
    if (entry.hosts.empty())
        return;

    String query_path_prefix = getRoot() + "/query-";
    zookeeper->createAncestors(query_path_prefix);

    String node_path = zookeeper->create(query_path_prefix, entry.toString(), zkutil::CreateMode::PersistentSequential);
    createStatusDirs(node_path);
}


void DDLWorker::run()
{
    using namespace std::chrono_literals;

    while (!stop_flag)
    {
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

        event_queue_updated->wait();
    }
}


class DDLQueryStatusInputSream : IProfilingBlockInputStream
{

};


BlockIO executeDDLQueryOnCluster(const String & query, const String & cluster_name, Context & context)
{
    ClusterPtr cluster = context.getCluster(cluster_name);
    Cluster::AddressesWithFailover shards = cluster->getShardsWithFailoverAddresses();

    Array hosts_names_failed, hosts_errors, hosts_names_pending;
    size_t num_hosts_total = 0;
    size_t num_hosts_finished_successfully = 0;

    DDLWorker & ddl_worker = context.getDDLWorker();

    DDLLogEntry entry;
    entry.query = query;
    entry.initiator = ddl_worker.getHostName();

    for (const auto & shard : shards)
        for (const auto & addr : shard)
            entry.hosts.emplace_back(addr.toString());

    ddl_worker.enqueueQuery(entry);


    auto aray_of_strings = std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    Block block{
        {std::make_shared<DataTypeUInt64>(),    "num_hosts_total"},
        {std::make_shared<DataTypeUInt64>(),    "num_hosts_finished_successfully"},
        {std::make_shared<DataTypeUInt64>(),    "num_hosts_finished_unsuccessfully"},
        {std::make_shared<DataTypeUInt64>(),    "num_hosts_pending"},
        {aray_of_strings->clone(),              "hosts_finished_unsuccessfully"},
        {aray_of_strings->clone(),              "hosts_finished_unsuccessfully_errors"},
        {aray_of_strings->clone(),              "hosts_pending"}
    };

    size_t num_hosts_finished = num_hosts_total;
    size_t num_hosts_finished_unsuccessfully = num_hosts_finished - num_hosts_finished_successfully;
    block.getByName("num_hosts_total").column->insert(num_hosts_total);
    block.getByName("num_hosts_finished_successfully").column->insert(num_hosts_finished_successfully);
    block.getByName("num_hosts_finished_unsuccessfully").column->insert(num_hosts_finished_unsuccessfully);
    block.getByName("num_hosts_pending").column->insert(0LU);
    block.getByName("hosts_finished_unsuccessfully").column->insert(hosts_names_failed);
    block.getByName("hosts_finished_unsuccessfully_errors").column->insert(hosts_errors);
    block.getByName("hosts_pending").column->insert(hosts_names_pending);

    BlockIO io;
    io.in_sample = block.cloneEmpty();
    io.in = std::make_shared<OneBlockInputStream>(block);
    return io;
}


}
