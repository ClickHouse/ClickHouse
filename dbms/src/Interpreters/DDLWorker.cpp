#include <Interpreters/DDLWorker.h>

#include <Parsers/parseQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTAlterQuery.h>

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
}

namespace {

/// Helper class which extracts from the ClickHouse configuration file
/// the parameters we need for operating the resharding thread.
// struct Arguments
// {
// public:
//     Arguments(const Poco::Util::AbstractConfiguration & config, const std::string & config_name)
//     {
//         Poco::Util::AbstractConfiguration::Keys keys;
//         config.keys(config_name, keys);
//
//         for (const auto & key : keys)
//         {
//             if (key == "distributed_ddl_root")
//                 ddl_queries_root = config.getString(config_name + "." + key);
//             else
//                 throw Exception{"Unknown parameter in distributed DDL configuration", ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG};
//         }
//
//         if (ddl_queries_root.empty())
//             throw Exception{"Distributed DDL: missing parameter distributed_ddl_root", ErrorCodes::INVALID_CONFIG_PARAMETER};
//     }
//
//     std::string ddl_queries_root;
// };

}


DDLWorker::DDLWorker(const std::string & zk_root_dir, Context & context_)
    : context(context_), stop_flag(false)
{
    root_dir = zk_root_dir;
    if (root_dir.back() == '/')
        root_dir.resize(root_dir.size() - 1);

    hostname = getFQDNOrHostName() + ':' + DB::toString(context.getTCPPort());
    assign_dir = getAssignsDir() + hostname;
    master_dir = getMastersDir() + hostname;

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
    auto zookeeper = context.getZooKeeper();

    if (!zookeeper->exists(assign_dir))
        return;

    Strings tasks = zookeeper->getChildren(assign_dir);
    if (tasks.empty())
        return;

    String current_task = *std::min_element(tasks.cbegin(), tasks.cend());

    try
    {
        processTask(current_task);
    }
    catch (...)
    {
        tryLogCurrentException(log, "An unexpected error occurred during task " + current_task);
        throw;
    }
}


bool DDLWorker::processTask(const std::string & task)
{
    auto zookeeper = context.getZooKeeper();

    String query_dir    = root_dir + "/" + task;
    String assign_node  = assign_dir + "/" + task;
    String active_node  = query_dir + "/active/" + hostname;
    String sucsess_node = query_dir + "/sucess/" + hostname;
    String fail_node    = query_dir + "/failed/" + hostname;

    String query = zookeeper->get(query_dir);

    if (zookeeper->exists(sucsess_node) || zookeeper->exists(fail_node))
    {
        throw Exception(
            "Task " + task + " (query " + query + ")  was already processed by node " + hostname, ErrorCodes::LOGICAL_ERROR);
    }

    /// Create active flag
    zookeeper->create(active_node, "", zkutil::CreateMode::Ephemeral);

    /// Will delete task from host's tasks list, delete active flag and ...
    zkutil::Ops ops;
    ops.emplace_back(std::make_unique<zkutil::Op::Remove>(assign_node, -1));
    ops.emplace_back(std::make_unique<zkutil::Op::Remove>(active_node, -1));

    try
    {
        executeQuery(query, context);
    }
    catch (...)
    {
        /// ... and create fail flag
        String exception_msg = getCurrentExceptionMessage(false, true);
        ops.emplace_back(std::make_unique<zkutil::Op::Create>(fail_node, exception_msg, nullptr, zkutil::CreateMode::Persistent));
        zookeeper->multi(ops);

        tryLogCurrentException(log, "Query " + query + " wasn't finished successfully");
        return false;
    }

    /// .. and create sucess flag
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(sucsess_node, "", nullptr, zkutil::CreateMode::Persistent));
    zookeeper->multi(ops);

    return true;
}


void DDLWorker::enqueueQuery(const String & query, const std::vector<Cluster::Address> & addrs)
{
    auto zookeeper = context.getZooKeeper();

    String assigns_dir = getAssignsDir();
    String master_dir = getCurrentMasterDir();
    String query_path_prefix = getRoot() + "/query-";

    zookeeper->createAncestors(assigns_dir + "/");
    zookeeper->createAncestors(master_dir + "/");
    String query_path = zookeeper->create(query_path_prefix, query, zkutil::CreateMode::PersistentSequential);
    String query_node = query_path.substr(query_path.find_last_of('/') + 1);

    zkutil::Ops ops;
    auto acl = zookeeper->getDefaultACL();
    constexpr size_t max_ops_per_call = 100;

    /// Create /root/masters/query_node and /root/query-node/* to monitor status of the tasks initiated by us
    {
        String num_hosts = toString(addrs.size());
        String master_query_node = master_dir + "/" + query_node;
        ops.emplace_back(std::make_unique<zkutil::Op::Create>(master_query_node,      "",        acl, zkutil::CreateMode::Persistent));
        ops.emplace_back(std::make_unique<zkutil::Op::Create>(query_path + "/active", "",        acl, zkutil::CreateMode::Persistent));
        ops.emplace_back(std::make_unique<zkutil::Op::Create>(query_path + "/sucess", num_hosts, acl, zkutil::CreateMode::Persistent));
        ops.emplace_back(std::make_unique<zkutil::Op::Create>(query_path + "/failed", "",        acl, zkutil::CreateMode::Persistent));
        zookeeper->multi(ops);
        ops.clear();
    }

    /// Create hosts's taks dir /root/assigns/host (if not exists)
    for (auto it = addrs.cbegin(); it != addrs.cend(); ++it)
    {
        String cur_assign_dir = assigns_dir + "/" + it->toString();
        ops.emplace_back(std::make_unique<zkutil::Op::Create>(cur_assign_dir, "", acl, zkutil::CreateMode::Persistent));

        if (ops.size() > max_ops_per_call || std::next(it) == addrs.cend())
        {
            int code = zookeeper->tryMulti(ops);
            ops.clear();

            if (code != ZOK && code != ZNODEEXISTS)
                throw zkutil::KeeperException(code);
        }
    }

    /// Asssign tasks to hosts /root/assigns/host/query_node
    for (auto it = addrs.cbegin(); it != addrs.cend(); ++it)
    {
        String cur_task_path = assigns_dir + "/" + it->toString() + "/" + query_node;
        ops.emplace_back(std::make_unique<zkutil::Op::Create>(cur_task_path, "", acl, zkutil::CreateMode::Persistent));

        if (ops.size() > max_ops_per_call || std::next(it) == addrs.cend())
        {
            zookeeper->multi(ops);
            ops.clear();
        }
    }
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
        catch (const std::exception & ex)
        {
            LOG_ERROR(log, ex.what());
        }

        std::unique_lock<std::mutex> g(lock);
        cond_var.wait_for(g, 10s);
    }
}


static bool getRemoteQueryExecutionStatus(const Cluster::Address & addr, const std::string & query, Exception & out_exception)
{
    Connection conn(addr.host_name, addr.port, "", addr.user, addr.password);
    conn.sendQuery(query);

    while (true)
    {
        Connection::Packet packet = conn.receivePacket();

        if (packet.type == Protocol::Server::Exception)
        {
            out_exception = *packet.exception;
            return false;
        }
        else if (packet.type == Protocol::Server::EndOfStream)
            break;
    }

    return true;
}


BlockIO executeDDLQueryOnCluster(const String & query, const String & cluster_name, Context & context)
{
    ClusterPtr cluster = context.getCluster(cluster_name);
    Cluster::AddressesWithFailover shards = cluster->getShardsWithFailoverAddresses();
    std::vector<Cluster::Address> pending_hosts;

    Array hosts_names_failed, hosts_errors, hosts_names_pending;
    size_t num_hosts_total = 0;
    size_t num_hosts_finished_successfully = 0;

    for (const auto & shard : shards)
    {
        for (const auto & addr : shard)
        {
            try
            {
                Exception ex;
                if (!getRemoteQueryExecutionStatus(addr, query, ex))
                {
                    /// Normal error
                    String exception_msg = getExceptionMessage(ex, false, true);
                    hosts_names_failed.emplace_back(addr.host_name);
                    hosts_errors.emplace_back(exception_msg);

                    LOG_INFO(&Logger::get("DDLWorker"), "Query " << query << " failed on " << addr.host_name << ": " << exception_msg);
                }
                else
                {
                    ++num_hosts_finished_successfully;
                }
            }
            catch (...)
            {
                /// Network error
                pending_hosts.emplace_back(addr);
                hosts_names_pending.emplace_back(addr.host_name);
            }
        }
        num_hosts_total += shard.size();
    }


    if (!pending_hosts.empty())
        context.getDDLWorker().enqueueQuery(query, pending_hosts);


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

    size_t num_hosts_finished = num_hosts_total - pending_hosts.size();
    size_t num_hosts_finished_unsuccessfully = num_hosts_finished - num_hosts_finished_successfully;
    block.getByName("num_hosts_total").column->insert(num_hosts_total);
    block.getByName("num_hosts_finished_successfully").column->insert(num_hosts_finished_successfully);
    block.getByName("num_hosts_finished_unsuccessfully").column->insert(num_hosts_finished_unsuccessfully);
    block.getByName("num_hosts_pending").column->insert(pending_hosts.size());
    block.getByName("hosts_finished_unsuccessfully").column->insert(hosts_names_failed);
    block.getByName("hosts_finished_unsuccessfully_errors").column->insert(hosts_errors);
    block.getByName("hosts_pending").column->insert(hosts_names_pending);

    BlockIO io;
    io.in_sample = block.cloneEmpty();
    io.in = std::make_shared<OneBlockInputStream>(block);
    return io;
}


}
