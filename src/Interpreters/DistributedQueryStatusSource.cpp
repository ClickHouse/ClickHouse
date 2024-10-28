#include <Core/Block.h>
#include <Core/Settings.h>
#include <Core/SettingsEnums.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/DistributedQueryStatusSource.h>
#include <Common/Exception.h>
#include <Common/ZooKeeper/ZooKeeper.h>

namespace DB
{
namespace Setting
{
extern const SettingsDistributedDDLOutputMode distributed_ddl_output_mode;
extern const SettingsInt64 distributed_ddl_task_timeout;
}
namespace ErrorCodes
{
extern const int UNFINISHED;
}

DistributedQueryStatusSource::DistributedQueryStatusSource(
    const String & zk_node_path,
    const String & zk_replicas_path,
    Block block,
    ContextPtr context_,
    const Strings & hosts_to_wait,
    const char * logger_name)
    : ISource(block)
    , node_path(zk_node_path)
    , replicas_path(zk_replicas_path)
    , context(context_)
    , watch(CLOCK_MONOTONIC_COARSE)
    , log(getLogger(logger_name))
{
    auto output_mode = context->getSettingsRef()[Setting::distributed_ddl_output_mode];
    throw_on_timeout = output_mode == DistributedDDLOutputMode::THROW || output_mode == DistributedDDLOutputMode::NONE;
    throw_on_timeout_only_active
        = output_mode == DistributedDDLOutputMode::THROW_ONLY_ACTIVE || output_mode == DistributedDDLOutputMode::NONE_ONLY_ACTIVE;

    waiting_hosts = NameSet(hosts_to_wait.begin(), hosts_to_wait.end());

    only_running_hosts = output_mode == DistributedDDLOutputMode::THROW_ONLY_ACTIVE
        || output_mode == DistributedDDLOutputMode::NULL_STATUS_ON_TIMEOUT_ONLY_ACTIVE
        || output_mode == DistributedDDLOutputMode::NONE_ONLY_ACTIVE;

    addTotalRowsApprox(waiting_hosts.size());
    timeout_seconds = context->getSettingsRef()[Setting::distributed_ddl_task_timeout];
}


IProcessor::Status DistributedQueryStatusSource::prepare()
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

NameSet DistributedQueryStatusSource::getOfflineHosts(const NameSet & hosts_to_wait, const ZooKeeperPtr & zookeeper)
{
    Strings paths;
    Strings hosts_array;
    for (const auto & host : hosts_to_wait)
    {
        hosts_array.push_back(host);
        paths.push_back(fs::path(replicas_path) / host / "active");
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

Strings DistributedQueryStatusSource::getNewAndUpdate(const Strings & current_finished_hosts)
{
    Strings diff;
    for (const String & host : current_finished_hosts)
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


ExecutionStatus DistributedQueryStatusSource::getExecutionStatus(const fs::path & status_path)
{
    ExecutionStatus status(-1, "Cannot obtain error message");

    String status_data;
    bool finished_exists = false;

    auto retries_ctl = ZooKeeperRetriesControl(
        "executeDDLQueryOnCluster", getLogger("DDLQueryStatusSource"), getRetriesInfo(), context->getProcessListElement());
    retries_ctl.retryLoop([&]() { finished_exists = context->getZooKeeper()->tryGet(status_path, status_data); });
    if (finished_exists)
        status.tryDeserializeText(status_data);

    return status;
}

ZooKeeperRetriesInfo DistributedQueryStatusSource::getRetriesInfo()
{
    const auto & config_ref = Context::getGlobalContextInstance()->getConfigRef();
    return ZooKeeperRetriesInfo(
        config_ref.getInt("distributed_ddl_keeper_max_retries", 5),
        config_ref.getInt("distributed_ddl_keeper_initial_backoff_ms", 100),
        config_ref.getInt("distributed_ddl_keeper_max_backoff_ms", 5000));
}

std::pair<String, UInt16> DistributedQueryStatusSource::parseHostAndPort(const String & host_id)
{
    String host = host_id;
    UInt16 port = 0;
    auto host_and_port = Cluster::Address::fromString(host_id);
    host = host_and_port.first;
    port = host_and_port.second;
    return {host, port};
}

Chunk DistributedQueryStatusSource::generate()
{
    bool all_hosts_finished = num_hosts_finished >= waiting_hosts.size();

    /// Seems like num_hosts_finished cannot be strictly greater than waiting_hosts.size()
    assert(num_hosts_finished <= waiting_hosts.size());

    if (all_hosts_finished || timeout_exceeded)
        return {};

    size_t try_number = 0;
    while (true)
    {
        if (isCancelled())
            return {};

        if (stop_waiting_offline_hosts)
        {
            return stopWaitingOfflineHosts();
        }

        if ((timeout_seconds >= 0 && watch.elapsedSeconds() > timeout_seconds))
        {
            return handleTimeoutExceeded();
        }

        sleepForMilliseconds(std::min<size_t>(1000, 50 * try_number));

        bool node_exists = false;
        Strings tmp_hosts;
        Strings tmp_active_hosts;

        {
            auto retries_ctl = ZooKeeperRetriesControl(
                "executeDistributedQueryOnCluster", getLogger(getName()), getRetriesInfo(), context->getProcessListElement());
            retries_ctl.retryLoop(
                [&]()
                {
                    auto zookeeper = context->getZooKeeper();
                    Strings paths = getNodesToWait();
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
                        offline_hosts = getOfflineHosts(waiting_hosts, zookeeper);
                });
        }

        if (!node_exists)
        {
            /// Paradoxically, this exception will be throw even in case of "never_throw" mode.

            if (!first_exception)
                first_exception = std::make_unique<Exception>(Exception(
                    ErrorCodes::UNFINISHED,
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
            ExecutionStatus status = checkStatus(host_id);

            if (status.code != 0)
            {
                handleNonZeroStatusCode(status, host_id);
            }

            ++num_hosts_finished;
            fillHostStatus(host_id, status, columns);
        }

        return Chunk(std::move(columns), new_hosts.size());
    }
}

}
