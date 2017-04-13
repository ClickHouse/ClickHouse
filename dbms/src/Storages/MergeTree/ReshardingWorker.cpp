#include <Storages/MergeTree/ReshardingWorker.h>
#include <Storages/MergeTree/ReshardingJob.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataMerger.h>
#include <Storages/MergeTree/ReplicatedMergeTreeAddress.h>
#include <Storages/MergeTree/MergeTreeSharder.h>
#include <Storages/MergeTree/MergeTreeBlockInputStream.h>
#include <Storages/StorageReplicatedMergeTree.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Common/getFQDNOrHostName.h>
#include <Common/SipHash.h>
#include <Common/StringUtils.h>
#include <Common/randomSeed.h>

#include <Interpreters/executeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/Cluster.h>

#include <Common/ThreadPool.h>

#include <zkutil/ZooKeeper.h>

#include <Poco/Event.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/File.h>

#include <future>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <random>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ABORTED;
    extern const int UNEXPECTED_ZOOKEEPER_ERROR;
    extern const int PARTITION_COPY_FAILED;
    extern const int PARTITION_ATTACH_FAILED;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int RESHARDING_BUSY_CLUSTER;
    extern const int RESHARDING_BUSY_SHARD;
    extern const int RESHARDING_NO_SUCH_COORDINATOR;
    extern const int RESHARDING_NO_COORDINATOR_MEMBERSHIP;
    extern const int RESHARDING_ALREADY_SUBSCRIBED;
    extern const int RESHARDING_REMOTE_NODE_UNAVAILABLE;
    extern const int RESHARDING_REMOTE_NODE_ERROR;
    extern const int RESHARDING_COORDINATOR_DELETED;
    extern const int RESHARDING_DISTRIBUTED_JOB_ON_HOLD;
    extern const int RESHARDING_INVALID_QUERY;
    extern const int RWLOCK_NO_SUCH_LOCK;
    extern const int NO_SUCH_BARRIER;
    extern const int RESHARDING_ILL_FORMED_LOG;
}

namespace
{

constexpr size_t hash_size = 16;
constexpr long wait_duration = 1000;

/// Helper class which extracts from the ClickHouse configuration file
/// the parameters we need for operating the resharding thread.
class Arguments final
{
public:
    Arguments(const Poco::Util::AbstractConfiguration & config, const std::string & config_name)
    {
        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys(config_name, keys);

        for (const auto & key : keys)
        {
            if (key == "task_queue_path")
                ddl_queries_root = config.getString(config_name + "." + key);
            else
                throw Exception{"Unknown parameter in resharding configuration", ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG};
        }

        if (ddl_queries_root.empty())
            throw Exception{"Resharding: missing parameter task_queue_path", ErrorCodes::INVALID_CONFIG_PARAMETER};
    }

    Arguments(const Arguments &) = delete;
    Arguments & operator=(const Arguments &) = delete;

    std::string getTaskQueuePath() const
    {
        return ddl_queries_root;
    }

private:
    std::string ddl_queries_root;
};

/// Helper class we use to read and write the status of a coordinator
/// or a performer that has subscribed to a coordinator.
/// The status format is: status_code [, optional_message]
class Status final
{
public:
    Status(ReshardingWorker::StatusCode code_, const std::string & msg_)
        : code{code_}, msg{msg_}
    {
    }

    Status(const std::string & str)
    {
        size_t pos = str.find(',');
        code = static_cast<ReshardingWorker::StatusCode>(std::stoull(str.substr(0, pos)));
        if (pos != std::string::npos)
        {
            if ((pos + 1) < str.length())
                msg = str.substr(pos + 1);
        }
    }

    Status(const Status &) = delete;
    Status & operator=(const Status &) = delete;

    std::string toString() const
    {
        return DB::toString(static_cast<UInt64>(code)) + (msg.empty() ? "" : ",") + msg;
    }

    ReshardingWorker::StatusCode getCode() const
    {
        return code;
    }

    std::string getMessage() const
    {
        return msg;
    }

private:
    ReshardingWorker::StatusCode code;
    std::string msg;
};

/// Compute the hash value of a specified string.
/// The hash function we use is SipHash.
std::string computeHashFromString(const std::string & in)
{
    SipHash hash;
    hash.update(in.data(), in.size());

    char out[hash_size];
    hash.get128(out);

    return {out, hash_size};
}

#if 0
/// Compute the value value from the checksum files of a given partition.
/// The hash function we use is SipHash.
std::string computeHashFromPartition(const std::string & data_path, const std::string & partition_name)
{
    std::vector<std::string> files;

    Poco::DirectoryIterator end;
    Poco::DirectoryIterator end2;

    for (Poco::DirectoryIterator it(data_path); it != end; ++it)
    {
        const auto filename = it.name();
        if (!ActiveDataPartSet::isPartDirectory(filename))
            continue;
        if (!startsWith(filename, partition_name))
            continue;

        const auto part_path = it.path().absolute().toString();
        for (Poco::DirectoryIterator it2(part_path); it2 != end2; ++it2)
        {
            const auto & filename = it2.name();
            if (filename == "checksums.txt")
                files.push_back(it2.path().absolute().toString());
        }
    }

    std::sort(files.begin(), files.end());

    SipHash hash;

    for (const auto & file : files)
    {
        ReadBufferFromFile buf{file};
        while (buf.next())
        {
            size_t byte_count = buf.buffer().end() - buf.position();
            hash.update(buf.position(), byte_count);
        }
    }

    char out[hash_size];
    hash.get128(out);

    return {out, hash_size};
}
#endif

}

/// Job structure:
///
/// /shards: various information on target shards that is needed in order to build
/// the log;
///
/// /log: contains one log record for each operation to be performed;
///
/// /is_published: znode created after uploading sharded partitions. If we have just
/// recovered from a failure, and this znode exists, apply changes without resharding
/// the initial partition. Indeed we could experience a failure after completing a
/// "ALTER TABLE ... DROP PARTITION ...", which would rule out any further attempt
/// to reshard the initial partition;
///
/// /is_log_created: znode created after creating the log. Prevents from recreating
/// it after a failure;
///
/// /is_committed: changes have been committed locally.
///

/// Rationale for distributed jobs:
///
/// A distributed job is initiated in a query ALTER TABLE RESHARD inside which
/// we specify a distributed table. Then ClickHouse creates a job coordinating
/// structure in ZooKeeper, namely a coordinator, identified by a so-called
/// coordinator id.
/// Each shard of the cluster specified in the distributed table metadata
/// receives one query ALTER TABLE RESHARD with the keyword COORDINATE WITH
/// indicating the aforementioned coordinator id.
///
/// Locking notes:
///
/// In order to avoid any deadlock situation, two approaches are implemented:
///
/// 1. As long as a cluster is busy with a distributed job, we forbid clients
/// to submit any new distributed job on this cluster. For this purpose, clusters
/// are identified by the hash value of the ordered list of their hostnames and
/// ports. In the event that an initiator should identify a cluster node by means
/// of a local address, some additional checks are performed on shards themselves.
///
/// 2. Also, the jobs that constitute a distributed job are submitted in identical
/// order on all the shards of a cluster. If one or more shards fail while performing
/// a distributed job, the latter is put on hold. Then no new jobs are performed
/// until the failing shards have come back online.
///
/// ZooKeeper coordinator structure description:
///
/// At the highest level we have under the /resharding_distributed znode:
///
/// /lock: global distributed read/write lock;
/// /online: currently online performers;
/// /coordination: one znode for each coordinator.
///
/// A coordinator whose identifier is ${id} has the following layout
/// under the /coordination/${id} znode:
///
/// /lock: coordinator-specific distributed read/write lock;
///
/// /deletion_lock: for safe coordinator deletion
///
/// /query_hash: hash value obtained from the query that is sent to the performers;
///
/// /increment: unique block number allocator;
///
/// /status: coordinator status before its performers have subscribed;
///
/// /status/${host}: status of an individual performer;
///
/// /status_probe: znode that we update just after having updated either the status
/// of a performer or the status of the coordinator as a whole;
///
/// /cluster: cluster on which the distributed job is to be performed;
///
/// /node_count: number of nodes of the cluster that participate in at
/// least one distributed resharding job;
///
/// /cluster_addresses: the list of addresses, in both IP and hostname
/// representations, of all the nodes of the cluster; used to check if a given node
/// is a member of the cluster;
///
/// /shards: the list of shards that have subscribed;
///
/// /subscribe_barrier: when all the performers have subscribed to their coordinator,
/// proceed further
///
/// /check_barrier: when all the performers have checked that they can perform
/// resharding operations, proceed further;
///
/// /opt_out_barrier: after having crossed this barrier, each node of the cluster
/// knows exactly whether it will take part in distributed jobs or not.
///
/// /partitions: partitions that must be resharded on more than one shard;
///
/// /partitions/${partition_id}/nodes: performers;
///
/// /partitions/${partition_id}/upload_barrier: when all the performers have uploaded
/// new data to the target shards, we can apply changes;
///
/// /partitions/${partition_id}/election_barrier: used for the election of
/// a leader among the performers;
///
/// /partitions/${partition_id}/commit_barrier: crossed when all the changes
/// required by a resharding operation have been applied;
///
/// /partitions/${partition_id}/recovery_barrier: recovery if one or several
/// performers had previously gone offline.
///

ReshardingWorker::ReshardingWorker(const Poco::Util::AbstractConfiguration & config,
    const std::string & config_name, Context & context_)
    : context{context_}, get_zookeeper{[&]() { return context.getZooKeeper(); }}
{
    Arguments arguments(config, config_name);
    auto zookeeper = context.getZooKeeper();

    std::string root = arguments.getTaskQueuePath();
    if (root.back() != '/')
        root += "/";

    auto current_host = getFQDNOrHostName();

    task_queue_path = root + "resharding/";

    host_task_queue_path = task_queue_path + current_host;
    zookeeper->createAncestors(host_task_queue_path + "/");

    distributed_path = root + "resharding_distributed";
    zookeeper->createAncestors(distributed_path + "/");

    distributed_online_path = distributed_path + "/online";
    zookeeper->createIfNotExists(distributed_online_path, "");

    /// Notify that we are online.
    int32_t code = zookeeper->tryCreate(distributed_online_path + "/" + current_host, "",
        zkutil::CreateMode::Ephemeral);
    if ((code != ZOK) && (code != ZNODEEXISTS))
        throw zkutil::KeeperException{code};

    distributed_lock_path = distributed_path + "/lock";
    zookeeper->createIfNotExists(distributed_lock_path, "");

    coordination_path = distributed_path + "/coordination";
    zookeeper->createAncestors(coordination_path + "/");
}

ReshardingWorker::~ReshardingWorker()
{
    try
    {
        shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void ReshardingWorker::start()
{
    job_tracker = std::thread{&ReshardingWorker::trackAndPerform, this};
}

void ReshardingWorker::shutdown()
{
    if (is_started)
    {
        must_stop = true;
        if (job_tracker.joinable())
            job_tracker.join();
        is_started = false;
    }
}

void ReshardingWorker::submitJob(const ReshardingJob & job)
{
    auto serialized_job = job.toString();
    auto zookeeper = context.getZooKeeper();
    (void) zookeeper->create(host_task_queue_path + "/task-", serialized_job,
        zkutil::CreateMode::PersistentSequential);
}

bool ReshardingWorker::isStarted() const
{
    return is_started;
}

void ReshardingWorker::trackAndPerform()
{
    std::string error_msg;

    try
    {
        bool old_val = false;
        if (!is_started.compare_exchange_strong(old_val, true, std::memory_order_seq_cst,
            std::memory_order_relaxed))
            throw Exception{"Resharding background thread already started", ErrorCodes::LOGICAL_ERROR};

        LOG_DEBUG(log, "Started resharding background thread.");

        try
        {
            performPendingJobs();
        }
        catch (const Exception & ex)
        {
            if (ex.code() == ErrorCodes::ABORTED)
                throw;
            else
                LOG_ERROR(log, ex.message());
        }
        catch (const std::exception & ex)
        {
            LOG_ERROR(log, ex.what());
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        while (true)
        {
            try
            {
                Strings children;

                while (true)
                {
                    auto zookeeper = context.getZooKeeper();
                    children = zookeeper->getChildren(host_task_queue_path, nullptr, event);

                    if (!children.empty())
                        break;

                    do
                    {
                        abortTrackingIfRequested();
                    }
                    while (!event->tryWait(wait_duration));
                }

                std::sort(children.begin(), children.end());
                perform(children);
            }
            catch (const Exception & ex)
            {
                if (ex.code() == ErrorCodes::ABORTED)
                    throw;
                else
                    LOG_ERROR(log, ex.message());
            }
            catch (const std::exception & ex)
            {
                LOG_ERROR(log, ex.what());
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }
    }
    catch (const Exception & ex)
    {
        if (ex.code() != ErrorCodes::ABORTED)
            error_msg = ex.message();
    }
    catch (const std::exception & ex)
    {
        error_msg = ex.what();
    }
    catch (...)
    {
        error_msg = "unspecified";
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    if (!error_msg.empty())
        LOG_ERROR(log, "Resharding background thread terminated with critical error: "
            << error_msg);
    else
        LOG_DEBUG(log, "Resharding background thread terminated.");
}

void ReshardingWorker::wakeUpTrackerThread()
{
    /// We inform the job tracker thread that something has just happened. This forces
    /// the job tracker to fetch all the current pending jobs. We need this when a
    /// distributed job is not ready to be performed yet. Otherwise if no new jobs
    /// were submitted, we wouldn't be able to check again whether we can perform
    /// the distributed job.
    event->set();

    /// Sleep for 3 time units in order to prevent overloading of the job tracker
    /// thread if we are the only job in the queue.
    auto zookeeper = context.getZooKeeper();
    if (zookeeper->getChildren(host_task_queue_path).size() == 1)
        std::this_thread::sleep_for(3 * std::chrono::milliseconds(wait_duration));
}

void ReshardingWorker::performPendingJobs()
{
    auto zookeeper = context.getZooKeeper();

    Strings children = zookeeper->getChildren(host_task_queue_path);
    std::sort(children.begin(), children.end());
    perform(children);
}

void ReshardingWorker::perform(const Strings & job_nodes)
{
    auto zookeeper = context.getZooKeeper();

    for (const auto & child : job_nodes)
    {
        std::string  child_full_path = host_task_queue_path + "/" + child;
        auto job_descriptor = zookeeper->get(child_full_path);

        try
        {
            perform(job_descriptor, child);
        }
        catch (const zkutil::KeeperException & ex)
        {
            /// We shall try again to perform this job.
            throw;
        }
        catch (const Exception & ex)
        {
            /// If the job has been cancelled, either locally or remotely, we keep it
            /// in the corresponding task queues for a later execution.
            /// If an error has occurred, either locally or remotely, while
            /// performing the job, we delete it from the corresponding task queues.
            try
            {
                if (ex.code() == ErrorCodes::ABORTED)
                {
                    /// nothing here
                }
                else if (ex.code() == ErrorCodes::RESHARDING_REMOTE_NODE_UNAVAILABLE)
                {
                    /// nothing here
                }
                else if (ex.code() == ErrorCodes::RESHARDING_DISTRIBUTED_JOB_ON_HOLD)
                {
                    /// nothing here
                }
                else if (ex.code() == ErrorCodes::RESHARDING_REMOTE_NODE_ERROR)
                    zookeeper->removeRecursive(child_full_path);
                else if (ex.code() == ErrorCodes::RESHARDING_COORDINATOR_DELETED)
                    zookeeper->removeRecursive(child_full_path);
                else if (ex.code() == ErrorCodes::RWLOCK_NO_SUCH_LOCK)
                    zookeeper->removeRecursive(child_full_path);
                else if (ex.code() == ErrorCodes::NO_SUCH_BARRIER)
                    zookeeper->removeRecursive(child_full_path);
                else
                    zookeeper->removeRecursive(child_full_path);
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }

            throw;
        }
        catch (...)
        {
            zookeeper->removeRecursive(child_full_path);
            throw;
        }

        zookeeper->removeRecursive(child_full_path);
    }
}

void ReshardingWorker::perform(const std::string & job_descriptor, const std::string & job_name)
{
    LOG_DEBUG(log, "Performing resharding job.");

    current_job = ReshardingJob{job_descriptor};
    current_job.job_name = job_name;

    zkutil::RWLock deletion_lock;

    if (current_job.isCoordinated())
        deletion_lock = createDeletionLock(current_job.coordinator_id);

    zkutil::RWLock::Guard<zkutil::RWLock::Read, zkutil::RWLock::NonBlocking> guard{deletion_lock};
    if (!deletion_lock.ownsLock())
        throw Exception{"Coordinator has been deleted", ErrorCodes::RESHARDING_COORDINATOR_DELETED};

    StoragePtr generic_storage = context.getTable(current_job.database_name, current_job.table_name);
    auto & storage = typeid_cast<StorageReplicatedMergeTree &>(*generic_storage);
    current_job.storage = &storage;

    std::string dumped_coordinator_state;

    auto handle_exception = [&](const std::string & cancel_msg, const std::string & error_msg)
    {
        try
        {
            /// Cancellation has priority over errors.
            if (must_stop)
            {
                if (current_job.isCoordinated())
                {
                    setStatus(current_job.coordinator_id, getFQDNOrHostName(), STATUS_ON_HOLD,
                        cancel_msg);
                    dumped_coordinator_state = dumpCoordinatorState(current_job.coordinator_id);
                }
                softCleanup();
            }
            else
            {
                /// An error has occurred on this performer.
                if (current_job.isCoordinated())
                {
                    setStatus(current_job.coordinator_id, getFQDNOrHostName(), STATUS_ERROR,
                        error_msg);
                    dumped_coordinator_state = dumpCoordinatorState(current_job.coordinator_id);
                }
                deletion_lock.release();
                hardCleanup();
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    };

    try
    {
        initializeJob();

        {
            ScopedAnomalyMonitor scoped_anomaly_monitor{anomaly_monitor};

            /// Create the new sharded partitions. Upload them to their target
            /// shards. Collect into persistent storage all the information we
            /// need in order to commit changes.
            if (!isPublished())
            {
                createShardedPartitions();
                storeTargetShardsInfo();
                publishShardedPartitions();
                deleteTemporaryData();
                markAsPublished();
            }

            waitForUploadCompletion();

            /// If the current job is part of a distributed job, participate in a
            /// leader election among all the performers. This is required because
            /// of the following: when changes are applied on the target shards,
            /// all the performers drop their respective source partitions; in
            /// addition the leader sends all the required attach requests to the
            /// target shards.
            electLeader();

            /// Build into persistent storage a log consisting of a description
            /// of all the operations to be performed within the commit operation.
            if (!isLogCreated())
            {
                createLog();
                markLogAsCreated();
            }

            if (!isCommitted())
            {
                commit();
                markAsCommitted();
            }

            /// A distributed job is considered to be complete if and only if
            /// changes have been committed by all the performers.
            waitForCommitCompletion();
        }
    }
    catch (const zkutil::KeeperException & ex)
    {
        /// We are experiencing problems with ZooKeeper. Since we don't have any
        /// means to communicate with other nodes, we merely perform retries until
        /// ZooKeeper has come back online.
        try
        {
            softCleanup();
            /// Wake up the tracker thread.
            event->set();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        throw;
    }
    catch (const Exception & ex)
    {
        try
        {
            if (ex.code() == ErrorCodes::ABORTED)
            {
                LOG_DEBUG(log, "Resharding job cancelled.");
                /// A soft shutdown is being performed on this performer.
                /// Put the current distributed job on hold in order to reliably handle
                /// the scenario in which the remote performers undergo a hard shutdown.
                if (current_job.isCoordinated())
                {
                    setStatus(current_job.coordinator_id, getFQDNOrHostName(), STATUS_ON_HOLD,
                        ex.message());
                    dumped_coordinator_state = dumpCoordinatorState(current_job.coordinator_id);
                }
                softCleanup();
            }
            else if (ex.code() == ErrorCodes::RESHARDING_REMOTE_NODE_UNAVAILABLE)
            {
                /// A remote performer has gone offline or we are experiencing network problems.
                /// Put the current distributed job on hold. Also wake up the tracker thread
                /// so that it will come accross this distributed job even if no new jobs
                /// are submitted.
                if (current_job.isCoordinated())
                {
                    setStatus(current_job.coordinator_id, getFQDNOrHostName(), STATUS_ON_HOLD,
                        ex.message());
                    dumped_coordinator_state = dumpCoordinatorState(current_job.coordinator_id);
                }
                softCleanup();
                wakeUpTrackerThread();
            }
            else if (ex.code() == ErrorCodes::RESHARDING_REMOTE_NODE_ERROR)
            {
                dumped_coordinator_state = dumpCoordinatorState(current_job.coordinator_id);
                deletion_lock.release();
                hardCleanup();
            }
            else if (ex.code() == ErrorCodes::RESHARDING_COORDINATOR_DELETED)
            {
            }
            else if (ex.code() == ErrorCodes::RESHARDING_DISTRIBUTED_JOB_ON_HOLD)
            {
                /// The current distributed job is on hold and one or more required performers
                /// have not gone online yet. Wake up the tracker thread so that it will come
                /// accross this distributed job even if no new jobs are submitted.
                setStatus(current_job.coordinator_id, getFQDNOrHostName(), STATUS_ON_HOLD,
                    ex.message());
                dumped_coordinator_state = dumpCoordinatorState(current_job.coordinator_id);
                wakeUpTrackerThread();
            }
            else
                handle_exception(ex.message(), ex.message());
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        if (current_job.isCoordinated())
            LOG_ERROR(log, dumped_coordinator_state);
        throw;
    }
    catch (const std::exception & ex)
    {
        /// An error has occurred on this performer.
        handle_exception("Resharding job cancelled", ex.what());
        if (current_job.isCoordinated())
            LOG_ERROR(log, dumped_coordinator_state);
        throw;
    }
    catch (...)
    {
        /// An error has occurred on this performer.
        handle_exception("Resharding job cancelled", "An unspecified error has occurred");
        if (current_job.isCoordinated())
            LOG_ERROR(log, dumped_coordinator_state);
        throw;
    }

    deletion_lock.release();
    hardCleanup();

    LOG_DEBUG(log, "Resharding job successfully completed.");
}

void ReshardingWorker::createShardedPartitions()
{
    abortJobIfRequested();

    LOG_DEBUG(log, "Splitting partition shard-wise.");

    auto & storage = *(current_job.storage);

    MergeTreeDataMerger merger{storage.data, context.getBackgroundPool()};

    MergeTreeDataMerger::CancellationHook hook = std::bind(&ReshardingWorker::abortJobIfRequested, this);
    merger.setCancellationHook(hook);

    MergeTreeData::PerShardDataParts & per_shard_data_parts = storage.data.per_shard_data_parts;
    per_shard_data_parts = merger.reshardPartition(current_job);
}

void ReshardingWorker::storeTargetShardsInfo()
{
    LOG_DEBUG(log, "Storing info on target shards");

    auto & storage = *(current_job.storage);
    MergeTreeData::PerShardDataParts & per_shard_data_parts = storage.data.per_shard_data_parts;

    auto zookeeper = context.getZooKeeper();

    zookeeper->tryRemove(getLocalJobPath() + "/shards");

    std::string out;
    WriteBufferFromString buf{out};

    size_t entries_count = 0;
    for (const auto & entry : per_shard_data_parts)
    {
        const MergeTreeData::MutableDataPartPtr & part_from_shard = entry.second;
        if (!part_from_shard)
            continue;
        ++entries_count;
    }

    writeVarUInt(entries_count, buf);

    for (const auto & entry : per_shard_data_parts)
    {
        size_t shard_no = entry.first;
        const MergeTreeData::MutableDataPartPtr & part_from_shard = entry.second;
        if (!part_from_shard)
            continue;

        std::string part = storage.data.getFullPath() + "reshard/" + toString(shard_no) + "/" + part_from_shard->name;
        auto hash = computeHashFromPart(part);

        writeVarUInt(shard_no, buf);
        writeBinary(part_from_shard->name, buf);
        writeBinary(hash, buf);
    }

    buf.next();

    (void) zookeeper->create(getLocalJobPath() + "/shards", out,
        zkutil::CreateMode::Persistent);
}

ReshardingWorker::ShardList ReshardingWorker::getTargetShardsInfo(const std::string & hostname,
    const std::string & job_name)
{
    ShardList shard_list;

    auto zookeeper = context.getZooKeeper();

    auto shards = zookeeper->get(task_queue_path + hostname + "/" + job_name + "/shards");

    ReadBufferFromString buf{shards};

    size_t entries_count;
    readVarUInt(entries_count, buf);

    for (size_t i = 0; i < entries_count; ++i)
    {
        size_t shard_no;
        readVarUInt(shard_no, buf);

        std::string part_name;
        readBinary(part_name, buf);

        std::string hash;
        readBinary(hash, buf);

        shard_list.emplace_back(shard_no, part_name, hash);
    }

    return shard_list;
}

void ReshardingWorker::publishShardedPartitions()
{
    abortJobIfRequested();

    LOG_DEBUG(log, "Sending newly created partitions to their respective shards.");

    auto & storage = *(current_job.storage);
    auto zookeeper = context.getZooKeeper();

    struct TaskInfo
    {
        TaskInfo(const std::string & replica_path_,
            const std::string & part_,
            const ReplicatedMergeTreeAddress & dest_,
            size_t shard_no_)
            : replica_path(replica_path_), dest(dest_), part(part_),
            shard_no(shard_no_)
        {
        }

        TaskInfo(const TaskInfo &) = delete;
        TaskInfo & operator=(const TaskInfo &) = delete;

        TaskInfo(TaskInfo &&) = default;
        TaskInfo & operator=(TaskInfo &&) = default;

        std::string replica_path;
        ReplicatedMergeTreeAddress dest;
        std::string part;
        size_t shard_no;
    };

    using TaskInfoList = std::vector<TaskInfo>;
    TaskInfoList task_info_list;

    /// Copy new partitions to the replicas of corresponding shards.

    /// Number of participating local replicas. It should be <= 1.
    size_t local_count = 0;

    for (const auto & entry : storage.data.per_shard_data_parts)
    {
        size_t shard_no = entry.first;
        const MergeTreeData::MutableDataPartPtr & part_from_shard = entry.second;
        if (!part_from_shard)
            continue;

        const WeightedZooKeeperPath & weighted_path = current_job.paths[shard_no];
        const std::string & zookeeper_path = weighted_path.first;

        auto children = zookeeper->getChildren(zookeeper_path + "/replicas");
        for (const auto & child : children)
        {
            const std::string replica_path = zookeeper_path + "/replicas/" + child;
            auto host = zookeeper->get(replica_path + "/host");
            ReplicatedMergeTreeAddress host_desc(host);
            task_info_list.emplace_back(replica_path, part_from_shard->name, host_desc, shard_no);
            if (replica_path == storage.replica_path)
            {
                ++local_count;
                if (local_count > 1)
                    throw Exception{"Detected more than one local replica", ErrorCodes::LOGICAL_ERROR};
                std::swap(task_info_list[0], task_info_list[task_info_list.size() - 1]);
            }
        }
    }

    abortJobIfRequested();

    size_t remote_count = task_info_list.size() - local_count;

    ThreadPool pool(remote_count);

    using Tasks = std::vector<std::packaged_task<bool()>>;
    Tasks tasks(remote_count);

    ReplicatedMergeTreeAddress local_address{zookeeper->get(storage.replica_path + "/host")};
    InterserverIOEndpointLocation from_location{storage.replica_path, local_address.host, local_address.replication_port};

    ShardedPartitionUploader::Client::CancellationHook hook = std::bind(&ReshardingWorker::abortJobIfRequested, this);

    storage.sharded_partition_uploader_client.setCancellationHook(hook);

    try
    {
        for (size_t i = local_count; i < task_info_list.size(); ++i)
        {
            const TaskInfo & entry = task_info_list[i];
            const auto & replica_path = entry.replica_path;
            const auto & dest = entry.dest;
            const auto & part = entry.part;
            size_t shard_no = entry.shard_no;

            InterserverIOEndpointLocation to_location{replica_path, dest.host, dest.replication_port};

            size_t j = i - local_count;
            tasks[j] = Tasks::value_type{std::bind(&ShardedPartitionUploader::Client::send,
                &storage.sharded_partition_uploader_client, part, shard_no, to_location)};
            pool.schedule([j, &tasks]{ tasks[j](); });
        }
    }
    catch (const Poco::TimeoutException & ex)
    {
        try
        {
            pool.wait();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        throw Exception{"Sharded partition upload operation timed out",
            ErrorCodes::RESHARDING_REMOTE_NODE_UNAVAILABLE};
    }
    catch (...)
    {
        try
        {
            pool.wait();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        if (!current_job.is_aborted)
        {
            /// We may have caught an error because one or more remote performers have
            /// gone offline while performing I/O. The following check is here to sort
            /// out this ambiguity.
            abortJobIfRequested();
        }

        throw;
    }

    pool.wait();

    for (auto & task : tasks)
    {
        bool res = task.get_future().get();
        if (!res)
            throw Exception{"Failed to copy partition", ErrorCodes::PARTITION_COPY_FAILED};
    }

    abortJobIfRequested();

    if (local_count == 1)
    {
        /// On the local replica, simply move the sharded partition to the `detached/` folder.
        const TaskInfo & entry = task_info_list[0];
        const auto & part = entry.part;
        size_t shard_no = entry.shard_no;

        std::string from_path = storage.full_path + "reshard/" + toString(shard_no) + "/" + part + "/";
        std::string to_path = storage.full_path + "detached/";
        Poco::File{from_path}.moveTo(to_path);
    }
}

void ReshardingWorker::commit()
{
    /// Note: we never rollback any change. After having recovered from an abnormal
    /// situation, we attempt to apply all the pending changes.

    auto zookeeper = context.getZooKeeper();

    auto log_path = getLocalJobPath() + "/log";

    std::vector<LogRecord> log_records;

    auto children = zookeeper->getChildren(log_path);
    if (children.empty())
        return;

    LOG_DEBUG(log, "Committing changes.");

    std::sort(children.begin(), children.end());

    for (const auto & child : children)
        log_records.emplace_back(zookeeper, log_path + "/" + child);

    /// Find defective log records and repair them.
    for (LogRecord & log_record : log_records)
    {
        if (log_record.state == LogRecord::RUNNING)
            repairLogRecord(log_record);
    }

    size_t operation_count = 0;
    for (const LogRecord & log_record : log_records)
    {
        if (log_record.state == LogRecord::READY)
            ++operation_count;
    }

    if (operation_count == 0)
    {
        /// All the operations have already been performed.
        return;
    }

    if (!current_job.do_copy)
    {
        /// If the keyword COPY is not specified, we have drop operations. They are
        /// always performed first. This is to prevent a name clash if an attach
        /// operation should ever happen to run locally.
        if (log_records[0].operation != LogRecord::OP_DROP)
            throw Exception{"Ill-formed log", ErrorCodes::RESHARDING_ILL_FORMED_LOG};

        if (log_records[0].state == LogRecord::READY)
            executeLogRecord(log_records[0]);

        --operation_count;

        if (operation_count == 0)
        {
            /// The drop operation was the only task to be performed.
            return;
        }
    }

    /// Execute all the remaining log records.

    size_t pool_size = operation_count;
    ThreadPool pool(pool_size);

    using Tasks = std::vector<std::packaged_task<void()>>;
    Tasks tasks(pool_size);

    try
    {
        size_t j = 0;
        /// Note: yes, i = 0 is correct indeed since, if we have just performed
        /// a drop, it won't be run again. Maintaining a supplementary variable
        /// to keep track of what we have already done is definitely not worth
        /// the price.
        for (size_t i = 0; i < log_records.size(); ++i)
        {
            if (log_records[i].state == LogRecord::READY)
            {
                if (operation_count == 0)
                    throw Exception{"ReshardingWorker: found discrepancy while committing",
                        ErrorCodes::LOGICAL_ERROR};

                tasks[j] = Tasks::value_type{std::bind(&ReshardingWorker::executeLogRecord, this,
                    log_records[i])};
                pool.schedule([j, &tasks]{ tasks[j](); });
                ++j;
                --operation_count;
            }
        }
    }
    catch (const Poco::TimeoutException & ex)
    {
        try
        {
            pool.wait();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        throw Exception{"A remote operation timed out while committing",
            ErrorCodes::RESHARDING_REMOTE_NODE_UNAVAILABLE};
    }
    catch (...)
    {
        try
        {
            pool.wait();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        throw;
    }

    pool.wait();

    LOG_DEBUG(log, "Changes successfully committed.");
}

void ReshardingWorker::repairLogRecord(LogRecord & log_record)
{
    bool found = true;

    if (log_record.operation == LogRecord::OP_DROP)
    {
        /// We make the following conservative assumptions:
        /// 1. If there is no partition with the source partition name, the source partition
        /// has already been dropped.
        /// 2. If there is such a partition but it has changed since we built from it
        /// new sharded partitions, we cannot drop it since it could result in data loss.
        auto & storage = *(current_job.storage);

        if (storage.data.getPartitionSize(current_job.partition) == 0)
            found = false;
        else
        {
            /// XXX Disabled this check because, in order to make it reliable,
            /// we should disable merging for this partition. This is not a good
            /// idea anyway.
            /// In a a future release, we should implement a fence that would,
            /// for a given partition, separate the data that are to be resharded,
            /// from the data that are not to be touched. The tricky part is to
            /// get it consistent on *each* replica (e.g. imagine that a resharding
            /// operation starts while an INSERT operation is not fully replicated
            /// on all the replicas).
            /// For now we make the assumption that no write operations happen
            /// while resharding a partition.
#if 0
            auto current_hash = computeHashFromPartition(storage.data.getFullPath(),
                current_job.partition);
            if (current_hash != log_record.partition_hash)
            {
                LOG_WARNING(log, "The source partition " << current_job.partition
                    << " cannot be dropped because it has changed since the last"
                    " time we were online");
                found = false;
            }
#endif
        }
    }
    else if (log_record.operation == LogRecord::OP_ATTACH)
        found = checkAttachLogRecord(log_record);
    else
        throw Exception{"Ill-formed log", ErrorCodes::RESHARDING_ILL_FORMED_LOG};

    if (!found)
    {
        /// The operation was completed.
        log_record.state = LogRecord::DONE;
    }
    else
    {
        /// The operation was not performed. Do it again.
        log_record.state = LogRecord::READY;
    }

    log_record.writeBack();
}

void ReshardingWorker::executeLogRecord(LogRecord & log_record)
{
    if (log_record.operation == LogRecord::OP_DROP)
        executeDrop(log_record);
    else if (log_record.operation == LogRecord::OP_ATTACH)
        executeAttach(log_record);
    else
        throw Exception{"Ill-formed log", ErrorCodes::RESHARDING_ILL_FORMED_LOG};
}

void ReshardingWorker::executeDrop(LogRecord & log_record)
{
    log_record.state = LogRecord::RUNNING;
    log_record.writeBack();

    /// Locally drop the source partition.
    std::string query_str = "ALTER TABLE " + current_job.database_name + "."
        + current_job.table_name + " DROP PARTITION " + current_job.partition;
    (void) executeQuery(query_str, context, true);

    log_record.state = LogRecord::DONE;
    log_record.writeBack();
}

bool ReshardingWorker::checkAttachLogRecord(LogRecord & log_record)
{
    /// We check that all the replicas of the shard that must receive an attach
    /// request store all the required detached partitions. Moreover we verify
    /// that these detached partitions have not changed. We conservatively
    /// assume that, if this check fails, the attach request cannot be applied.

    auto & storage = *(current_job.storage);
    auto zookeeper = context.getZooKeeper();

    struct TaskInfo
    {
        TaskInfo(const std::string & replica_path_,
            const ReplicatedMergeTreeAddress & dest_,
            const std::string & part_, const std::string & hash_)
            : replica_path(replica_path_), dest(dest_),
            part(part_), hash(hash_)
        {
        }

        TaskInfo(const TaskInfo &) = delete;
        TaskInfo & operator=(const TaskInfo &) = delete;

        TaskInfo(TaskInfo &&) = default;
        TaskInfo & operator=(TaskInfo &&) = default;

        std::string replica_path;
        ReplicatedMergeTreeAddress dest;
        std::string part;
        std::string hash;
    };

    using TaskInfoList = std::vector<TaskInfo>;
    TaskInfoList task_info_list;

    const WeightedZooKeeperPath & weighted_path = current_job.paths[log_record.shard_no];
    const std::string & zookeeper_path = weighted_path.first;

    auto children = zookeeper->getChildren(zookeeper_path + "/replicas");
    for (const auto & child : children)
    {
        const std::string & replica_path = zookeeper_path + "/replicas/" + child;
        auto host = zookeeper->get(replica_path + "/host");
        ReplicatedMergeTreeAddress host_desc{host};

        for (const auto & entry : log_record.parts_with_hash)
        {
            const auto & part = entry.first;
            const auto & hash = entry.second;
            task_info_list.emplace_back(replica_path, host_desc, part, hash);
        }
    }

    ThreadPool pool(task_info_list.size());

    using Tasks = std::vector<std::packaged_task<RemotePartChecker::Status()>>;
    Tasks tasks(task_info_list.size());

    try
    {
        for (size_t i = 0; i < task_info_list.size(); ++i)
        {
            const TaskInfo & entry = task_info_list[i];
            const auto & replica_path = entry.replica_path;
            const auto & dest = entry.dest;
            const auto & part = entry.part;
            const auto & hash = entry.hash;

            InterserverIOEndpointLocation to_location{replica_path, dest.host, dest.replication_port};

            tasks[i] = Tasks::value_type{std::bind(&RemotePartChecker::Client::check,
                &storage.remote_part_checker_client, part, hash, to_location)};
            pool.schedule([i, &tasks]{ tasks[i](); });
        }
    }
    catch (const Poco::TimeoutException & ex)
    {
        try
        {
            pool.wait();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        throw Exception{"Part checking on remote node timed out while attempting "
            "to fix a failed ATTACH operation",
            ErrorCodes::RESHARDING_REMOTE_NODE_UNAVAILABLE};
    }
    catch (...)
    {
        try
        {
            pool.wait();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        throw;
    }

    pool.wait();

    bool may_perform_attach = true;

    for (size_t i = 0; i < tasks.size(); ++i)
    {
        RemotePartChecker::Status status = tasks[i].get_future().get();
        if (status != RemotePartChecker::Status::OK)
        {
            may_perform_attach = false;
            const TaskInfo & entry = task_info_list[i];

            if (status == RemotePartChecker::Status::INCONSISTENT)
                LOG_WARNING(log, "Cannot attach sharded partition " << current_job.partition
                    << " on host " << entry.dest.host << " because some changes have occurred "
                    " in part " << entry.part << " since the last time we were online.");
            else if (status == RemotePartChecker::Status::ERROR)
                LOG_WARNING(log, "Cannot attach sharded partition " << current_job.partition
                    << " on host " << entry.dest.host << " because an unexpected error "
                    << " was triggered while trying to check part " << entry.part);
        }
    }

    return may_perform_attach;
}

void ReshardingWorker::executeAttach(LogRecord & log_record)
{
    auto & storage = *(current_job.storage);
    auto zookeeper = context.getZooKeeper();

    /// Description of a task on a replica.
    struct TaskInfo
    {
        TaskInfo(const std::string & replica_path_, const ReplicatedMergeTreeAddress & dest_)
            : replica_path(replica_path_), dest(dest_)
        {
        }

        TaskInfo(const TaskInfo &) = delete;
        TaskInfo & operator=(const TaskInfo &) = delete;

        TaskInfo(TaskInfo &&) = default;
        TaskInfo & operator=(TaskInfo &&) = default;

        std::string replica_path;
        ReplicatedMergeTreeAddress dest;
    };

    /// Description of tasks for each replica of a shard.
    /// For fault tolerance purposes, some fields are provided
    /// to perform attempts on more than one replica if needed.
    struct ShardTaskInfo
    {
        ShardTaskInfo()
        {
            rng = std::mt19937(randomSeed());
        }

        ShardTaskInfo(const ShardTaskInfo &) = delete;
        ShardTaskInfo & operator=(const ShardTaskInfo &) = delete;

        ShardTaskInfo(ShardTaskInfo &&) = default;
        ShardTaskInfo & operator=(ShardTaskInfo &&) = default;

        /// one task for each replica
        std::vector<TaskInfo> shard_tasks;
        /// index to the replica to be used
        size_t next = 0;
        /// result of the operation on the current replica
        bool is_success = false;
        /// For pseudo-random number generation.
        std::mt19937 rng;
    };

    const WeightedZooKeeperPath & weighted_path = current_job.paths[log_record.shard_no];
    const std::string & zookeeper_path = weighted_path.first;

    ShardTaskInfo shard_task_info;

    auto children = zookeeper->getChildren(zookeeper_path + "/replicas");
    for (const auto & child : children)
    {
        const std::string replica_path = zookeeper_path + "/replicas/" + child;
        auto host = zookeeper->get(replica_path + "/host");
        ReplicatedMergeTreeAddress host_desc(host);

        shard_task_info.shard_tasks.emplace_back(replica_path, host_desc);
    }

    log_record.state = LogRecord::RUNNING;
    log_record.writeBack();

    while (true)
    {
        /// Randomly choose a replica on which to perform the operation.
        long int rand_res = shard_task_info.rng();
        size_t current = shard_task_info.next + rand_res % (shard_task_info.shard_tasks.size() - shard_task_info.next);
        std::swap(shard_task_info.shard_tasks[shard_task_info.next], shard_task_info.shard_tasks[current]);
        ++shard_task_info.next;

        TaskInfo & cur_task_shard_task_info = shard_task_info.shard_tasks[shard_task_info.next - 1];

        const auto & replica_path = cur_task_shard_task_info.replica_path;
        const auto & dest = cur_task_shard_task_info.dest;

        /// Run the operation.

        InterserverIOEndpointLocation location(replica_path, dest.host, dest.replication_port);

        std::string query_str = "ALTER TABLE " + dest.database + "."
            + dest.table + " ATTACH PARTITION " + current_job.partition;

        bool res = storage.remote_query_executor_client.executeQuery(location, query_str);
        if (res)
            break;
        else if (shard_task_info.next == shard_task_info.shard_tasks.size())
        {
            /// No more attempts are possible.
            throw Exception{"Failed to attach partition on shard",
                ErrorCodes::PARTITION_ATTACH_FAILED};
        }
    }

    log_record.state = LogRecord::DONE;
    log_record.writeBack();
}

void ReshardingWorker::electLeader()
{
    /// If we are not a distributed job, do nothing since we are obviously the
    /// leader. Otherwise each performer of the distributed job creates an ephemeral
    /// sequential znode onto ZooKeeper persistent storage. When all the performers
    /// have entered the game, i.e. the election barrier is released, the winner
    /// is the performer having the znode with the lowest ID.
    /// Then one of the nodes publishes this piece of information as a new znode.
    ///
    /// In case of failure this election scheme is guaranteed to always succeed:
    ///
    /// 1. If one performer experiences a failure, it will eventually get winner
    /// information if another performer was able to publish it.
    ///
    /// 2. If all the performers experience a failure before any of them could publish
    /// winner information, the election is simply re-run. This is possible since
    /// SingleBarrier objects may be, by design, crossed again after release.
    ///
    /// 3. If two performers A, B get inconsistent winner information because of the
    /// failure of a third performer C (e.g. A determines that C is the winner, but
    /// then, C fails; consequently the corresponding znode disappears; then B
    /// determines that A is the winner), save for any failure, either A or B
    /// will succeed to publish first its information. That is indeed all what
    /// we need.

    if (!current_job.isCoordinated())
        return;

    LOG_DEBUG(log, "Performing leader election");

    auto leader = getPartitionPath() + "/leader";
    auto election_path = getPartitionPath() + "/leader_election";

    auto zookeeper = context.getZooKeeper();

    if (!zookeeper->exists(leader))
    {
        zookeeper->create(election_path + "/node-", getFQDNOrHostName(),
            zkutil::CreateMode::EphemeralSequential);

        waitForElectionCompletion();

        auto nodes = zookeeper->getChildren(election_path);
        std::sort(nodes.begin(), nodes.end());
        auto winner = zookeeper->get(election_path + "/" + nodes.front());

        zookeeper->createIfNotExists(leader, winner);
    }
}

bool ReshardingWorker::isLeader()
{
    if (!current_job.isCoordinated())
        return true;

    auto zookeeper = context.getZooKeeper();
    return zookeeper->get(getPartitionPath() + "/leader") == getFQDNOrHostName();
}

void ReshardingWorker::createLog()
{
    LOG_DEBUG(log, "Creating log");

    //auto & storage = *(current_job.storage);
    auto zookeeper = context.getZooKeeper();

    auto log_path = getLocalJobPath() + "/log";

    if (zookeeper->exists(log_path))
    {
        /// An abnormal condition occurred while creating the log the last time
        /// we were online. Therefore we assume that it must be garbage.
        zookeeper->removeRecursive(log_path);
    }

    (void) zookeeper->create(log_path, "", zkutil::CreateMode::Persistent);

    /// If the keyword COPY is not specified, a drop request is performed on
    /// each performer.
    if (!current_job.do_copy)
    {
        LogRecord log_record{zookeeper};
        log_record.operation = LogRecord::OP_DROP;
        log_record.partition = current_job.partition;
        /// Disabled. See comment in repairLogRecord().
#if 0
        log_record.partition_hash = computeHashFromPartition(storage.data.getFullPath(),
            current_job.partition);
#endif
        log_record.state = LogRecord::READY;

        log_record.enqueue(log_path);
    }

    if (!isLeader())
        return;

    /// The leader performs all the attach requests on the target shards.

    std::unordered_map<size_t, ShardList> shard_to_info;

    if (current_job.isCoordinated())
    {
        auto nodes = zookeeper->getChildren(getPartitionPath() + "/nodes");
        for (const auto & node : nodes)
        {
            auto job_name = zookeeper->get(getPartitionPath() + "/nodes/" + node);
            ShardList shards_from_node = getTargetShardsInfo(node, job_name);
            for (const TargetShardInfo & shard_info : shards_from_node)
                shard_to_info[shard_info.shard_no].push_back(shard_info);
        }
    }
    else
    {
        ShardList shards_from_node = getTargetShardsInfo(getFQDNOrHostName(), current_job.job_name);
        for (const TargetShardInfo & shard_info : shards_from_node)
            shard_to_info[shard_info.shard_no].push_back(shard_info);
    }

    for (const auto & entry : shard_to_info)
    {
        size_t shard_no = entry.first;
        const ShardList & shard_list = entry.second;

        LogRecord log_record{zookeeper};
        log_record.operation = LogRecord::OP_ATTACH;
        log_record.partition = current_job.partition;
        log_record.shard_no = shard_no;
        log_record.state = LogRecord::READY;

        for (const TargetShardInfo & info : shard_list)
            log_record.parts_with_hash.emplace(info.part_name, info.hash);

        log_record.enqueue(log_path);
    }
}

void ReshardingWorker::softCleanup()
{
    LOG_DEBUG(log, "Performing soft cleanup.");
    deleteTemporaryData();
    current_job.clear();
}

void ReshardingWorker::hardCleanup()
{
    LOG_DEBUG(log, "Performing cleanup.");
    deleteTemporaryData();
    finalizeJob();
    current_job.clear();
}

void ReshardingWorker::deleteTemporaryData()
{
    auto & storage = *(current_job.storage);
    storage.data.per_shard_data_parts.clear();

    if (Poco::File{storage.full_path + "/reshard"}.exists())
    {
        Poco::DirectoryIterator end;
        for (Poco::DirectoryIterator it(storage.full_path + "/reshard"); it != end; ++it)
        {
            auto absolute_path = it.path().absolute().toString();
            Poco::File{absolute_path}.remove(true);
        }
    }
}

std::string ReshardingWorker::createCoordinator(const Cluster & cluster)
{
    const std::string cluster_name = cluster.getHashOfAddresses();
    auto zookeeper = context.getZooKeeper();

    auto lock = getGlobalLock();
    zkutil::RWLock::Guard<zkutil::RWLock::Write> guard{lock};

    auto coordinators = zookeeper->getChildren(coordination_path);
    for (const auto & coordinator : coordinators)
    {
        auto effective_cluster_name = zookeeper->get(coordination_path + "/" + coordinator + "/cluster");
        if (effective_cluster_name == cluster_name)
            throw Exception{"The cluster specified for this table is currently busy with another "
                "distributed job. Please try later", ErrorCodes::RESHARDING_BUSY_CLUSTER};
    }

    std::string coordinator_id = zookeeper->create(coordination_path + "/coordinator-", "",
        zkutil::CreateMode::PersistentSequential);
    coordinator_id = coordinator_id.substr(coordination_path.length() + 1);

    (void) zookeeper->create(getCoordinatorPath(coordinator_id) + "/lock",
        "", zkutil::CreateMode::Persistent);

    (void) zookeeper->create(getCoordinatorPath(coordinator_id) + "/deletion_lock",
        "", zkutil::CreateMode::Persistent);

    (void) zookeeper->create(getCoordinatorPath(coordinator_id) + "/cluster",
        cluster_name, zkutil::CreateMode::Persistent);

    (void) zookeeper->create(getCoordinatorPath(coordinator_id) + "/increment",
        "0", zkutil::CreateMode::Persistent);

    (void) zookeeper->create(getCoordinatorPath(coordinator_id) + "/node_count",
        toString(cluster.getRemoteShardCount() + cluster.getLocalShardCount()),
        zkutil::CreateMode::Persistent);

    (void) zookeeper->create(getCoordinatorPath(coordinator_id) + "/shards",
        "", zkutil::CreateMode::Persistent);

    (void) zookeeper->create(getCoordinatorPath(coordinator_id) + "/status",
        toString(static_cast<UInt64>(STATUS_OK)), zkutil::CreateMode::Persistent);

    (void) zookeeper->create(getCoordinatorPath(coordinator_id) + "/status_probe",
        "", zkutil::CreateMode::Persistent);

    (void) zookeeper->create(getCoordinatorPath(coordinator_id) + "/partitions",
        "", zkutil::CreateMode::Persistent);

    /// Register the addresses, IP and hostnames, of all the nodes of the cluster.

    (void) zookeeper->create(getCoordinatorPath(coordinator_id) + "/cluster_addresses",
        "", zkutil::CreateMode::Persistent);

    auto publish_address = [&](const std::string & host, size_t shard_no)
    {
        int32_t code = zookeeper->tryCreate(getCoordinatorPath(coordinator_id) + "/cluster_addresses/"
            + host, toString(shard_no), zkutil::CreateMode::Persistent);
        if ((code != ZOK) && (code != ZNODEEXISTS))
            throw zkutil::KeeperException{code};
    };

    if (!cluster.getShardsAddresses().empty())
    {
        size_t shard_no = 0;
        for (const auto & address : cluster.getShardsAddresses())
        {
            publish_address(address.host_name, shard_no);
            publish_address(address.resolved_address.host().toString(), shard_no);
            ++shard_no;
        }
    }
    else if (!cluster.getShardsWithFailoverAddresses().empty())
    {
        size_t shard_no = 0;
        for (const auto & addresses : cluster.getShardsWithFailoverAddresses())
        {
            for (const auto & address : addresses)
            {
                publish_address(address.host_name, shard_no);
                publish_address(address.resolved_address.host().toString(), shard_no);
            }
            ++shard_no;
        }
    }
    else
        throw Exception{"ReshardingWorker: ill-formed cluster", ErrorCodes::LOGICAL_ERROR};

    return coordinator_id;
}

void ReshardingWorker::registerQuery(const std::string & coordinator_id, const std::string & query)
{
    auto zookeeper = context.getZooKeeper();
    (void) zookeeper->create(getCoordinatorPath(coordinator_id) + "/query_hash",
        computeHashFromString(query), zkutil::CreateMode::Persistent);
}

void ReshardingWorker::deleteCoordinator(const std::string & coordinator_id)
{
    /// We don't acquire a scoped lock because we delete everything including this lock.
    auto deletion_lock = createDeletionLock(coordinator_id);
    deletion_lock.acquireWrite(zkutil::RWLock::Blocking);

    auto zookeeper = context.getZooKeeper();
    if (zookeeper->exists(getCoordinatorPath(coordinator_id)))
        zookeeper->removeRecursive(getCoordinatorPath(coordinator_id));
}

UInt64 ReshardingWorker::subscribe(const std::string & coordinator_id, const std::string & query)
{
    auto zookeeper = context.getZooKeeper();

    if (!zookeeper->exists(getCoordinatorPath(coordinator_id)))
        throw Exception{"Coordinator " + coordinator_id + " does not exist",
        ErrorCodes::RESHARDING_NO_SUCH_COORDINATOR};

    auto current_host = getFQDNOrHostName();

    /// Make sure that this shard is not busy in another distributed job.
    {
        auto lock = getGlobalLock();
        zkutil::RWLock::Guard<zkutil::RWLock::Read> guard{lock};

        auto coordinators = zookeeper->getChildren(coordination_path);
        for (const auto & coordinator : coordinators)
        {
            if (coordinator == coordinator_id)
                continue;

            auto cluster_addresses = zookeeper->getChildren(coordination_path + "/" + coordinator
                + "/cluster_addresses");
            if (std::find(cluster_addresses.begin(), cluster_addresses.end(), current_host)
                != cluster_addresses.end())
                throw Exception{"This shard is already busy with another distributed job",
                    ErrorCodes::RESHARDING_BUSY_SHARD};
        }
    }

    UInt64 block_number;

    {
        auto lock = getCoordinatorLock(coordinator_id);
        zkutil::RWLock::Guard<zkutil::RWLock::Write> guard{lock};

        /// Make sure that the query ALTER TABLE RESHARD with the "COORDINATE WITH" tag
        /// is not bogus.

        auto cluster_addresses = zookeeper->getChildren(getCoordinatorPath(coordinator_id)
            + "/cluster_addresses");
        if (std::find(cluster_addresses.begin(), cluster_addresses.end(), current_host)
            == cluster_addresses.end())
            throw Exception{"This host is not allowed to subscribe to coordinator "
                + coordinator_id,
                ErrorCodes::RESHARDING_NO_COORDINATOR_MEMBERSHIP};

        /// Check that the coordinator recognizes our query.
        auto query_hash = zookeeper->get(getCoordinatorPath(coordinator_id) + "/query_hash");
        if (computeHashFromString(query) != query_hash)
            throw Exception{"Coordinator " + coordinator_id + " does not handle this query",
                ErrorCodes::RESHARDING_INVALID_QUERY};

        /// Access granted. Now perform subscription.
        auto my_shard_no = zookeeper->get(getCoordinatorPath(coordinator_id) + "/cluster_addresses/"
            + current_host);
        int32_t code = zookeeper->tryCreate(getCoordinatorPath(coordinator_id) + "/shards/"
            + my_shard_no, "", zkutil::CreateMode::Persistent);
        if (code == ZNODEEXISTS)
            throw Exception{"This shard has already subscribed to coordinator " + coordinator_id,
                ErrorCodes::RESHARDING_ALREADY_SUBSCRIBED};
        else if (code != ZOK)
            throw zkutil::KeeperException{code};

        zookeeper->create(getCoordinatorPath(coordinator_id) + "/status/"
            + current_host, Status(STATUS_OK, "").toString(), zkutil::CreateMode::Persistent);

        /// Assign a unique block number to the current performer. We will use it in order
        /// to avoid any possible conflict when uploading resharded partitions.
        auto current_block_number = zookeeper->get(getCoordinatorPath(coordinator_id) + "/increment");
        block_number = std::stoull(current_block_number);
        zookeeper->set(getCoordinatorPath(coordinator_id) + "/increment", toString(block_number + 1));
    }

    return block_number;
}

void ReshardingWorker::unsubscribe(const std::string & coordinator_id)
{
    /// Note: we don't remove this shard from the /shards znode because
    /// it can subscribe to a distributed job only if its cluster is not
    /// currently busy with any distributed job.

    auto zookeeper = context.getZooKeeper();

    auto lock = getCoordinatorLock(coordinator_id);
    zkutil::RWLock::Guard<zkutil::RWLock::Write> guard{lock};

    auto current_host = getFQDNOrHostName();
    zookeeper->remove(getCoordinatorPath(coordinator_id) + "/status/" + current_host);

    auto node_count = zookeeper->get(getCoordinatorPath(coordinator_id) + "/node_count");
    UInt64 cur_node_count = std::stoull(node_count);
    if (cur_node_count == 0)
        throw Exception{"ReshardingWorker: invalid node count", ErrorCodes::LOGICAL_ERROR};
    zookeeper->set(getCoordinatorPath(coordinator_id) + "/node_count", toString(cur_node_count - 1));
}

void ReshardingWorker::addPartitions(const std::string & coordinator_id,
    const PartitionList & partition_list)
{
    auto zookeeper = context.getZooKeeper();

    auto lock = getCoordinatorLock(coordinator_id);
    zkutil::RWLock::Guard<zkutil::RWLock::Write> guard{lock};

    auto current_host = getFQDNOrHostName();

    for (const auto & partition : partition_list)
    {
        auto partition_path = getCoordinatorPath(coordinator_id) + "/partitions/" + partition;

        auto nodes_path = partition_path + "/nodes/";
        zookeeper->createAncestors(nodes_path);
        (void) zookeeper->create(nodes_path + current_host, "", zkutil::CreateMode::Persistent);

        zookeeper->createAncestors(partition_path + "/leader_election/");
    }
}

ReshardingWorker::PartitionList::iterator ReshardingWorker::categorizePartitions(const std::string & coordinator_id,
    PartitionList & partition_list)
{
    auto current_host = getFQDNOrHostName();
    auto zookeeper = context.getZooKeeper();

    auto is_coordinated = [&](const std::string & partition)
    {
        auto path = getCoordinatorPath(coordinator_id) + "/partitions/" + partition + "/nodes";
        auto nodes = zookeeper->getChildren(path);
        if ((nodes.size() == 1) && (nodes[0] == current_host))
        {
            zookeeper->removeRecursive(getCoordinatorPath(coordinator_id) + "/partitions/" + partition);
            return false;
        }
        else
            return true;
    };

    int size = partition_list.size();
    int i = -1;
    int j = size;

    {
        auto lock = getCoordinatorLock(coordinator_id);
        zkutil::RWLock::Guard<zkutil::RWLock::Write> guard{lock};

        while (true)
        {
            do
            {
                ++i;
            }
            while ((i < j) && (is_coordinated(partition_list[i])));

            if (i >= j)
                break;

            do
            {
                --j;
            }
            while ((i < j) && (!is_coordinated(partition_list[j])));

            if (i >= j)
                break;

            std::swap(partition_list[i], partition_list[j]);
        };
    }

    auto uncoordinated_begin = std::next(partition_list.begin(), j);

    std::sort(partition_list.begin(), uncoordinated_begin);
    std::sort(uncoordinated_begin, partition_list.end());

    return uncoordinated_begin;
}

size_t ReshardingWorker::getPartitionCount(const std::string & coordinator_id)
{
    auto lock = getCoordinatorLock(coordinator_id);
    zkutil::RWLock::Guard<zkutil::RWLock::Read> guard{lock};

    return getPartitionCountUnlocked(coordinator_id);
}

size_t ReshardingWorker::getPartitionCountUnlocked(const std::string & coordinator_id)
{
    auto zookeeper = context.getZooKeeper();
    return zookeeper->getChildren(getCoordinatorPath(coordinator_id) + "/partitions").size();
}

size_t ReshardingWorker::getNodeCount(const std::string & coordinator_id)
{
    auto lock = getCoordinatorLock(coordinator_id);
    zkutil::RWLock::Guard<zkutil::RWLock::Read> guard{lock};

    auto zookeeper = context.getZooKeeper();
    auto count = zookeeper->get(getCoordinatorPath(coordinator_id) + "/node_count");
    return std::stoull(count);
}

void ReshardingWorker::waitForCheckCompletion(const std::string & coordinator_id)
{
    /// Since we get the information about all the shards only after
    /// having crosssed this barrier, we set up a timeout for safety
    /// purposes.
    auto timeout = context.getSettingsRef().resharding_barrier_timeout;
    getCheckBarrier(coordinator_id).enter(timeout);
}

void ReshardingWorker::waitForOptOutCompletion(const std::string & coordinator_id, size_t count)
{
    getOptOutBarrier(coordinator_id, count).enter();
}

void ReshardingWorker::setStatus(const std::string & coordinator_id, StatusCode status,
    const std::string & msg)
{
    auto zookeeper = context.getZooKeeper();
    zookeeper->set(getCoordinatorPath(coordinator_id) + "/status", Status(status, msg).toString());
    zookeeper->set(getCoordinatorPath(coordinator_id) + "/status_probe", "");
}

void ReshardingWorker::setStatus(const std::string & coordinator_id, const std::string & hostname,
    StatusCode status, const std::string & msg)
{
    auto zookeeper = context.getZooKeeper();
    zookeeper->set(getCoordinatorPath(coordinator_id) + "/status/" + hostname,
        Status(status, msg).toString());
    zookeeper->set(getCoordinatorPath(coordinator_id) + "/status_probe", "");
}

bool ReshardingWorker::detectOfflineNodes(const std::string & coordinator_id)
{
    return detectOfflineNodesCommon(getCoordinatorPath(coordinator_id) + "/status", coordinator_id);
}

bool ReshardingWorker::detectOfflineNodes()
{
    return detectOfflineNodesCommon(getPartitionPath() + "/nodes", current_job.coordinator_id);
}

bool ReshardingWorker::detectOfflineNodesCommon(const std::string & path, const std::string & coordinator_id)
{
    auto zookeeper = context.getZooKeeper();

    auto nodes = zookeeper->getChildren(path);
    std::sort(nodes.begin(), nodes.end());

    auto online = zookeeper->getChildren(distributed_path + "/online");
    std::sort(online.begin(), online.end());

    std::vector<std::string> offline(nodes.size());
    auto end = std::set_difference(nodes.begin(), nodes.end(),
        online.begin(), online.end(), offline.begin());
    offline.resize(end - offline.begin());

    if (!offline.empty())
    {
        for (const auto & node : offline)
            zookeeper->set(getCoordinatorPath(coordinator_id) + "/status/" + node,
                Status(STATUS_ON_HOLD, "Node has gone offline").toString());
        zookeeper->set(getCoordinatorPath(coordinator_id) + "/status_probe", "");
    }

    return !offline.empty();
}

bool ReshardingWorker::isPublished()
{
    auto zookeeper = context.getZooKeeper();
    return zookeeper->exists(getLocalJobPath() + "/is_published");
}

void ReshardingWorker::markAsPublished()
{
    auto zookeeper = context.getZooKeeper();
    (void) zookeeper->create(getLocalJobPath() + "/is_published",
        "", zkutil::CreateMode::Persistent);
}

bool ReshardingWorker::isLogCreated()
{
    auto zookeeper = context.getZooKeeper();
    return zookeeper->exists(getLocalJobPath() + "/is_log_created");
}

void ReshardingWorker::markLogAsCreated()
{
    auto zookeeper = context.getZooKeeper();
    (void) zookeeper->create(getLocalJobPath() + "/is_log_created",
        "", zkutil::CreateMode::Persistent);
}

bool ReshardingWorker::isCommitted()
{
    auto zookeeper = context.getZooKeeper();
    return zookeeper->exists(getLocalJobPath() + "/is_committed");
}

void ReshardingWorker::markAsCommitted()
{
    auto zookeeper = context.getZooKeeper();
    (void) zookeeper->create(getLocalJobPath() + "/is_committed",
        "", zkutil::CreateMode::Persistent);
}

ReshardingWorker::StatusCode ReshardingWorker::getCoordinatorStatus(const std::string & coordinator_id)
{
    return getStatusCommon(getCoordinatorPath(coordinator_id) + "/status", coordinator_id);
}

ReshardingWorker::StatusCode ReshardingWorker::getStatus()
{
    return getStatusCommon(getPartitionPath() + "/nodes", current_job.coordinator_id);
}

ReshardingWorker::StatusCode ReshardingWorker::getStatusCommon(const std::string & path, const std::string & coordinator_id)
{
    /// Note: we don't need any synchronization for the status.
    /// That's why we don't acquire any read/write lock.
    /// All the operations are either reads or idempotent writes.

    auto zookeeper = context.getZooKeeper();

    auto coordinator_status = Status(zookeeper->get(getCoordinatorPath(coordinator_id) + "/status")).getCode();

    if (coordinator_status != STATUS_OK)
        return coordinator_status;

    (void) detectOfflineNodesCommon(path, coordinator_id);

    auto nodes = zookeeper->getChildren(path);

    bool has_error = false;
    bool has_on_hold = false;

    /// Determine the status.
    for (const auto & node : nodes)
    {
        auto status = Status(zookeeper->get(getCoordinatorPath(coordinator_id) + "/status/" + node)).getCode();
        if (status == STATUS_ERROR)
            has_error = true;
        else if (status == STATUS_ON_HOLD)
            has_on_hold = true;
    }

    /// Cancellation notifications have priority over error notifications.
    if (has_on_hold)
        return STATUS_ON_HOLD;
    else if (has_error)
        return STATUS_ERROR;
    else
        return STATUS_OK;
}

std::string ReshardingWorker::dumpCoordinatorState(const std::string & coordinator_id)
{
    std::string out;

    auto current_host = getFQDNOrHostName();

    try
    {
        WriteBufferFromString buf{out};

        writeString("Coordinator dump: ", buf);
        writeString("ID: {", buf);
        writeString(coordinator_id + "}; ", buf);

        auto zookeeper = context.getZooKeeper();

        Status status(zookeeper->get(getCoordinatorPath(coordinator_id) + "/status"));

        if (status.getCode() != STATUS_OK)
        {
            writeString("Global status: {", buf);
            writeString(status.getMessage() + "}; ", buf);
        }

        auto hosts = zookeeper->getChildren(getCoordinatorPath(coordinator_id) + "/status");
        for (const auto & host : hosts)
        {
            Status status(zookeeper->get(getCoordinatorPath(coordinator_id) + "/status/" + host));

            if (status.getCode() != STATUS_OK)
            {
                writeString("NODE ", buf);
                writeString(((host == current_host) ? "localhost" : host) + ": {", buf);
                writeString(status.getMessage() + "}; ", buf);
            }
        }

        buf.next();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    return out;
}

/// Compute the hash function from the checksum files of a given part.
/// The hash function we use is SipHash.
std::string ReshardingWorker::computeHashFromPart(const std::string & path)
{
    std::vector<std::string> files;

    Poco::DirectoryIterator end;
    for (Poco::DirectoryIterator it(path); it != end; ++it)
    {
        const auto & filename = it.name();
        if (filename == "checksums.txt")
            files.push_back(it.path().absolute().toString());
    }

    std::sort(files.begin(), files.end());

    SipHash hash;

    for (const auto & file : files)
    {
        ReadBufferFromFile buf{file};
        while (buf.next())
        {
            size_t byte_count = buf.buffer().end() - buf.position();
            hash.update(buf.position(), byte_count);
        }
    }

    char out[hash_size];
    hash.get128(out);

    return {out, hash_size};
}

ReshardingWorker::AnomalyType ReshardingWorker::probeForAnomaly()
{
    AnomalyType anomaly_type = ANOMALY_NONE;

    bool is_remote_node_unavailable = false;
    bool is_remote_node_error = false;

    bool cancellation_result = false;
    if (current_job.isCoordinated())
    {
        try
        {
            auto status = getStatus();
            if (status == STATUS_ON_HOLD)
                is_remote_node_unavailable = true;
            else if (status == STATUS_ERROR)
                is_remote_node_error = true;

            cancellation_result = status != STATUS_OK;
        }
        catch (...)
        {
            cancellation_result = true;
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    bool must_abort = must_stop || cancellation_result;

    if (must_abort)
    {
        /// Important: always keep the following order.
        if (must_stop)
            anomaly_type = ANOMALY_LOCAL_SHUTDOWN;
        else if (is_remote_node_unavailable)
            anomaly_type = ANOMALY_REMOTE_NODE_UNAVAILABLE;
        else if (is_remote_node_error)
            anomaly_type = ANOMALY_REMOTE_ERROR;
        else
            anomaly_type = ANOMALY_LOCAL_ERROR;
    }

    return anomaly_type;
}

void ReshardingWorker::processAnomaly(AnomalyType anomaly_type)
{
    if (anomaly_type == ANOMALY_NONE)
        return;

    current_job.is_aborted = true;

    if (anomaly_type == ANOMALY_LOCAL_SHUTDOWN)
        throw Exception{"Cancelled resharding", ErrorCodes::ABORTED};
    else if (anomaly_type == ANOMALY_REMOTE_NODE_UNAVAILABLE)
        throw Exception{"Remote node unavailable",
            ErrorCodes::RESHARDING_REMOTE_NODE_UNAVAILABLE};
    else if (anomaly_type == ANOMALY_REMOTE_ERROR)
        throw Exception{"An error occurred on a remote node",
            ErrorCodes::RESHARDING_REMOTE_NODE_ERROR};
    else
        throw Exception{"An error occurred on local node", ErrorCodes::LOGICAL_ERROR};
}

void ReshardingWorker::initializeJob()
{
    if (!current_job.isCoordinated())
        return;

    auto zookeeper = context.getZooKeeper();

    auto status = getStatus();

    if (status == STATUS_ERROR)
    {
        /// This case is triggered if an error occured on a performer
        /// while we were going offline.
        throw Exception{"An error occurred on a remote node", ErrorCodes::RESHARDING_REMOTE_NODE_ERROR};
    }
    else if (status == STATUS_ON_HOLD)
    {
        /// The current distributed job is on hold. Check that all the required
        /// performers are online. If it is so, wait for them to be ready to
        /// perform the job.

        setStatus(current_job.coordinator_id, getFQDNOrHostName(), STATUS_OK);

        getRecoveryBarrier().enter();

        /// Catch any error that could have happened while crossing the barrier.
        processAnomaly(probeForAnomaly());
    }
    else if (status == STATUS_OK)
    {
        /// For the purpose of creating the log, we need to know the locations
        /// of all the jobs that constitute this distributed job.
        zookeeper->set(getPartitionPath() + "/nodes/" + getFQDNOrHostName(),
            current_job.job_name);
    }
    else
    {
        /// This should never happen but we must take this case into account for the sake
        /// of completeness.
        throw Exception{"ReshardingWorker: unexpected status", ErrorCodes::LOGICAL_ERROR};
    }
}

void ReshardingWorker::finalizeJob()
{
    if (!current_job.isCoordinated())
        return;

    auto zookeeper = context.getZooKeeper();

    bool delete_coordinator = false;

    {
        /// finalizeJob() may be called when an error has occurred. For this reason,
        /// in the call to getCoordinatorLock(), the flag may_use_in_emergency
        /// is set to true: local or remote errors won't interrupt lock acquisition.
        auto lock = getCoordinatorLock(current_job.coordinator_id, true);
        zkutil::RWLock::Guard<zkutil::RWLock::Write> guard{lock};

        auto children = zookeeper->getChildren(getPartitionPath() + "/nodes");
        if (children.empty())
            throw Exception{"ReshardingWorker: unable to detach job", ErrorCodes::LOGICAL_ERROR};
        bool was_last_node = children.size() == 1;

        auto current_host = getFQDNOrHostName();
        zookeeper->remove(getPartitionPath() + "/nodes/" + current_host);

        if (was_last_node)
        {
            /// All the performers have processed the current partition.
            zookeeper->removeRecursive(getPartitionPath());
            if (getPartitionCountUnlocked(current_job.coordinator_id) == 0)
            {
                /// All the partitions of the current distributed job have been processed.
                delete_coordinator = true;
            }
        }
    }

    if (delete_coordinator)
        deleteCoordinator(current_job.coordinator_id);
}

void ReshardingWorker::waitForUploadCompletion()
{
    if (!current_job.isCoordinated())
        return;
    getUploadBarrier().enter();
}

void ReshardingWorker::waitForElectionCompletion()
{
    getElectionBarrier().enter();
}

void ReshardingWorker::waitForCommitCompletion()
{
    if (!current_job.isCoordinated())
        return;
    getCommitBarrier().enter();
}

void ReshardingWorker::abortTrackingIfRequested()
{
    if (must_stop)
        throw Exception{"Cancelled resharding", ErrorCodes::ABORTED};
}

void ReshardingWorker::abortRecoveryIfRequested()
{
    bool has_offline_nodes = false;
    bool must_abort;

    try
    {
        has_offline_nodes = detectOfflineNodes();
        must_abort = must_stop || has_offline_nodes;
    }
    catch (...)
    {
        must_abort = true;
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    if (must_abort)
    {
        if (must_stop)
            throw Exception{"Cancelled resharding", ErrorCodes::ABORTED};
        else if (has_offline_nodes)
            throw Exception{"Distributed job on hold. Ignoring for now",
                ErrorCodes::RESHARDING_DISTRIBUTED_JOB_ON_HOLD};
        else
        {
            /// We don't throw any exception here because the other performers waiting
            /// to cross the recovery barrier would get stuck forever into a loop.
            /// Instead we rely on cancellation points to detect this error and
            /// therefore terminate the job.
            setStatus(current_job.coordinator_id, getFQDNOrHostName(), STATUS_ERROR,
                "Recovery failed for an unspecified reason");
        }
    }
}

void ReshardingWorker::abortCoordinatorIfRequested(const std::string & coordinator_id)
{
    bool is_remote_node_unavailable = false;
    bool is_remote_node_error = false;

    bool cancellation_result = false;

    try
    {
        auto status = getCoordinatorStatus(coordinator_id);
        if (status == STATUS_ON_HOLD)
            is_remote_node_unavailable = true;
        else if (status == STATUS_ERROR)
            is_remote_node_error = true;

        cancellation_result = status != STATUS_OK;
    }
    catch (...)
    {
        cancellation_result = true;
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    bool must_abort = must_stop || cancellation_result;

    if (must_abort)
    {
        /// Important: always keep the following order.
        if (must_stop)
            throw Exception{"Cancelled resharding", ErrorCodes::ABORTED};
        else if (is_remote_node_unavailable)
            throw Exception{"Remote node unavailable",
                ErrorCodes::RESHARDING_REMOTE_NODE_UNAVAILABLE};
        else if (is_remote_node_error)
            throw Exception{"An error occurred on a remote node",
                ErrorCodes::RESHARDING_REMOTE_NODE_ERROR};
        else
            throw Exception{"An error occurred on local node", ErrorCodes::LOGICAL_ERROR};
    }
}

void ReshardingWorker::abortJobIfRequested()
{
    AnomalyType anomaly;

    if (current_job.isCoordinated())
        anomaly = anomaly_monitor.getAnomalyType();
    else
    {
        /// Very cheap because it actually just checks for must_stop.
        anomaly = probeForAnomaly();
    }

    processAnomaly(anomaly);
}

zkutil::RWLock ReshardingWorker::getGlobalLock()
{
    zkutil::RWLock lock{get_zookeeper, distributed_lock_path};
    zkutil::RWLock::CancellationHook hook = std::bind(&ReshardingWorker::abortTrackingIfRequested, this);
    lock.setCancellationHook(hook);

    return lock;
}

zkutil::RWLock ReshardingWorker::getCoordinatorLock(const std::string & coordinator_id,
    bool usable_in_emergency)
{
    zkutil::RWLock lock{get_zookeeper, getCoordinatorPath(coordinator_id) + "/lock"};

    zkutil::RWLock::CancellationHook hook;
    if (usable_in_emergency)
        hook = std::bind(&ReshardingWorker::abortTrackingIfRequested, this);
    else
        hook = std::bind(&ReshardingWorker::abortCoordinatorIfRequested,
            this, coordinator_id);

    lock.setCancellationHook(hook);

    return lock;
}

zkutil::RWLock ReshardingWorker::createDeletionLock(const std::string & coordinator_id)
{
    zkutil::RWLock lock{get_zookeeper, getCoordinatorPath(coordinator_id) + "/deletion_lock"};
    zkutil::RWLock::CancellationHook hook = std::bind(&ReshardingWorker::abortTrackingIfRequested, this);
    lock.setCancellationHook(hook);

    return lock;
}

zkutil::SingleBarrier ReshardingWorker::getCheckBarrier(const std::string & coordinator_id)
{
    auto zookeeper = context.getZooKeeper();

    auto node_count = zookeeper->get(getCoordinatorPath(coordinator_id) + "/node_count");

    zkutil::SingleBarrier check_barrier{get_zookeeper, getCoordinatorPath(coordinator_id) + "/check_barrier",
        std::stoull(node_count)};
    zkutil::SingleBarrier::CancellationHook hook = std::bind(&ReshardingWorker::abortCoordinatorIfRequested, this,
        coordinator_id);
    check_barrier.setCancellationHook(hook);

    return check_barrier;
}

zkutil::SingleBarrier ReshardingWorker::getOptOutBarrier(const std::string & coordinator_id,
    size_t count)
{
    zkutil::SingleBarrier opt_out_barrier{get_zookeeper, getCoordinatorPath(coordinator_id)
        + "/opt_out_barrier", count};
    zkutil::SingleBarrier::CancellationHook hook = std::bind(&ReshardingWorker::abortCoordinatorIfRequested, this,
        coordinator_id);
    opt_out_barrier.setCancellationHook(hook);

    return opt_out_barrier;
}

zkutil::SingleBarrier ReshardingWorker::getRecoveryBarrier()
{
    auto zookeeper = context.getZooKeeper();

    auto node_count = zookeeper->getChildren(getPartitionPath() + "/nodes").size();

    zkutil::SingleBarrier recovery_barrier{get_zookeeper, getPartitionPath() + "/recovery_barrier", node_count};
    zkutil::SingleBarrier::CancellationHook hook = std::bind(&ReshardingWorker::abortRecoveryIfRequested, this);
    recovery_barrier.setCancellationHook(hook);

    return recovery_barrier;
}

zkutil::SingleBarrier ReshardingWorker::getUploadBarrier()
{
    auto zookeeper = context.getZooKeeper();

    auto node_count = zookeeper->getChildren(getPartitionPath() + "/nodes").size();

    zkutil::SingleBarrier upload_barrier{get_zookeeper, getPartitionPath() + "/upload_barrier", node_count};

    zkutil::SingleBarrier::CancellationHook hook = std::bind(&ReshardingWorker::abortJobIfRequested, this);
    upload_barrier.setCancellationHook(hook);

    return upload_barrier;
}

zkutil::SingleBarrier ReshardingWorker::getElectionBarrier()
{
    auto zookeeper = context.getZooKeeper();

    auto node_count = zookeeper->getChildren(getPartitionPath() + "/nodes").size();

    zkutil::SingleBarrier election_barrier{get_zookeeper, getPartitionPath() + "/election_barrier", node_count};

    zkutil::SingleBarrier::CancellationHook hook = std::bind(&ReshardingWorker::abortJobIfRequested, this);
    election_barrier.setCancellationHook(hook);

    return election_barrier;
}

zkutil::SingleBarrier ReshardingWorker::getCommitBarrier()
{
    auto zookeeper = context.getZooKeeper();

    auto node_count = zookeeper->getChildren(getPartitionPath() + "/nodes").size();

    zkutil::SingleBarrier commit_barrier{get_zookeeper, getPartitionPath() + "/commit_barrier", node_count};
    zkutil::SingleBarrier::CancellationHook hook = std::bind(&ReshardingWorker::abortJobIfRequested, this);
    commit_barrier.setCancellationHook(hook);

    return commit_barrier;
}

std::string ReshardingWorker::getCoordinatorPath(const std::string & coordinator_id) const
{
    return coordination_path + "/" + coordinator_id;
}

std::string ReshardingWorker::getPartitionPath() const
{
    return coordination_path + "/" + current_job.coordinator_id + "/partitions/" + current_job.partition;
}

std::string ReshardingWorker::getLocalJobPath() const
{
    return host_task_queue_path + "/" + current_job.job_name;
}

ReshardingWorker::AnomalyMonitor::AnomalyMonitor(ReshardingWorker & resharding_worker_)
    : resharding_worker{resharding_worker_}
{
}

ReshardingWorker::AnomalyMonitor::~AnomalyMonitor()
{
    try
    {
        shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void ReshardingWorker::AnomalyMonitor::start()
{
    if (resharding_worker.current_job.isCoordinated())
        thread_routine = std::thread{&ReshardingWorker::AnomalyMonitor::routine, this};
}

void ReshardingWorker::AnomalyMonitor::shutdown()
{
    if (is_started)
    {
        must_stop = true;

        if (thread_routine.joinable())
            thread_routine.join();

        is_started = false;
        must_stop = false;
        anomaly_type = ANOMALY_NONE;
    }
}

void ReshardingWorker::AnomalyMonitor::routine()
{
    bool old_val = false;
    if (!is_started.compare_exchange_strong(old_val, true, std::memory_order_seq_cst,
        std::memory_order_relaxed))
        throw Exception{"Anomaly probing thread already started", ErrorCodes::LOGICAL_ERROR};

    anomaly_type = ANOMALY_NONE;

    while (!must_stop)
    {
        auto zookeeper = resharding_worker.context.getZooKeeper();
        auto coordinator_id = resharding_worker.current_job.coordinator_id;

        /// We create a new instance of Poco::Event each time we run
        /// the loop body in order to avoid multiple notifications.
        zkutil::EventPtr event = std::make_shared<Poco::Event>();

        /// Monitor both status changes and movements of the performers.
        (void) zookeeper->get(resharding_worker.getCoordinatorPath(coordinator_id)
            + "/status_probe", nullptr, event);
        (void) zookeeper->getChildren(resharding_worker.distributed_online_path,
            nullptr, event);

        auto probed_anomaly_type = resharding_worker.probeForAnomaly();
        if (probed_anomaly_type != ANOMALY_NONE)
        {
            /// An anomaly has just been found. No need to monitor further.
            anomaly_type = probed_anomaly_type;
            break;
        }

        while (!event->tryWait(wait_duration))
        {
            /// We are going offline.
            if (resharding_worker.must_stop)
                break;
            /// We have received a request to stop this thread.
            if (must_stop)
                break;
        }
    }
}

ReshardingWorker::AnomalyType ReshardingWorker::AnomalyMonitor::getAnomalyType() const
{
    return anomaly_type;
}

ReshardingWorker::LogRecord::LogRecord(zkutil::ZooKeeperPtr zookeeper_)
    : zookeeper{zookeeper_}
{
}

ReshardingWorker::LogRecord::LogRecord(zkutil::ZooKeeperPtr zookeeper_, const std::string & zk_path_)
    : zookeeper{zookeeper_}, zk_path{zk_path_}
{
    auto serialized_record = zookeeper->get(zk_path);
    ReadBufferFromString buf{serialized_record};

    unsigned int val;
    readVarUInt(val, buf);
    operation = static_cast<Operation>(val);

    readVarUInt(val, buf);
    state = static_cast<State>(val);

    readBinary(partition, buf);
    readBinary(partition_hash, buf);
    readVarUInt(shard_no, buf);

    size_t s;
    readVarUInt(s, buf);

    for (size_t i = 0; i < s; ++i)
    {
        std::string part;
        readBinary(part, buf);

        std::string hash;
        readBinary(hash, buf);

        parts_with_hash.emplace(part, hash);
    }
}

void ReshardingWorker::LogRecord::enqueue(const std::string & log_path)
{
    (void) zookeeper->create(log_path + "/rec-", toString(), zkutil::CreateMode::PersistentSequential);
}

void ReshardingWorker::LogRecord::writeBack()
{
    zookeeper->set(zk_path, toString());
}

std::string ReshardingWorker::LogRecord::toString()
{
    std::string out;
    WriteBufferFromString buf{out};

    writeVarUInt(static_cast<unsigned int>(operation), buf);
    writeVarUInt(static_cast<unsigned int>(state), buf);
    writeBinary(partition, buf);
    writeBinary(partition_hash, buf);
    writeVarUInt(shard_no, buf);

    writeVarUInt(parts_with_hash.size(), buf);
    for (const auto & entry : parts_with_hash)
    {
        writeBinary(entry.first, buf);
        writeBinary(entry.second, buf);
    }

    buf.next();

    return out;
}

}
