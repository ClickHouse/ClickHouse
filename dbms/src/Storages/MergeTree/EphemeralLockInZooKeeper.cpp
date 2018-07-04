#include <Storages/MergeTree/EphemeralLockInZooKeeper.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

EphemeralLockInZooKeeper::EphemeralLockInZooKeeper(
    const String & path_prefix_, const String & temp_path, zkutil::ZooKeeper & zookeeper_, zkutil::Requests * precheck_ops)
    : zookeeper(&zookeeper_), path_prefix(path_prefix_)
{
    /// The /abandonable_lock- name is for backward compatibility.
    String holder_path_prefix = temp_path + "/abandonable_lock-";

    /// Let's create an secondary ephemeral node.
    if (!precheck_ops || precheck_ops->empty())
    {
        holder_path = zookeeper->create(holder_path_prefix, "", zkutil::CreateMode::EphemeralSequential);
    }
    else
    {
        precheck_ops->emplace_back(zkutil::makeCreateRequest(holder_path_prefix, "", zkutil::CreateMode::EphemeralSequential));
        zkutil::Responses op_results = zookeeper->multi(*precheck_ops);
        holder_path = dynamic_cast<const zkutil::CreateResponse &>(*op_results.back()).path_created;
    }

    /// Write the path to the secondary node in the main node.
    path = zookeeper->create(path_prefix, holder_path, zkutil::CreateMode::EphemeralSequential);

    if (path.size() <= path_prefix.size())
        throw Exception("Logical error: name of the main node is shorter than prefix.", ErrorCodes::LOGICAL_ERROR);
}

void EphemeralLockInZooKeeper::unlock()
{
    zkutil::Requests ops;
    getUnlockOps(ops);
    zookeeper->multi(ops);
    holder_path = "";
}

void EphemeralLockInZooKeeper::getUnlockOps(zkutil::Requests & ops)
{
    checkCreated();
    ops.emplace_back(zkutil::makeRemoveRequest(path, -1));
    ops.emplace_back(zkutil::makeRemoveRequest(holder_path, -1));
}

EphemeralLockInZooKeeper::~EphemeralLockInZooKeeper()
{
    if (!isCreated())
        return;

    try
    {
        unlock();
    }
    catch (...)
    {
        tryLogCurrentException("~EphemeralLockInZooKeeper");
    }
}


EphemeralLocksInAllPartitions::EphemeralLocksInAllPartitions(
    const String & block_numbers_path, const String & path_prefix, const String & temp_path,
    zkutil::ZooKeeper & zookeeper_)
    : zookeeper(zookeeper_)
{
    std::vector<String> holders;
    while (true)
    {
        zkutil::Stat partitions_stat;
        Strings partitions = zookeeper.getChildren(block_numbers_path, &partitions_stat);

        if (holders.size() < partitions.size())
        {
            std::vector<std::future<zkutil::CreateResponse>> holder_futures;
            for (size_t i = 0; i < partitions.size() - holders.size(); ++i)
            {
                String path = temp_path + "/abandonable_lock-";
                holder_futures.push_back(zookeeper.asyncCreate(path, {}, zkutil::CreateMode::EphemeralSequential));
            }
            for (auto & future : holder_futures)
            {
                auto resp = future.get();
                holders.push_back(resp.path_created);
            }
        }

        zkutil::Requests lock_ops;
        for (size_t i = 0; i < partitions.size(); ++i)
        {
            String partition_path_prefix = block_numbers_path + "/" + partitions[i] + "/" + path_prefix;
            lock_ops.push_back(zkutil::makeCreateRequest(
                    partition_path_prefix, holders[i], zkutil::CreateMode::EphemeralSequential));
        }
        lock_ops.push_back(zkutil::makeCheckRequest(block_numbers_path, partitions_stat.version));

        zkutil::Responses lock_responses;
        int rc = zookeeper.tryMulti(lock_ops, lock_responses);
        if (rc == ZooKeeperImpl::ZooKeeper::ZBADVERSION)
        {
            LOG_TRACE(&Logger::get("EphemeralLocksInAllPartitions"),
                "Someone has inserted a block in a new partition while we were creating locks. Retry.");
            continue;
        }
        else if (rc != ZooKeeperImpl::ZooKeeper::ZOK)
            throw zkutil::KeeperException(rc);

        for (size_t i = 0; i < partitions.size(); ++i)
        {
            size_t prefix_size = block_numbers_path.size() + 1 + partitions[i].size() + 1 + path_prefix.size();
            const String & path = static_cast<const zkutil::CreateResponse &>(*lock_responses[i]).path_created;
            if (path.size() <= prefix_size)
                throw Exception("Logical error: name of the sequential node is shorter than prefix.",
                    ErrorCodes::LOGICAL_ERROR);

            UInt64 number = parse<UInt64>(path.c_str() + prefix_size, path.size() - prefix_size);
            locks.push_back(LockInfo{path, holders[i], partitions[i], number});
        }

        return;
    }
}

void EphemeralLocksInAllPartitions::unlock()
{
    std::vector<zkutil::ZooKeeper::FutureMulti> futures;
    for (const auto & lock : locks)
    {
        zkutil::Requests unlock_ops;
        unlock_ops.emplace_back(zkutil::makeRemoveRequest(lock.path, -1));
        unlock_ops.emplace_back(zkutil::makeRemoveRequest(lock.holder_path, -1));
        futures.push_back(zookeeper.asyncMulti(unlock_ops));
    }

    for (auto & future : futures)
        future.get();

    locks.clear();
}

EphemeralLocksInAllPartitions::~EphemeralLocksInAllPartitions()
{
    try
    {
        unlock();
    }
    catch (...)
    {
        tryLogCurrentException("~EphemeralLocksInAllPartitions");
    }
}

}
