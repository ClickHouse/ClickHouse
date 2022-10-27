#include <Storages/MergeTree/EphemeralLockInZooKeeper.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/logger_useful.h>
#include <base/types.h>
#include <Storages/MergeTree/ZooKeeperWithFaultInjection.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

EphemeralLockInZooKeeper::EphemeralLockInZooKeeper(const String & path_prefix_, const ZooKeeperWithFaultInjectionPtr & zookeeper_, const String & holder_path_)
    : zookeeper(zookeeper_), path_prefix(path_prefix_), holder_path(holder_path_)
{
    /// Write the path to the secondary node in the main node.
    path = zookeeper->create(path_prefix, holder_path, zkutil::CreateMode::EphemeralSequential);
    if (path.size() <= path_prefix.size())
        throw Exception("Logical error: name of the main node is shorter than prefix.", ErrorCodes::LOGICAL_ERROR);
}

std::optional<EphemeralLockInZooKeeper> createEphemeralLockInZooKeeper(
    const String & path_prefix_, const String & temp_path, const ZooKeeperWithFaultInjectionPtr & zookeeper_, const String & deduplication_path)
{
    /// The /abandonable_lock- name is for backward compatibility.
    String holder_path_prefix = temp_path + "/abandonable_lock-";
    String holder_path;

    /// Let's create an secondary ephemeral node.
    if (deduplication_path.empty())
    {
        holder_path = zookeeper_->create(holder_path_prefix, "", zkutil::CreateMode::EphemeralSequential);
    }
    else
    {
        /// Check for duplicates in advance, to avoid superfluous block numbers allocation
        Coordination::Requests ops;
        ops.emplace_back(zkutil::makeCreateRequest(deduplication_path, "", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeRemoveRequest(deduplication_path, -1));
        ops.emplace_back(zkutil::makeCreateRequest(holder_path_prefix, "", zkutil::CreateMode::EphemeralSequential));
        Coordination::Responses responses;
        Coordination::Error e = zookeeper_->tryMulti(ops, responses);
        if (e != Coordination::Error::ZOK)
        {
            if (responses[0]->error == Coordination::Error::ZNODEEXISTS)
            {
                LOG_DEBUG(
                    &Poco::Logger::get("createEphemeralLockInZooKeeper"),
                    "Deduplication path already exists: deduplication_path={}",
                    deduplication_path);
                return {};
            }
            else
            {
                zkutil::KeeperMultiException::check(e, ops, responses); // This should always throw the proper exception
                throw Exception("Unable to handle error {} when acquiring ephemeral lock in ZK", ErrorCodes::LOGICAL_ERROR);
            }
        }

        holder_path = dynamic_cast<const Coordination::CreateResponse *>(responses.back().get())->path_created;
    }

    return EphemeralLockInZooKeeper{path_prefix_, zookeeper_, holder_path};
}

void EphemeralLockInZooKeeper::unlock()
{
    Coordination::Requests ops;
    getUnlockOps(ops);
    zookeeper->multi(ops);
    holder_path = "";
}

void EphemeralLockInZooKeeper::getUnlockOps(Coordination::Requests & ops)
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
    catch (const zkutil::KeeperException & e)
    {
        if (Coordination::isHardwareError(e.code))
            LOG_DEBUG(
                &Poco::Logger::get("EphemeralLockInZooKeeper"),
                "ZooKeeper communication error during unlock: code={} message='{}'",
                e.code,
                e.message());
        else if (e.code == Coordination::Error::ZNONODE)
            /// To avoid additional round-trip for unlocking,
            /// ephemeral node can be deleted explicitly as part of another multi op request to ZK
            /// and marked as such via assumeUnlocked() if we got successful response.
            /// But it's possible that the multi op request can be executed on server side, and client will not get response due to network issue.
            /// In such case, assumeUnlocked() will not be called, so we'll get ZNONODE error here since the noded is already deleted
            LOG_DEBUG(
                &Poco::Logger::get("EphemeralLockInZooKeeper"),
                "ZooKeeper node was already deleted: code={} message={}",
                e.code,
                e.message());
        else
            tryLogCurrentException("EphemeralLockInZooKeeper");
    }
    catch (...)
    {
        tryLogCurrentException("EphemeralLockInZooKeeper");
    }
}


EphemeralLocksInAllPartitions::EphemeralLocksInAllPartitions(
    const String & block_numbers_path, const String & path_prefix, const String & temp_path,
    zkutil::ZooKeeper & zookeeper_)
    : zookeeper(&zookeeper_)
{
    std::vector<String> holders;
    while (true)
    {
        Coordination::Stat partitions_stat;
        Strings partitions = zookeeper->getChildren(block_numbers_path, &partitions_stat);

        if (holders.size() < partitions.size())
        {
            std::vector<std::future<Coordination::CreateResponse>> holder_futures;
            for (size_t i = 0; i < partitions.size() - holders.size(); ++i)
            {
                String path = temp_path + "/abandonable_lock-";
                holder_futures.push_back(zookeeper->asyncCreate(path, {}, zkutil::CreateMode::EphemeralSequential));
            }
            for (auto & future : holder_futures)
            {
                auto resp = future.get();
                holders.push_back(resp.path_created);
            }
        }

        Coordination::Requests lock_ops;
        for (size_t i = 0; i < partitions.size(); ++i)
        {
            String partition_path_prefix = block_numbers_path + "/" + partitions[i] + "/" + path_prefix;
            lock_ops.push_back(zkutil::makeCreateRequest(
                    partition_path_prefix, holders[i], zkutil::CreateMode::EphemeralSequential));
        }
        lock_ops.push_back(zkutil::makeCheckRequest(block_numbers_path, partitions_stat.version));

        Coordination::Responses lock_responses;
        Coordination::Error rc = zookeeper->tryMulti(lock_ops, lock_responses);
        if (rc == Coordination::Error::ZBADVERSION)
        {
            LOG_TRACE(&Poco::Logger::get("EphemeralLocksInAllPartitions"), "Someone has inserted a block in a new partition while we were creating locks. Retry.");
            continue;
        }
        else if (rc != Coordination::Error::ZOK)
            throw Coordination::Exception(rc);

        for (size_t i = 0; i < partitions.size(); ++i)
        {
            size_t prefix_size = block_numbers_path.size() + 1 + partitions[i].size() + 1 + path_prefix.size();
            const String & path = dynamic_cast<const Coordination::CreateResponse &>(*lock_responses[i]).path_created;
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
    if (!zookeeper)
        return;

    std::vector<zkutil::ZooKeeper::FutureMulti> futures;
    for (const auto & lock : locks)
    {
        Coordination::Requests unlock_ops;
        unlock_ops.emplace_back(zkutil::makeRemoveRequest(lock.path, -1));
        unlock_ops.emplace_back(zkutil::makeRemoveRequest(lock.holder_path, -1));
        futures.push_back(zookeeper->asyncMulti(unlock_ops));
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
