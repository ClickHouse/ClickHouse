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

EphemeralLockInZooKeeper::EphemeralLockInZooKeeper(const String & path_prefix_, const ZooKeeperWithFaultInjectionPtr & zookeeper_, const String & path_)
    : zookeeper(zookeeper_), path_prefix(path_prefix_), path(path_)
{
    if (path.size() <= path_prefix.size())
        throw Exception("Logical error: name of the main node is shorter than prefix.", ErrorCodes::LOGICAL_ERROR);
}

std::optional<EphemeralLockInZooKeeper> createEphemeralLockInZooKeeper(
    const String & path_prefix_, const String & temp_path, const ZooKeeperWithFaultInjectionPtr & zookeeper_, const String & deduplication_path)
{
    String path;

    if (deduplication_path.empty())
    {
        String holder_path = temp_path + "/" + EphemeralLockInZooKeeper::LEGACY_LOCK_OTHER;
        path = zookeeper_->create(path_prefix_, holder_path, zkutil::CreateMode::EphemeralSequential);
    }
    else
    {
        String holder_path = temp_path + "/" + EphemeralLockInZooKeeper::LEGACY_LOCK_INSERT;

        /// Check for duplicates in advance, to avoid superfluous block numbers allocation
        Coordination::Requests ops;
        ops.emplace_back(zkutil::makeCreateRequest(deduplication_path, "", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeRemoveRequest(deduplication_path, -1));
        ops.emplace_back(zkutil::makeCreateRequest(path_prefix_, holder_path, zkutil::CreateMode::EphemeralSequential));
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

        path = dynamic_cast<const Coordination::CreateResponse *>(responses.back().get())->path_created;
    }

    return EphemeralLockInZooKeeper{path_prefix_, zookeeper_, path};
}

void EphemeralLockInZooKeeper::unlock()
{
    Coordination::Requests ops;
    getUnlockOp(ops);
    zookeeper->multi(ops);
    zookeeper = nullptr;
}

void EphemeralLockInZooKeeper::getUnlockOp(Coordination::Requests & ops)
{
    checkCreated();
    ops.emplace_back(zkutil::makeRemoveRequest(path, -1));
}

EphemeralLockInZooKeeper::~EphemeralLockInZooKeeper()
{
    if (!isLocked())
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
    String holder_path = temp_path + "/" + EphemeralLockInZooKeeper::LEGACY_LOCK_OTHER;
    while (true)
    {
        Coordination::Stat partitions_stat;
        Strings partitions = zookeeper->getChildren(block_numbers_path, &partitions_stat);

        Coordination::Requests lock_ops;
        for (const auto & partition : partitions)
        {
            String partition_path_prefix = block_numbers_path + "/" + partition + "/" + path_prefix;
            lock_ops.push_back(zkutil::makeCreateRequest(
                    partition_path_prefix, holder_path, zkutil::CreateMode::EphemeralSequential));
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
            locks.push_back(LockInfo{path, partitions[i], number});
        }

        return;
    }
}

void EphemeralLocksInAllPartitions::unlock()
{
    if (!zookeeper)
        return;

    std::vector<zkutil::ZooKeeper::FutureRemove> futures;
    futures.reserve(locks.size());
    for (const auto & lock : locks)
    {
        futures.push_back(zookeeper->asyncRemove(lock.path));
    }

    for (auto & future : futures)
        future.get();

    locks.clear();
    zookeeper = nullptr;
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
